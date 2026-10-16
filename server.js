const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

app.get('/', (req, res) => {
    const clientIp =
        req.headers['x-forwarded-for'] ||
        req.socket.remoteAddress;

    res.status(200).json({
        status: 'Mobira Server Online',
        ip: clientIp
    });
});

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const activeUsers = {};
const inCallUsers = new Set();

/* =========================================
   VASTAAMATTOMAT PUHELUT

   missedCalls: numerolle kertyneet vastaamattomat
   puhelut.

   ringingCalls: kohdenumero -> soittajan numero.
   ========================================= */

const missedCalls = {};
const ringingCalls = {};

function addMissedCall(targetNumber, fromNumber) {
    if (!targetNumber || !fromNumber) return;

    if (!missedCalls[targetNumber]) {
        missedCalls[targetNumber] = new Set();
    }

    missedCalls[targetNumber].add(fromNumber);

    console.log(
        `Vastaamaton puhelu merkitty: ${fromNumber} -> ${targetNumber}`
    );

    const targetSocketId = activeUsers[targetNumber];

    if (targetSocketId) {
        io.to(targetSocketId).emit('missed_call', {
            fromNumber: fromNumber
        });
    }
}

function clearMissedCall(numberThatMissed, fromNumber) {
    if (
        missedCalls[numberThatMissed] &&
        missedCalls[numberThatMissed].has(fromNumber)
    ) {
        missedCalls[numberThatMissed].delete(fromNumber);

        if (missedCalls[numberThatMissed].size === 0) {
            delete missedCalls[numberThatMissed];
        }

        console.log(
            `Vastaamaton puhelu kuitattu: ${numberThatMissed} soitti takaisin ${fromNumber}`
        );
    }
}

/* =========================================
   APUFUNKTIOT
   ========================================= */

function bindNumber(socket, number) {
    activeUsers[number] = socket.id;

    socket.mobiraInfo = socket.mobiraInfo || {};
    socket.mobiraInfo.phoneNumber = number;
}

function numberOfSocket(socket) {
    const n =
        socket.mobiraInfo &&
        socket.mobiraInfo.phoneNumber;

    if (n && activeUsers[n] === socket.id) {
        return n;
    }

    for (const [num, id] of Object.entries(activeUsers)) {
        if (id === socket.id) return num;
    }

    return null;
}

io.on('connection', (socket) => {

    /* =========================================
       NUMERON REKISTERÖINTI
       ========================================= */

    socket.on('register_number', (number) => {
        if (number && typeof number === 'object') {
            number =
                number.number ??
                number.phoneNumber ??
                number.fromNumber;
        }

        number = String(number ?? '').trim();

        if (!number) return;

        bindNumber(socket, number);

        console.log(
            `Numero ${number} rekisteröity socketiin ${socket.id}`
        );

        if (
            missedCalls[number] &&
            missedCalls[number].size > 0
        ) {
            missedCalls[number].forEach((fromNumber) => {
                socket.emit('missed_call', {
                    fromNumber: fromNumber
                });
            });
        }
    });

    /* =========================================
       LAITTEEN REKISTERÖINTI
       ========================================= */

    socket.on('register_device', (data) => {
        socket.mobiraInfo = {
            ...(socket.mobiraInfo || {}),

            deviceType:
                data && data.deviceType
                    ? data.deviceType
                    : null,

            hiddenChannel: !!(
                data &&
                data.hiddenChannel
            ),

            currentChannel:
                data &&
                data.currentChannel != null
                    ? data.currentChannel
                    : null
        };

        console.log(
            'Laite rekisteröity:',
            socket.mobiraInfo
        );
    });

    /* =========================================
       KANAVAN VAIHTUMINEN
       ========================================= */

    socket.on('set_channel', (channel) => {
        socket.mobiraInfo =
            socket.mobiraInfo || {};

        socket.mobiraInfo.currentChannel = channel;

        console.log(
            `Socket ${socket.id} kanava: ${channel}`
        );
    });

    /* =========================================
       PUHELUN ALOITUS
       ========================================= */

    socket.on('start_call', (data) => {
        const fromNumber =
            String(data.fromNumber || '').trim();

        const targetNumber =
            String(data.targetNumber || '').trim();

        /*
         * Puuttuva numero ei aiheuta varattuääntä.
         */

        if (!fromNumber || !targetNumber) {
            socket.emit('number_unavailable', {
                targetNumber,
                reason: 'invalid_number'
            });

            return;
        }

        /*
         * Sidotaan soittajan numero socketiin.
         */

        if (activeUsers[fromNumber] !== socket.id) {
            bindNumber(socket, fromNumber);

            console.log(
                `Soittaja ${fromNumber} sidottu sockettiin ${socket.id}`
            );
        }

        /*
         * Takaisinsoitto kuittaa vastaamattoman puhelun.
         */

        clearMissedCall(fromNumber, targetNumber);

        /*
         * Tarkistetaan vastaanottajan verkkoyhteys.
         */

        const targetSocketId =
            activeUsers[targetNumber];

        /*
         * Jos jompikumpi on jo puhelussa,
         * kyseessä on oikeasti varattu linja.
         */

        if (
            inCallUsers.has(targetNumber) ||
            inCallUsers.has(fromNumber)
        ) {
            console.log(
                `Varattu: ${fromNumber} -> ${targetNumber}. ` +
                `Puhelussa: [${[...inCallUsers].join(', ')}]`
            );

            socket.emit('line_busy');

            return;
        }

        /*
         * Vastaanottajan laite ei ole verkossa.
         *
         * Ei lähetetä line_busy-tapahtumaa, joten
         * asiakkaan ei pitäisi soittaa varattuääntä.
         */

        if (!targetSocketId) {
            console.log(
                `Kohdetta ${targetNumber} ei ole verkossa. ` +
                `Verkossa olevat numerot: [${Object.keys(activeUsers).join(', ')}]`
            );

            addMissedCall(targetNumber, fromNumber);

            socket.emit('number_unavailable', {
                targetNumber,
                reason: 'offline'
            });

            return;
        }

        /* =====================================
           KANAVAN KÄSITTELY
           ===================================== */

        let callChannel =
            data.channel ?? null;

        /*
         * Cityman käyttää vastaanottajan nykyistä kanavaa.
         */

        if (
            data.cityman === true ||
            data.deviceType === 'CITYMAN'
        ) {
            const targetSocket =
                io.sockets.sockets.get(targetSocketId);

            if (
                targetSocket &&
                targetSocket.mobiraInfo &&
                targetSocket.mobiraInfo.currentChannel != null
            ) {
                callChannel =
                    targetSocket.mobiraInfo.currentChannel;
            }
        }

        /* =====================================
           MERKITÄÄN PUHELUN OSAPUOLET
           ===================================== */

        inCallUsers.add(fromNumber);
        inCallUsers.add(targetNumber);

        /*
         * Merkitään puhelu soimaan.
         */

        ringingCalls[targetNumber] = fromNumber;

        /* =====================================
           LÄHETETÄÄN TULEVA PUHELU
           ===================================== */

        io.to(targetSocketId).emit('incoming_call', {
            fromNumber: fromNumber,
            channel: callChannel
        });

        console.log(
            `Puhelu ${fromNumber} -> ${targetNumber}, kanava: ${callChannel}`
        );
    });

    /* =========================================
       PUHELUUN VASTAAMINEN
       ========================================= */

    socket.on('answer_call', (data) => {
        const answeringNumber =
            numberOfSocket(socket);

        if (answeringNumber) {
            inCallUsers.add(answeringNumber);
        }

        if (
            answeringNumber &&
            ringingCalls[answeringNumber]
        ) {
            delete ringingCalls[answeringNumber];
        }

        const callerSocketId =
            activeUsers[data.targetNumber];

        if (callerSocketId) {
            io.to(callerSocketId).emit('call_answered', {
                channel:
                    data &&
                    data.channel != null
                        ? data.channel
                        : null
            });
        }
    });

    /* =========================================
       WEBRTC OFFER
       ========================================= */

    socket.on('webrtc_offer', (data) => {
        const targetSocketId =
            activeUsers[data.targetNumber];

        if (targetSocketId) {
            io.to(targetSocketId).emit('webrtc_offer', {
                fromNumber: data.fromNumber,
                offer: data.offer
            });
        }
    });

    /* =========================================
       WEBRTC ANSWER
       ========================================= */

    socket.on('webrtc_answer', (data) => {
        const targetSocketId =
            activeUsers[data.targetNumber];

        if (targetSocketId) {
            io.to(targetSocketId).emit('webrtc_answer', {
                answer: data.answer
            });
        }
    });

    /* =========================================
       WEBRTC ICE
       ========================================= */

    socket.on('webrtc_ice_candidate', (data) => {
        const targetSocketId =
            activeUsers[data.targetNumber];

        if (targetSocketId) {
            io.to(targetSocketId).emit(
                'webrtc_ice_candidate',
                {
                    candidate: data.candidate
                }
            );
        }
    });

    /* =========================================
       PUHELUN LOPETUS
       ========================================= */

    socket.on('end_call', (data) => {
        const thisNumber =
            numberOfSocket(socket);

        if (thisNumber) {
            inCallUsers.delete(thisNumber);
        }

        if (data && data.targetNumber) {
            const targetNumber =
                String(data.targetNumber);

            inCallUsers.delete(targetNumber);

            /*
             * Jos puhelu ehti soida mutta siihen ei vastattu,
             * merkitään se vastaamattomaksi.
             */

            if (
                thisNumber &&
                ringingCalls[targetNumber] === thisNumber
            ) {
                addMissedCall(
                    targetNumber,
                    thisNumber
                );

                delete ringingCalls[targetNumber];

            } else if (
                thisNumber &&
                ringingCalls[thisNumber] === targetNumber
            ) {
                delete ringingCalls[thisNumber];
            }

            const targetSocketId =
                activeUsers[targetNumber];

            if (targetSocketId) {
                io.to(targetSocketId).emit('call_ended');
            }
        }
    });

    /* =========================================
       YHTEYDEN KATKEAMINEN
       ========================================= */

    socket.on('disconnect', () => {
        for (
            const [number, id]
            of Object.entries(activeUsers)
        ) {
            if (id === socket.id) {
                delete activeUsers[number];

                inCallUsers.delete(number);

                /*
                 * Jos laite poistuu verkosta puhelun soidessa,
                 * tallennetaan puhelu vastaamattomaksi.
                 */

                if (ringingCalls[number]) {
                    const fromNumber =
                        ringingCalls[number];

                    addMissedCall(
                        number,
                        fromNumber
                    );

                    delete ringingCalls[number];

                    inCallUsers.delete(fromNumber);

                    const callerSocketId =
                        activeUsers[fromNumber];

                    if (callerSocketId) {
                        io.to(callerSocketId).emit(
                            'call_ended'
                        );
                    }
                }

                console.log(
                    `Numero ${number} poistui verkosta`
                );

                break;
            }
        }
    });
});

/* =========================================
   SERVERIN KÄYNNISTYS
   ========================================= */

const PORT =
    process.env.PORT || 3000;

server.listen(PORT, () => {
    console.log(
        `Mobira-palvelin pyörii portissa ${PORT}`
    );
});
