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
   VASTAAMATTOMAT PUHELUT (KUTSU E)

   missedCalls: { numero: Set(numerot, jotka ovat
   soittaneet eikä niihin ole vastattu / soitettu
   takaisin) }

   ringingCalls: { kohdenumero: soittajanNumero }
   Kertoo, kenelle soi juuri nyt vastaamaton
   puhelu. Käytetään sen päättelyyn, oliko puhelu
   vielä vastaamatta kun se päättyi.
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

io.on('connection', (socket) => {

    /* =========================================
       NUMERON REKISTERÖINTI
       ========================================= */

    socket.on('register_number', (number) => {
        number = String(number);

        activeUsers[number] = socket.id;

        socket.mobiraInfo = socket.mobiraInfo || {};
        socket.mobiraInfo.phoneNumber = number;

        console.log(
            `Numero ${number} rekisteröity socketiin ${socket.id}`
        );

        /*
         * Jos numerolle on kertynyt vastaamattomia
         * puheluita sillä aikaa kun se ei ollut
         * verkossa (esim. html kiinni), ilmoitetaan
         * ne nyt kaikki.
         */

        if (missedCalls[number] && missedCalls[number].size > 0) {
            missedCalls[number].forEach((fromNumber) => {
                socket.emit('missed_call', {
                    fromNumber: fromNumber
                });
            });
        }
    });


    /* =========================================
       LAITTEEN REKISTERÖINTI

       Cityman käyttää tätä ilmoittaakseen:
       - olevansa CITYMAN
       - että kanava on käyttäjältä piilossa
       ========================================= */

    socket.on('register_device', (data) => {

        socket.mobiraInfo = {
            ...(socket.mobiraInfo || {}),

            deviceType:
                data && data.deviceType
                    ? data.deviceType
                    : null,

            hiddenChannel:
                !!(
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
       NMT:N KANAVAN VAIHTUMINEN

       Jeikin NMT ilmoittaa aina uuden kanavan.

       Esimerkiksi:
       set_channel("1H")
       set_channel("6H")
       ========================================= */

    socket.on('set_channel', (channel) => {

        socket.mobiraInfo =
            socket.mobiraInfo || {};

        socket.mobiraInfo.currentChannel =
            channel;

        console.log(
            `Socket ${socket.id} kanava: ${channel}`
        );
    });


    /* =========================================
       PUHELUN ALOITUS
       ========================================= */

    socket.on('start_call', (data) => {

        const fromNumber =
            String(data.fromNumber || '');

        const targetNumber =
            String(data.targetNumber || '');

        if (!fromNumber || !targetNumber) {

            socket.emit('line_busy');

            return;
        }


        /*
         * Jos fromNumber soittaa nyt targetNumberille
         * ja fromNumberilla on merkittynä vastaamaton
         * puhelu juuri tältä numerolta, tämä lasketaan
         * kuittaukseksi - KUTSU E sammuu.
         */

        clearMissedCall(fromNumber, targetNumber);


        /*
         * Etsitään vastaanottajan socket.
         */

        const targetSocketId =
            activeUsers[targetNumber];


        /*
         * Jos jompikumpi on jo puhelussa,
         * linja on varattu.
         */

        if (
            inCallUsers.has(targetNumber) ||
            inCallUsers.has(fromNumber)
        ) {

            socket.emit('line_busy');

            return;
        }


        /*
         * Vastaanottajaa ei ole verkossa
         * (esim. html on kiinni). Merkitään
         * vastaamattomaksi puheluksi, jotta
         * se odottaa vastaanottajaa kun tämä
         * seuraavan kerran rekisteröityy.
         */

        if (!targetSocketId) {

            addMissedCall(targetNumber, fromNumber);

            socket.emit('line_busy');

            return;
        }


        /* =====================================
           KANAVAN KÄSITTELY
           ===================================== */

        let callChannel =
            data.channel ?? null;


        /*
         * CITYMAN
         *
         * Citymanilla ei ole käyttäjän näkyvää
         * kanavaa.
         *
         * Jos Cityman soittaa NMT:lle,
         * serveri ottaa kohde-NMT:n nykyisen
         * kanavan Citymanin puhelun kanavaksi.
         */

        if (
            data.cityman === true ||
            data.deviceType === 'CITYMAN'
        ) {

            const targetSocket =
                io.sockets.sockets.get(
                    targetSocketId
                );


            if (
                targetSocket &&
                targetSocket.mobiraInfo &&
                targetSocket.mobiraInfo.currentChannel != null
            ) {

                callChannel =
                    targetSocket
                        .mobiraInfo
                        .currentChannel;
            }
        }


        /* =====================================
           MERKITÄÄN PUHELUN OSAPUOLET
           ===================================== */

        inCallUsers.add(fromNumber);
        inCallUsers.add(targetNumber);


        /*
         * Merkitään puhelu soimaan vastaamatta,
         * kunnes joko vastataan tai se päättyy.
         */

        ringingCalls[targetNumber] = fromNumber;


        /* =====================================
           LÄHETETÄÄN TULEVA PUHELU
           ===================================== */

        io.to(targetSocketId).emit(
            'incoming_call',
            {
                fromNumber: fromNumber,

                /*
                 * Tämä voi olla esimerkiksi:
                 * 1H
                 * 6H
                 *
                 * Cityman ei kuitenkaan näytä
                 * tätä käyttäjälle.
                 */

                channel: callChannel
            }
        );


        console.log(
            `Puhelu ${fromNumber} -> ${targetNumber}, kanava: ${callChannel}`
        );
    });


    /* =========================================
       PUHELUUN VASTAAMINEN
       ========================================= */

    socket.on('answer_call', (data) => {

        let answeringNumber = null;


        /*
         * Selvitetään, mikä numero vastasi.
         */

        for (
            const [num, id]
            of Object.entries(activeUsers)
        ) {

            if (id === socket.id) {

                answeringNumber = num;

                inCallUsers.add(num);

                break;
            }
        }


        /*
         * Puheluun vastattiin - se ei ole enää
         * soimassa vastaamatta.
         */

        if (
            answeringNumber &&
            ringingCalls[answeringNumber]
        ) {

            delete ringingCalls[answeringNumber];
        }


        /*
         * targetNumber on soittajan numero.
         */

        const callerSocketId =
            activeUsers[data.targetNumber];


        if (callerSocketId) {

            io.to(callerSocketId).emit(
                'call_answered',
                {
                    channel:
                        data &&
                        data.channel != null
                            ? data.channel
                            : null
                }
            );
        }
    });


    /* =========================================
       WEBRTC OFFER
       ========================================= */

    socket.on('webrtc_offer', (data) => {

        const targetSocketId =
            activeUsers[data.targetNumber];


        if (targetSocketId) {

            io.to(targetSocketId).emit(
                'webrtc_offer',
                {
                    fromNumber:
                        data.fromNumber,

                    offer:
                        data.offer
                }
            );
        }
    });


    /* =========================================
       WEBRTC ANSWER
       ========================================= */

    socket.on('webrtc_answer', (data) => {

        const targetSocketId =
            activeUsers[data.targetNumber];


        if (targetSocketId) {

            io.to(targetSocketId).emit(
                'webrtc_answer',
                {
                    answer:
                        data.answer
                }
            );
        }
    });


    /* =========================================
       WEBRTC ICE
       ========================================= */

    socket.on(
        'webrtc_ice_candidate',
        (data) => {

            const targetSocketId =
                activeUsers[
                    data.targetNumber
                ];


            if (targetSocketId) {

                io.to(targetSocketId).emit(
                    'webrtc_ice_candidate',
                    {
                        candidate:
                            data.candidate
                    }
                );
            }
        }
    );


    /* =========================================
       PUHELUN LOPETUS
       ========================================= */

    socket.on('end_call', (data) => {

        /*
         * Selvitetään tämän socketin oma numero.
         */

        let thisNumber = null;

        for (
            const [num, id]
            of Object.entries(activeUsers)
        ) {

            if (id === socket.id) {

                thisNumber = num;

                inCallUsers.delete(num);
            }
        }


        /*
         * Poistetaan myös vastapuoli.
         */

        if (
            data &&
            data.targetNumber
        ) {

            const targetNumber =
                String(data.targetNumber);

            inCallUsers.delete(
                targetNumber
            );


            /*
             * Jos thisNumber oli soittamassa
             * targetNumberille eikä tämä ehtinyt
             * vastata, merkitään puhelu
             * vastaamattomaksi targetNumberille.
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

                /*
                 * thisNumber oli itse vastaamatta
                 * soivan puhelun vastaanottaja ja
                 * katkaisi sen itse (esim. hylkäsi) -
                 * tätä ei lasketa vastaamattomaksi.
                 */

                delete ringingCalls[thisNumber];
            }


            const targetSocketId =
                activeUsers[targetNumber];


            if (targetSocketId) {

                io.to(targetSocketId).emit(
                    'call_ended'
                );
            }
        }
    });


    /* =========================================
       DISCONNECT
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
                 * Jos tälle numerolle oli juuri
                 * soimassa vastaamaton puhelu ja
                 * yhteys katkesi (esim. html
                 * suljettiin), merkitään se
                 * vastaamattomaksi ja kerrotaan
                 * soittajalle että puhelu päättyi.
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

server.listen(
    PORT,
    () => {

        console.log(
            `Mobira-palvelin pyörii portissa ${PORT}`
        );
    }
);
