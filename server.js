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
   SERVERILOKI (Serverilogi.html kuuntelee tätä)
   ========================================= */

/*
 * Jos asetat Renderissä ympäristömuuttujan MONITOR_KEY,
 * lokia voi katsoa vain se, joka tietää avaimen.
 */
const MONITOR_KEY = process.env.MONITOR_KEY || '';

/* Viimeiset 300 riviä uusille lokikatselijoille. */
const logBuffer = [];

/*
 * Käynnissä olevan puhelun tiedot, avaimena vastaanottajan numero:
 * callInfo['246927'] = { from, channel, network, established }
 */
const callInfo = {};

function serverLog(msg, kind) {
    const entry = {
        ts: Date.now(),
        msg: String(msg),
        kind: kind || 'info'
    };

    logBuffer.push(entry);
    if (logBuffer.length > 300) logBuffer.shift();

    io.to('monitors').emit('server_log', entry);
}

/* =========================================
   VERKKOTYYPPI (ARP / NMT)
   ========================================= */

/*
 * numero -> verkkotyyppi, esim. '21028' -> 'ARP'.
 * Täyttyy, kun ARP-puhelin lähettää register_network- tai
 * start_call-viestin. Lisäksi kaikki 210-alkuiset numerot
 * tunnistetaan ARP-numeroiksi.
 */
const numberNetwork = {};

function networkOf(number) {
    number = String(number ?? '');

    if (numberNetwork[number]) return numberNetwork[number];
    if (/^210/.test(number)) return 'ARP';

    return null;
}

function setNetwork(number, network) {
    number = String(number ?? '').trim();
    network = String(network ?? '').trim().toUpperCase();

    if (!number || !network) return;

    numberNetwork[number] = network;
}

/* =========================================
   VASTAANOTTAJAN TAVOITTAMISEN VIIVE (NMT)
   ========================================= */

/*
 * Oikeassa NMT-verkossa vastaanottajan tavoittaminen kesti
 * noin 3-10 sekuntia. Arvoja voi muuttaa Renderin
 * ympäristömuuttujilla PAGING_MIN_MS ja PAGING_MAX_MS.
 */
const PAGING_MIN_MS = Number(process.env.PAGING_MIN_MS) || 3000;
const PAGING_MAX_MS = Number(process.env.PAGING_MAX_MS) || 10000;

function pagingDelay() {
    return PAGING_MIN_MS + Math.random() * (PAGING_MAX_MS - PAGING_MIN_MS);
}

/*
 * Puhelut, joiden vastaanottajaa vielä haetaan.
 * pendingCalls['246927'] = { from, timers: [] }
 */
const pendingCalls = {};

function cancelPending(targetNumber) {
    const p = pendingCalls[targetNumber];
    if (!p) return null;

    p.timers.forEach(clearTimeout);
    delete pendingCalls[targetNumber];

    return p;
}

/* =========================================
   VASTAAMATTOMAT PUHELUT
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

    serverLog(
        `Missed call: ${fromNumber} -> ${targetNumber}`,
        'warn'
    );

    const targetSocketId = activeUsers[targetNumber];

    if (targetSocketId) {
        io.to(targetSocketId).emit('missed_call', {
            fromNumber: fromNumber,
            network: networkOf(fromNumber)
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
    const n = socket.mobiraInfo && socket.mobiraInfo.phoneNumber;

    if (n && activeUsers[n] === socket.id) return n;

    for (const [num, id] of Object.entries(activeUsers)) {
        if (id === socket.id) return num;
    }

    return null;
}

/* =========================================
   TESTISOITTO
   ========================================= */

const TEST_FROM_DEFAULT = process.env.TEST_CALL_FROM || '000000';
const TEST_RING_MS = Number(process.env.TEST_CALL_RING_MS) || 30000;
const TEST_MAX_AHEAD_MS = 24 * 60 * 60 * 1000;   /* max 24 h eteenpäin */
const MAX_SCHEDULED_TESTS = 20;
const scheduledTests = new Set();

/*
 * Palvelin soittaa vastaanottajalle itse. Soittajaa ei ole oikeaa
 * puhelinta, joten kukaan ei kuule dial tonea: call_ringing-viestiä
 * ei lähetetä kenellekään, vain vastaanottaja saa incoming_call.
 */
function runTestCall(fromNumber, targetNumber) {
    const targetSocketId = activeUsers[targetNumber];

    serverLog(`Test call: incoming call signal from ${fromNumber}`);
    serverLog(`Asking for Registration ID from number ${targetNumber}`);

    if (!targetSocketId) {
        serverLog(
            `Number ${targetNumber} is not registered - test call failed`,
            'err'
        );
        return;
    }

    if (inCallUsers.has(targetNumber) || inCallUsers.has(fromNumber)) {
        serverLog(
            `Line busy: ${fromNumber} -> ${targetNumber} (test call)`,
            'err'
        );
        return;
    }

    inCallUsers.add(fromNumber);
    inCallUsers.add(targetNumber);

    ringingCalls[targetNumber] = fromNumber;

    callInfo[targetNumber] = {
        from: fromNumber,
        channel: null,
        network: networkOf(fromNumber) || 'NMT',
        established: false
    };

    serverLog(`Sending call to ${targetNumber}`);

    io.to(targetSocketId).emit('incoming_call', {
        fromNumber: fromNumber,
        channel: null,
        network: networkOf(fromNumber),
        testCall: true
    });

    /* Ei vastausta ajassa -> vastaamaton puhelu */
    setTimeout(() => {
        if (ringingCalls[targetNumber] !== fromNumber) return;

        delete ringingCalls[targetNumber];

        if (inCallUsers.has(targetNumber)) {
            inCallUsers.delete(targetNumber);
            inCallUsers.delete(fromNumber);
            delete callInfo[targetNumber];

            addMissedCall(targetNumber, fromNumber);

            const id = activeUsers[targetNumber];
            if (id) io.to(id).emit('call_ended');

            serverLog(
                `Test call not answered (${fromNumber} -> ${targetNumber})`,
                'warn'
            );
        } else {
            inCallUsers.delete(fromNumber);
        }
    }, TEST_RING_MS);
}

io.on('connection', (socket) => {

    /* =========================================
       LOKIKATSELIJA (Serverilogi.html)
       ========================================= */

    socket.on('monitor_subscribe', (key, ack) => {
        if (MONITOR_KEY && key !== MONITOR_KEY) {
            if (typeof ack === 'function') ack({ ok: false });
            return;
        }

        /*
         * Lokikatselija ei ole puhelin: se ei rekisteröi numeroa,
         * joten se ei vaikuta puheluihin eikä näy puhelimena.
         */
        socket.join('monitors');

        if (typeof ack === 'function') {
            ack({ ok: true, history: logBuffer });
        }
    });

    /* =========================================
       TESTISOITTO (Serverilogi.html)
       ========================================= */

    socket.on('test_call', (data, ack) => {
        const reply = (r) => { if (typeof ack === 'function') ack(r); };

        data = data || {};

        if (MONITOR_KEY && data.key !== MONITOR_KEY) {
            return reply({ ok: false, error: 'Väärä lokiavain' });
        }

        const number = String(data.number ?? '').trim();
        const from = String(data.fromNumber || TEST_FROM_DEFAULT).trim();

        if (!/^\+?\d{3,15}$/.test(number)) {
            return reply({ ok: false, error: 'Virheellinen numero' });
        }
        if (!/^\+?\d{3,15}$/.test(from)) {
            return reply({ ok: false, error: 'Virheellinen soittajan numero' });
        }
        if (scheduledTests.size >= MAX_SCHEDULED_TESTS) {
            return reply({ ok: false, error: 'Liikaa ajastettuja testisoittoja' });
        }

        const runAt = Number(data.runAt) || Date.now();
        const delay = Math.max(0, runAt - Date.now());

        if (delay > TEST_MAX_AHEAD_MS) {
            return reply({ ok: false, error: 'Aika on yli 24 h päässä' });
        }

        const handle = {};
        scheduledTests.add(handle);

        handle.timer = setTimeout(() => {
            scheduledTests.delete(handle);
            runTestCall(from, number);
        }, delay);

        serverLog(
            `Test call scheduled: ${from} -> ${number} in ${Math.round(delay / 1000)} s`,
            'sys'
        );

        reply({ ok: true });
    });

    /* =========================================
       NUMERON REKISTERÖINTI
       ========================================= */

    socket.on('register_number', (number) => {
        if (number && typeof number === 'object') {
            number = number.number ?? number.phoneNumber ?? number.fromNumber;
        }

        number = String(number ?? '').trim();

        if (!number) return;

        bindNumber(socket, number);

        console.log(
            `Numero ${number} rekisteröity socketiin ${socket.id}`
        );

        serverLog(`Phone ${number} registered`);

        if (missedCalls[number] && missedCalls[number].size > 0) {
            missedCalls[number].forEach((fromNumber) => {
                socket.emit('missed_call', {
                    fromNumber: fromNumber,
                    network: networkOf(fromNumber)
                });
            });
        }
    });

    /* =========================================
       VERKKOTYYPIN REKISTERÖINTI (ARP-puhelin)
       ========================================= */

    socket.on('register_network', (data) => {
        if (!data || typeof data !== 'object') return;

        const number =
            String(
                data.number ??
                data.phoneNumber ??
                numberOfSocket(socket) ??
                ''
            ).trim();

        setNetwork(number, data.network);

        socket.mobiraInfo = socket.mobiraInfo || {};
        socket.mobiraInfo.network =
            String(data.network ?? '').trim().toUpperCase() || null;

        console.log(
            `Numero ${number} verkko: ${socket.mobiraInfo.network}`
        );

        serverLog(
            `Phone ${number} announced network: ${socket.mobiraInfo.network}`
        );
    });

    /* =========================================
       VASTAAMATTOMIEN PUHELUIDEN NOLLAUS (CL pohjassa)
       ========================================= */

    socket.on('clear_missed_calls', () => {
        const n = numberOfSocket(socket);

        if (n && missedCalls[n]) {
            delete missedCalls[n];

            console.log(
                `Vastaamattomat puhelut nollattu: ${n}`
            );

            serverLog(`Missed calls cleared by ${n}`);
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
        data = data || {};

        const fromNumber =
            String(data.fromNumber || '');

        const targetNumber =
            String(data.targetNumber || '');

        /*
         * Puuttuva numero:
         * ei dial tonea eikä varattuääntä.
         */

        if (!fromNumber || !targetNumber) {
            serverLog(
                'Invalid call signal (missing number)',
                'err'
            );

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
         * Soittaja voi kertoa verkkotyyppinsä (esim. ARP).
         */

        if (data.network) {
            setNetwork(fromNumber, data.network);
        }

        /*
         * Loki: puhelusignaali saapui, verkko tunnistettiin
         * ja kohdenumeron rekisteröintiä kysytään.
         */

        const callNetwork = networkOf(fromNumber) || 'NMT';

        serverLog(`Incoming call signal from ${fromNumber}`);
        serverLog(`Phone's network identified: ${callNetwork}`);
        serverLog(`Asking for Registration ID from number ${targetNumber}`);

        /*
         * Takaisinsoitto kuittaa vastaamattoman puhelun.
         */

        clearMissedCall(fromNumber, targetNumber);

        /*
         * Jos jompikumpi on jo puhelussa,
         * ilmoitetaan oikeasta varatusta linjasta.
         */

        if (
            inCallUsers.has(targetNumber) ||
            inCallUsers.has(fromNumber)
        ) {
            console.log(
                `Varattu: ${fromNumber} -> ${targetNumber}. ` +
                `Puhelussa: [${[...inCallUsers].join(', ')}]`
            );

            serverLog(
                `Line busy: ${fromNumber} -> ${targetNumber}`,
                'err'
            );

            socket.emit('line_busy');

            return;
        }

        /*
         * Merkitään osapuolet varatuiksi heti, jotta kolmas
         * soittaja saa varattuäänen hakuvaiheessakin.
         */
        inCallUsers.add(fromNumber);
        inCallUsers.add(targetNumber);

        const delay = pagingDelay();
        const pending = { from: fromNumber, timers: [] };
        pendingCalls[targetNumber] = pending;

        console.log(
            `Haetaan ${targetNumber} (${fromNumber} soittaa), viive ${Math.round(delay)} ms`
        );

        /*
         * Välivaihe: vastaanottajan puhelin "vastaa" rekisteröintikyselyyn.
         */
        pending.timers.push(setTimeout(() => {
            if (activeUsers[targetNumber]) {
                serverLog(
                    `Registration ID received from number ${targetNumber}`,
                    'ok'
                );

                /*
                 * Puhelun lähetys alkaa tästä. Itse puhelu ei välity
                 * vielä: se saapuu vastaanottajalle vasta, kun
                 * lähetysvaihe on kestänyt oman aikansa.
                 */
                serverLog(`Sending call to ${targetNumber}`);
            }
        }, delay * 0.6));

        /*
         * Viive päättyy: joko vastaanottaja löytyi tai ei.
         */
        pending.timers.push(setTimeout(() => {
            delete pendingCalls[targetNumber];

            const callerSocketId = activeUsers[fromNumber];
            const targetSocketId = activeUsers[targetNumber];

            /* Vastaanottajaa ei löytynyt */
            if (!targetSocketId) {
                console.log(
                    `Kohdetta ${targetNumber} ei löytynyt. ` +
                    `Verkossa: [${Object.keys(activeUsers).join(', ')}]`
                );

                serverLog(
                    `Number ${targetNumber} is not registered - call failed`,
                    'err'
                );

                inCallUsers.delete(fromNumber);
                inCallUsers.delete(targetNumber);

                addMissedCall(targetNumber, fromNumber);

                if (callerSocketId) {
                    io.to(callerSocketId).emit('number_unavailable', {
                        targetNumber,
                        reason: 'offline'
                    });

                    /* Katkaistaan puhelu soittajan päässä */
                    io.to(callerSocketId).emit('call_ended');
                }

                return;
            }

            /* Kanavan käsittely (Cityman käyttää vastaanottajan kanavaa) */
            let callChannel = data.channel ?? null;

            if (data.cityman === true || data.deviceType === 'CITYMAN') {
                const targetSocket = io.sockets.sockets.get(targetSocketId);

                if (
                    targetSocket &&
                    targetSocket.mobiraInfo &&
                    targetSocket.mobiraInfo.currentChannel != null
                ) {
                    callChannel = targetSocket.mobiraInfo.currentChannel;
                }
            }

            ringingCalls[targetNumber] = fromNumber;

            callInfo[targetNumber] = {
                from: fromNumber,
                channel: callChannel,
                network: callNetwork,
                established: false
            };

            /* Soittaja kuulee dial tonen vasta nyt, kun kohde on tavoitettu */
            if (callerSocketId) {
                io.to(callerSocketId).emit('call_ringing', {
                    targetNumber: targetNumber
                });
            }

            io.to(targetSocketId).emit('incoming_call', {
                fromNumber: fromNumber,
                channel: callChannel,
                network: networkOf(fromNumber)
            });

            console.log(
                `Puhelu ${fromNumber} -> ${targetNumber}, kanava: ${callChannel}, verkko: ${networkOf(fromNumber)}`
            );
        }, delay));
    });

    /* =========================================
       PUHELUUN VASTAAMINEN
       ========================================= */

    socket.on('answer_call', (data) => {
        const answeringNumber = numberOfSocket(socket);

        if (answeringNumber) {
            inCallUsers.add(answeringNumber);
        }

        if (
            answeringNumber &&
            ringingCalls[answeringNumber]
        ) {
            delete ringingCalls[answeringNumber];
        }

        /*
         * Loki: vastaus ja yhteyden muodostus alkaa.
         */

        if (answeringNumber) {
            const info = callInfo[answeringNumber];

            serverLog(`Call picked up (${answeringNumber})`);
            serverLog(
                `Establishing connection - Network type: ${
                    info ? info.network : (networkOf(data && data.targetNumber) || 'NMT')
                }`
            );
        }

        /*
         * Vastattu puhelu ei ole enää vastaamaton.
         */

        if (answeringNumber && data && data.targetNumber) {
            clearMissedCall(
                answeringNumber,
                String(data.targetNumber)
            );
        }

        /*
         * targetNumber on soittajan numero.
         */

        const callerSocketId =
            data && activeUsers[data.targetNumber];

        if (callerSocketId) {
            io.to(callerSocketId).emit('call_answered', {
                channel:
                    data && data.channel != null
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
            data && activeUsers[data.targetNumber];

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
            data && activeUsers[data.targetNumber];

        if (targetSocketId) {
            io.to(targetSocketId).emit('webrtc_answer', {
                answer: data.answer
            });
        }

        /*
         * Loki: vastaaja lähetti WebRTC-vastauksen -> yhteys muodostettu.
         * Lokiin vain kerran per puhelu.
         */

        const answeringNumber = numberOfSocket(socket);
        const info = answeringNumber && callInfo[answeringNumber];

        if (info && !info.established) {
            info.established = true;

            serverLog(
                `Connection established on channel ${
                    info.channel != null ? info.channel : 'unknown'
                }`,
                'ok'
            );
        }
    });

    /* =========================================
       WEBRTC ICE
       ========================================= */

    socket.on('webrtc_ice_candidate', (data) => {
        const targetSocketId =
            data && activeUsers[data.targetNumber];

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
        const thisNumber = numberOfSocket(socket);

        /*
         * Loki: kirjataan lopetus vain kerran, vaikka
         * molemmat osapuolet lähettävät end_call.
         */

        const targetForLog =
            data && data.targetNumber
                ? String(data.targetNumber)
                : null;

        /*
         * Soittaja lopetti ennen kuin vastaanottaja oli tavoitettu:
         * perutaan haku. Vastaanottajan puhelin ei ehtinyt soida,
         * joten vastaamatonta puhelua ei merkitä.
         */
        if (
            thisNumber && targetForLog &&
            pendingCalls[targetForLog] &&
            pendingCalls[targetForLog].from === thisNumber
        ) {
            cancelPending(targetForLog);
            inCallUsers.delete(thisNumber);
            inCallUsers.delete(targetForLog);

            serverLog(
                `Call cancelled before recipient was reached (${thisNumber} -> ${targetForLog})`,
                'warn'
            );

            return;
        }

        const wasActive =
            (thisNumber && inCallUsers.has(thisNumber)) ||
            (targetForLog && inCallUsers.has(targetForLog));

        if (wasActive && thisNumber && targetForLog) {
            serverLog(
                `Call ended (${thisNumber} -> ${targetForLog})`,
                'warn'
            );
        }

        if (thisNumber) {
            inCallUsers.delete(thisNumber);
            delete callInfo[thisNumber];
        }

        if (data && data.targetNumber) {
            const targetNumber =
                String(data.targetNumber);

            inCallUsers.delete(targetNumber);
            delete callInfo[targetNumber];

            /*
             * Jos vastaanottaja ei ehtinyt vastata,
             * merkitään puhelu vastaamattomaksi.
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
                /*
                 * Puhelin sammui tai katkesi verkosta kesken puhelun:
                 * katkaistaan puhelu toiselta osapuolelta.
                 */
                let peer = null;

                if (callInfo[number]) {
                    peer = callInfo[number].from;
                } else {
                    for (const [t, info] of Object.entries(callInfo)) {
                        if (info.from === number) {
                            peer = t;
                            break;
                        }
                    }
                }

                delete activeUsers[number];

                inCallUsers.delete(number);
                delete callInfo[number];

                if (peer) {
                    inCallUsers.delete(peer);
                    delete callInfo[peer];

                    /* Soiva puhelu käsitellään alempana erikseen */
                    if (ringingCalls[number] !== peer) {
                        if (ringingCalls[peer] === number) {
                            addMissedCall(peer, number);
                            delete ringingCalls[peer];
                        }

                        const peerSocketId = activeUsers[peer];
                        if (peerSocketId) {
                            io.to(peerSocketId).emit('call_ended');
                        }

                        serverLog(
                            `Call ended - ${number} went offline (${number} <-> ${peer})`,
                            'warn'
                        );
                    }
                }

                /* Vastaanottaja katosi kesken haun */
                if (pendingCalls[number]) {
                    const p = cancelPending(number);
                    inCallUsers.delete(p.from);
                    addMissedCall(number, p.from);

                    const pendingCallerSocketId = activeUsers[p.from];
                    if (pendingCallerSocketId) {
                        io.to(pendingCallerSocketId).emit('call_ended');
                    }
                }

                /* Soittaja katosi kesken haun */
                for (const [target, p] of Object.entries(pendingCalls)) {
                    if (p.from === number) {
                        cancelPending(target);
                        inCallUsers.delete(target);
                    }
                }

                /*
                 * Jos laite poistuu verkosta puhelun
                 * soidessa, tallennetaan puhelu
                 * vastaamattomaksi.
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

                serverLog(
                    `Phone ${number} disconnected`,
                    'warn'
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
