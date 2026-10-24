const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

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
    },
    maxHttpBufferSize: 5e6   /* vastaajaviestit (äänitiedosto) kulkevat socketin kautta */
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
    number = String(number ?? '').replace(/^\+/, '');

    /* 940- ja 950-alkuiset numerot ovat aina GSM */
    if (/^(940|950)/.test(number)) return 'GSM';

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

// Tekstiviestit: tuki on oletuksena pois päältä. Puhelimen on ilmoitettava
// supportsSms: true register_device-tapahtumassa ennen SMS:n käyttöä.
const smsMailboxes = Object.create(null);
const smsCapableNumbers = new Set();
const MAX_SMS_PER_MAILBOX = 100;

function socketSupportsSms(socketId) {
    const s = io.sockets.sockets.get(socketId);
    return !!(s && s.mobiraInfo && s.mobiraInfo.supportsSms === true);
}

function deliverQueuedSms(number, socketId) {
    if (!socketSupportsSms(socketId)) return;
    const box = smsMailboxes[number];
    if (!box || !box.length) return;
    box.splice(0).forEach((message) => io.to(socketId).emit('incoming_sms', message));
}

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

/* =========================================
   TUNNETUT NUMEROT (onko numero koskaan rekisteröitynyt)
   ========================================= */

/*
 * Numero, joka on joskus rekisteröitynyt verkkoon, on "tunnettu".
 * Soitto tunnettuun mutta nyt poissa olevaan numeroon -> reason: 'offline'
 *   (puhelin: "Tavoittelemanne henkilö ei ole saatavilla")
 * Soitto numeroon, joka ei ole koskaan rekisteröitynyt -> reason: 'not_registered'
 *   (puhelin: "Valitsemanne numero ei ole käytössä")
 *
 * Numerot tallennetaan pysyvästi Upstash Redis -tietokantaan, jos Renderissä on
 * ympäristömuuttujat UPSTASH_REDIS_REST_URL ja UPSTASH_REDIS_REST_TOKEN.
 * Tietokanta säilyy Renderin uudelleenkäynnistyksissä, joten numeroita ei
 * tarvitse ylläpitää käsin.
 *
 * Varalla toimivat edelleen tiedosto (Renderin ilmainen taso nollaa levyn)
 * ja ympäristömuuttuja KNOWN_NUMBERS (pilkulla eroteltu, esim. "940882,246927").
 * Vaatii Node 18+ (fetch on mukana).
 */
const KNOWN_NUMBERS_FILE =
    process.env.KNOWN_NUMBERS_FILE || path.join(__dirname, 'known-numbers.json');
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL || '';
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const REDIS_KEY = 'mobira:known_numbers';
const knownNumbers = new Set();

async function redisCommand(args) {
    const res = await fetch(REDIS_URL, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${REDIS_TOKEN}`,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(args)
    });
    const json = await res.json();
    if (json.error) throw new Error(json.error);
    return json.result;
}

(function loadKnownNumbers() {
    try {
        const list = JSON.parse(fs.readFileSync(KNOWN_NUMBERS_FILE, 'utf8'));
        if (Array.isArray(list)) list.forEach((n) => knownNumbers.add(String(n)));
    } catch (e) { /* tiedostoa ei ole vielä */ }

    String(process.env.KNOWN_NUMBERS || '')
        .split(',')
        .map((n) => n.trim())
        .filter(Boolean)
        .forEach((n) => knownNumbers.add(n));

    if (REDIS_URL && REDIS_TOKEN) {
        redisCommand(['SMEMBERS', REDIS_KEY])
            .then((list) => {
                (list || []).forEach((n) => knownNumbers.add(String(n)));
                console.log(`Tunnettuja numeroita ladattu tietokannasta: ${(list || []).length}`);
            })
            .catch((e) => console.log('Tunnettujen numeroiden lataus epäonnistui:', e.message));
    }
})();

function rememberNumber(number) {
    number = String(number ?? '').trim();
    if (!number || knownNumbers.has(number)) return;

    knownNumbers.add(number);

    try {
        fs.writeFileSync(KNOWN_NUMBERS_FILE, JSON.stringify([...knownNumbers]));
    } catch (e) {
        console.log('Tunnettujen numeroiden tallennus epäonnistui:', e.message);
    }

    if (REDIS_URL && REDIS_TOKEN) {
        redisCommand(['SADD', REDIS_KEY, number])
            .catch((e) => console.log('Numeron tallennus tietokantaan epäonnistui:', e.message));
    }
}

function bindNumber(socket, number) {
    activeUsers[number] = socket.id;
    rememberNumber(number);

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
   VASTAAJA
   ========================================= */

/*
 * Kun soitettu numero ei vastaa VOICEMAIL_RING_MS:n kuluessa, soitto katkaistaan
 * vastaanottajalta (tulee vastaamaton puhelu) ja soittajan puhelin saa
 * voicemail_prompt-viestin. Puhelin piippaa, äänittää ja lähettää valmiin viestin
 * (voicemail_send). Vastaanottaja soittaa 777 ja valitsee viestin näppäimellä 1...N.
 *
 * Vain puhelimet, jotka ilmoittavat register_device-viestissä supportsVoicemail: true,
 * saavat vastaajan; muiden soitot toimivat kuten ennen (soi kunnes vastataan).
 *
 * Viestit pidetään muistissa ja tallennetaan levylle (VOICEMAIL_DIR). Renderin
 * ilmaisella tasolla levy nollautuu uudelleenkäynnistyksessä, jolloin viestit katoavat.
 */
const VOICEMAIL_RING_MS = Number(process.env.VOICEMAIL_RING_MS) || 25000;
const VOICEMAIL_PERMIT_MS = 3 * 60 * 1000;      /* viestin lähetysaika äänityksen alusta */
const VOICEMAIL_MAX_BYTES = 4 * 1024 * 1024;
const VOICEMAIL_MAX_PER_BOX = 9;                /* puhelin valitsee viestin näppäimillä 1-9 */
const VOICEMAIL_DIR =
    process.env.VOICEMAIL_DIR || path.join(__dirname, 'voicemail-data');

const voicemails = {};      /* id -> { id, to, from, ts, mime, duration, played, audio: Buffer } */
const ringTimers = {};      /* vastaanottajan numero -> ajastin (soitto ilman vastausta) */
const vmSessions = {};      /* soittajan numero -> numero, jonka vastaajaan viestiä jätetään */
const vmPermits = {};       /* 'soittaja>vastaanottaja' -> voimassaolon loppu (ms) */

try {
    fs.mkdirSync(VOICEMAIL_DIR, { recursive: true });
    fs.readdirSync(VOICEMAIL_DIR).forEach((f) => {
        if (!f.endsWith('.json')) return;
        try {
            const m = JSON.parse(fs.readFileSync(path.join(VOICEMAIL_DIR, f), 'utf8'));
            m.audio = Buffer.from(m.audio, 'base64');
            if (m.id && !m.played) voicemails[m.id] = m;
        } catch (e) { /* viallinen tiedosto ohitetaan */ }
    });
} catch (e) {
    console.log('Vastaajaviestien lataus epäonnistui:', e.message);
}

function persistVoicemail(m) {
    fs.writeFile(
        path.join(VOICEMAIL_DIR, m.id + '.json'),
        JSON.stringify({ ...m, audio: m.audio.toString('base64') }),
        () => {}
    );
}

function removeVoicemailFile(id) {
    fs.unlink(path.join(VOICEMAIL_DIR, id + '.json'), () => {});
}

function unheardVoicemails(number) {
    return Object.values(voicemails)
        .filter((m) => m.to === number && !m.played)
        .sort((a, b) => a.ts - b.ts);
}

function publicVoicemail(m) {
    return { id: m.id, from: m.from, ts: m.ts };
}

function sendVoicemailList(number) {
    const id = activeUsers[number];
    if (!id) return;
    io.to(id).emit('voicemail_list', {
        messages: unheardVoicemails(number).map(publicVoicemail)
    });
}

function clearRingTimer(targetNumber) {
    if (ringTimers[targetNumber]) {
        clearTimeout(ringTimers[targetNumber]);
        delete ringTimers[targetNumber];
    }
}

function startVoicemailRingTimer(fromNumber, targetNumber) {
    const callerSocket = io.sockets.sockets.get(activeUsers[fromNumber]);

    if (
        !callerSocket ||
        !callerSocket.mobiraInfo ||
        callerSocket.mobiraInfo.supportsVoicemail !== true
    ) return;

    clearRingTimer(targetNumber);

    ringTimers[targetNumber] = setTimeout(() => {
        delete ringTimers[targetNumber];

        /* Puheluun ehdittiin vastata tai se katkaistiin */
        if (ringingCalls[targetNumber] !== fromNumber) return;

        delete ringingCalls[targetNumber];
        inCallUsers.delete(targetNumber);
        delete callInfo[targetNumber];

        addMissedCall(targetNumber, fromNumber);

        const targetSocketId = activeUsers[targetNumber];
        if (targetSocketId) io.to(targetSocketId).emit('call_ended');

        const callerSocketId = activeUsers[fromNumber];

        if (!callerSocketId || !inCallUsers.has(fromNumber)) {
            inCallUsers.delete(fromNumber);
            return;
        }

        vmSessions[fromNumber] = targetNumber;
        vmPermits[fromNumber + '>' + targetNumber] = Date.now() + VOICEMAIL_PERMIT_MS;

        serverLog(
            `No answer - voicemail offered: ${fromNumber} -> ${targetNumber}`,
            'warn'
        );

        io.to(callerSocketId).emit('voicemail_prompt', { targetNumber });
    }, VOICEMAIL_RING_MS);
}

/* =========================================
   TESTISOITTO
   ========================================= */

const TEST_FROM_DEFAULT = process.env.TEST_CALL_FROM || '000000';
/*
 * Testisoiton soittoaika. 0 = soi, kunnes vastataan (ei aikarajaa).
 * Voit asettaa aikarajan ympäristömuuttujalla TEST_CALL_RING_MS.
 */
const TEST_RING_MS = Number(process.env.TEST_CALL_RING_MS) || 0;
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

    /*
     * Ei vastausta ajassa -> vastaamaton puhelu.
     * Ajastin käynnistetään vain, jos aikaraja on asetettu (TEST_RING_MS > 0).
     * Oletuksena testisoitto soi, kunnes vastataan.
     */
    if (TEST_RING_MS > 0) setTimeout(() => {
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
            ack({ ok: true, history: logBuffer, active: Object.keys(activeUsers) });
        }
    });

    /* =========================================
       TUNNETTUJEN NUMEROIDEN SYNKRONOINTI (Serverilogi.html)
       ========================================= */

    /*
     * Lokisivu muistaa kaikki numerot, jotka ovat joskus olleet
     * rekisterissä, ja kertoo ne palvelimelle. Palvelin tallentaa ne
     * tunnetuiksi: soitto tunnettuun mutta nyt poissa olevaan numeroon
     * -> reason 'offline' (Ei saada yhteyttä), tuntematon numero
     * -> 'not_registered' (Ei ole käytössä).
     */
    socket.on('sync_known_numbers', (data, ack) => {
        const reply = (r) => { if (typeof ack === 'function') ack(r); };

        data = data || {};

        if (MONITOR_KEY && data.key !== MONITOR_KEY) {
            return reply({ ok: false, error: 'Väärä lokiavain' });
        }

        const list = Array.isArray(data.numbers) ? data.numbers.slice(0, 500) : [];
        let added = 0;

        list.forEach((n) => {
            n = String(n ?? '').trim().replace(/^\+/, '');
            if (!/^\d{3,15}$/.test(n) || knownNumbers.has(n)) return;
            rememberNumber(n);
            added++;
        });

        if (added) serverLog(`Known numbers synced from log: ${added} new`, 'sys');

        reply({ ok: true, added });
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
                    ? String(data.deviceType).toUpperCase()
                    : (socket.mobiraInfo && socket.mobiraInfo.deviceType) || null,

            supportsSms: !!(data && data.supportsSms === true),

            supportsVoicemail: !!(data && data.supportsVoicemail === true),

            hiddenChannel:
                !!(
                    data &&
                    data.hiddenChannel
                ),

            currentChannel:
                data &&
                data.currentChannel != null
                    ? data.currentChannel
                    : (socket.mobiraInfo && socket.mobiraInfo.currentChannel != null
                        ? socket.mobiraInfo.currentChannel
                        : null)
        };

        const registeredNumber = numberOfSocket(socket);
        if (registeredNumber && socket.mobiraInfo.supportsSms) smsCapableNumbers.add(registeredNumber);
        if (registeredNumber) deliverQueuedSms(registeredNumber, socket.id);

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

                const everRegistered = knownNumbers.has(targetNumber);

                serverLog(
                    everRegistered
                        ? `Number ${targetNumber} is not reachable (offline) - call failed`
                        : `Number ${targetNumber} has never been registered - call failed`,
                    'err'
                );

                inCallUsers.delete(fromNumber);
                inCallUsers.delete(targetNumber);

                /* Vastaamaton puhelu vain numerolle, joka oikeasti on olemassa */
                if (everRegistered) addMissedCall(targetNumber, fromNumber);

                if (callerSocketId) {
                    io.to(callerSocketId).emit('number_unavailable', {
                        targetNumber,
                        reason: everRegistered ? 'offline' : 'not_registered'
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

            startVoicemailRingTimer(fromNumber, targetNumber);

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
            clearRingTimer(answeringNumber);
        }

        if (
            answeringNumber &&
            ringingCalls[answeringNumber]
        ) {
            delete ringingCalls[answeringNumber];
        }

        /*
         * Loki: vastaus ja yhteyden muodostus alkaa.
         * Rivillä on soittaja ja vastaaja, jotta Serverilogi
         * tunnistaa puhelun alkavan (soittaja -> vastaaja).
         */

        if (answeringNumber) {
            const info = callInfo[answeringNumber];
            const callerNum = info
                ? info.from
                : (data && data.targetNumber ? String(data.targetNumber) : null);

            serverLog(
                `Call picked up (${callerNum ? callerNum + ' -> ' : ''}${answeringNumber})`
            );
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
         * Vastaajaviestin jättäminen päättyi: vapautetaan vain soittaja.
         * Vastaanottajaan ei kosketa (hän on voinut ehtiä toiseen puheluun).
         */
        if (thisNumber && targetForLog && vmSessions[thisNumber] === targetForLog) {
            delete vmSessions[thisNumber];
            inCallUsers.delete(thisNumber);

            serverLog(
                `Voicemail recording ended (${thisNumber} -> ${targetForLog})`
            );

            return;
        }

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
                clearRingTimer(targetNumber);

                addMissedCall(
                    targetNumber,
                    thisNumber
                );

                delete ringingCalls[targetNumber];

            } else if (
                thisNumber &&
                ringingCalls[thisNumber] === targetNumber
            ) {
                clearRingTimer(thisNumber);
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
       VASTAAJA
       ========================================= */

    socket.on('voicemail_sync', () => {
        const me = numberOfSocket(socket);
        if (me) sendVoicemailList(me);
    });

    socket.on('voicemail_send', (data) => {
        const from = numberOfSocket(socket);
        data = data || {};

        const to = String(data.targetNumber || '').trim();
        const permitKey = from + '>' + to;

        if (!from || !to || !(vmPermits[permitKey] > Date.now())) {
            serverLog(`Voicemail blocked: ${from} -> ${to}`, 'warn');
            return;
        }

        delete vmPermits[permitKey];

        const audio = Buffer.isBuffer(data.audio)
            ? data.audio
            : (data.audio ? Buffer.from(data.audio) : null);

        if (!audio || !audio.length || audio.length > VOICEMAIL_MAX_BYTES) {
            serverLog(`Voicemail rejected (size): ${from} -> ${to}`, 'warn');
            return;
        }

        if (unheardVoicemails(to).length >= VOICEMAIL_MAX_PER_BOX) {
            serverLog(`Voicemail box full: ${to}`, 'warn');
            return;
        }

        const m = {
            id: crypto.randomBytes(8).toString('hex'),
            to,
            from,
            ts: Date.now(),
            mime: String(data.mime || 'audio/webm'),
            duration: Number(data.duration) || 0,
            played: false,
            audio
        };

        voicemails[m.id] = m;
        persistVoicemail(m);

        serverLog(
            `Voicemail left: ${from} -> ${to} (${m.duration} s)`,
            'ok'
        );

        const targetSocketId = activeUsers[to];
        if (targetSocketId) io.to(targetSocketId).emit('voicemail_new', publicVoicemail(m));
    });

    socket.on('voicemail_get', (data) => {
        const me = numberOfSocket(socket);
        const m = data && voicemails[data.id];

        if (!me || !m || m.to !== me || m.played) return;

        socket.emit('voicemail_audio', { id: m.id, mime: m.mime, audio: m.audio });
    });

    socket.on('voicemail_played', (data) => {
        const me = numberOfSocket(socket);
        const m = data && voicemails[data.id];

        if (!me || !m || m.to !== me) return;

        m.played = true;
        delete voicemails[m.id];
        removeVoicemailFile(m.id);

        serverLog(`Voicemail heard: ${m.from} -> ${me}`);

        sendVoicemailList(me);
    });

    /* =========================================
       TEKSTIVIESTIT (SMS)
       ========================================= */

    socket.on('send_sms', (data, ack) => {
        const reply = (result) => {
            if (typeof ack === 'function') ack(result);
        };
        data = data || {};

        const fromNumber = numberOfSocket(socket);
        const toNumber = String(data.toNumber || '').trim();
        const text = String(data.text || '').trim();

        if (!fromNumber) return reply({ ok: false, error: 'Lähettäjän numeroa ei ole rekisteröity' });
        if (!socketSupportsSms(socket.id)) {
            serverLog(`SMS blocked: ${fromNumber} does not support SMS`, 'warn');
            return reply({ ok: false, error: 'Tämä puhelin ei tue tekstiviestejä' });
        }
        if (!/^\+?\d{3,15}$/.test(toNumber)) {
            return reply({ ok: false, error: 'Virheellinen vastaanottajan numero' });
        }
        if (!text) return reply({ ok: false, error: 'Viesti on tyhjä' });
        if (text.length > 500) return reply({ ok: false, error: 'Viesti on liian pitkä (enintään 500 merkkiä)' });

        const targetSocketId = activeUsers[toNumber];
        if (targetSocketId && !socketSupportsSms(targetSocketId)) {
            serverLog(`SMS blocked: recipient ${toNumber} does not support SMS`, 'warn');
            return reply({ ok: false, error: 'Vastaanottajan puhelin ei tue tekstiviestejä' });
        }

        const message = {
            fromNumber,
            toNumber,
            text,
            timestamp: Date.now()
        };

        if (targetSocketId && socketSupportsSms(targetSocketId)) {
            io.to(targetSocketId).emit('incoming_sms', message);
            serverLog(`SMS delivered: ${fromNumber} -> ${toNumber}`);
            return reply({ ok: true, delivered: true });
        }

        // Vastaanottaja ei ole verkossa: jonotetaan vain, jos numero tunnetaan
        // aiemmasta rekisteröinnistä ja sen puhelin on ilmoittanut SMS-tuen.
        const targetWasSmsCapable = smsCapableNumbers.has(toNumber);
        if (!targetWasSmsCapable) {
            return reply({ ok: false, error: 'Vastaanottajan numeroa ei löydy tai tekstiviestituki ei ole tiedossa' });
        }

        if (!smsMailboxes[toNumber]) smsMailboxes[toNumber] = [];
        if (smsMailboxes[toNumber].length >= MAX_SMS_PER_MAILBOX) smsMailboxes[toNumber].shift();
        smsMailboxes[toNumber].push(message);
        serverLog(`SMS queued: ${fromNumber} -> ${toNumber}`, 'warn');
        return reply({ ok: true, delivered: false, queued: true });
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
                clearRingTimer(number);
                delete vmSessions[number];

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
