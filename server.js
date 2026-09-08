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
         * Vastaanottajaa ei ole verkossa.
         */

        if (!targetSocketId) {

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
         * Poistetaan tämän socketin numero
         * aktiivisesta puhelusta.
         */

        for (
            const [num, id]
            of Object.entries(activeUsers)
        ) {

            if (id === socket.id) {

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
