const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const activeUsers = {};
const inCallUsers = new Set(); // Seurataan varattuja numeroita

io.on('connection', (socket) => {

  socket.on('register_number', (number) => {
    activeUsers[number] = socket.id;
  });

  socket.on('start_call', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    
    // TARKISTUS: Jos kohde on jo puhelussa
    if (inCallUsers.has(data.targetNumber)) {
      socket.emit('line_busy');
      return;
    }

    if (targetSocketId) {
      // Merkitään soittaja puhelutilaan myöhempiä tarkistuksia varten
      inCallUsers.add(data.fromNumber);
      
      io.to(targetSocketId).emit('incoming_call', { 
        fromNumber: data.fromNumber,
        channel: data.channel 
      });
    } else {
      // Jos numeroa ei löydy linjoilta, lähetetään myös varattu-tieto
      socket.emit('line_busy');
    }
  });

  socket.on('answer_call', (data) => {
    // Merkitään myös vastaaja varatuksi kun puhelu yhdistyy
    for (const [num, id] of Object.entries(activeUsers)) {
      if (id === socket.id) inCallUsers.add(num);
    }
    
    const callerSocketId = activeUsers[data.targetNumber];
    if (callerSocketId) {
      io.to(callerSocketId).emit('call_answered');
    }
  });

  socket.on('audio_stream', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('receive_audio', data.audioChunk);
    }
  });

  socket.on('end_call', (data) => {
    // Vapautetaan molemmat numerot puhelun päättyessä
    for (const [num, id] of Object.entries(activeUsers)) {
      if (id === socket.id) inCallUsers.delete(num);
    }
    if (data && data.targetNumber) {
      inCallUsers.delete(data.targetNumber);
    }

    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('call_ended');
    }
  });

  socket.on('disconnect', () => {
    for (const [number, id] of Object.entries(activeUsers)) {
      if (id === socket.id) {
        delete activeUsers[number];
        inCallUsers.delete(number);
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Mobira-palvelin pyörii portissa ${PORT}`);
});
