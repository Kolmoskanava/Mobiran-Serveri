const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const activeUsers = {};

io.on('connection', (socket) => {
  console.log('Laite yhdistetty:', socket.id);

  socket.on('register_number', (number) => {
    activeUsers[number] = socket.id;
    console.log(`Numero ${number} rekisteröity id:lle ${socket.id}`);
  });

  socket.on('start_call', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_call', { fromNumber: data.fromNumber });
    }
  });

  socket.on('audio_stream', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('receive_audio', data.audioChunk);
    }
  });

  socket.on('end_call', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('call_ended');
    }
  });

  socket.on('disconnect', () => {
    for (const [number, id] of Object.entries(activeUsers)) {
      if (id === socket.id) {
        delete activeUsers[number];
        break;
      }
    }
    console.log('Laite poistui:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Mobira-palvelin pyörii portissa ${PORT}`);
});