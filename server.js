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
const inCallUsers = new Set();

io.on('connection', (socket) => {

  socket.on('register_number', (number) => {
    activeUsers[number] = socket.id;
  });

  socket.on('start_call', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    
    if (inCallUsers.has(data.targetNumber)) {
      socket.emit('line_busy');
      return;
    }

    if (targetSocketId) {
      inCallUsers.add(data.fromNumber);
      
      io.to(targetSocketId).emit('incoming_call', { 
        fromNumber: data.fromNumber,
        channel: data.channel 
      });
    } else {
      socket.emit('line_busy');
    }
  });

  socket.on('answer_call', (data) => {
    for (const [num, id] of Object.entries(activeUsers)) {
      if (id === socket.id) inCallUsers.add(num);
    }
    
    const callerSocketId = activeUsers[data.targetNumber];
    if (callerSocketId) {
      io.to(callerSocketId).emit('call_answered');
    }
  });

  // --- WEBRTC SIGNALISOINTI (Välittää yhteydenmuodostusviestit) ---
  socket.on('webrtc_offer', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('webrtc_offer', { fromNumber: data.fromNumber, offer: data.offer });
    }
  });

  socket.on('webrtc_answer', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('webrtc_answer', { answer: data.answer });
    }
  });

  socket.on('webrtc_ice_candidate', (data) => {
    const targetSocketId = activeUsers[data.targetNumber];
    if (targetSocketId) {
      io.to(targetSocketId).emit('webrtc_ice_candidate', { candidate: data.candidate });
    }
  });
  // -------------------------------------------------------------

  socket.on('end_call', (data) => {
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
