const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const rooms = new Map();

io.on('connection', (socket) => {
  // إنشاء غرفة بكود مؤقت
  socket.on('create-room', () => {
    const code = Math.floor(100000 + Math.random() * 900000).toString();
    rooms.set(code, [socket.id]);
    socket.join(code);
    socket.emit('room-created', { code });
  });

  // الانضمام لغرفة عبر الكود
  socket.on('join-room', ({ code }) => {
    const room = rooms.get(code);
    if (room && room.length === 1) {
      room.push(socket.id);
      socket.join(code);
      socket.emit('room-joined', { code });
      io.to(room[0]).emit('peer-joined', { peerId: socket.id });
    } else {
      socket.emit('error-msg', 'الغرفة غير موجودة أو ممتلئة.');
    }
  });

  // تبادل إشارات WebRTC
  socket.on('signal', ({ target, signal }) => {
    io.to(target).emit('signal', { sender: socket.id, signal });
  });

  socket.on('disconnect', () => {
    rooms.forEach((peers, code) => {
      if (peers.includes(socket.id)) {
        rooms.delete(code);
        io.to(code).emit('peer-disconnected');
      }
    });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Signaling Server running on port ${PORT}`));