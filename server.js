const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

const MAX_CONCURRENT_ROOMS = 1500; // حد الأقصى 1500 غرفة (3000 جهاز)
const rooms = {};

function generateUniqueCode() {
  let code;
  do {
    code = Math.floor(100000 + Math.random() * 900000).toString();
  } while (rooms[code]);
  return code;
}

io.on('connection', (socket) => {
  
  socket.on('create-room', () => {
    const currentRoomsCount = Object.keys(rooms).length;
    if (currentRoomsCount >= MAX_CONCURRENT_ROOMS) {
      return socket.emit('error-msg', 'عذراً، وصل التطبيق للحد الأقصى من الاتصالات المتزامنة (1500 غرفة). يرجى المحاولة لاحقاً.');
    }

    const roomCode = generateUniqueCode();
    rooms[roomCode] = { host: socket.id, peer: null };
    socket.join(roomCode);
    socket.emit('room-created', { code: roomCode });
  });

  socket.on('join-room', ({ code }) => {
    const room = rooms[code];
    if (!room) {
      return socket.emit('error-msg', 'كود الغرفة غير صحيح أو غير موجود!');
    }
    if (room.peer) {
      return socket.emit('error-msg', 'الغرفة مكتملة بالفعل بوجود طرفين!');
    }

    room.peer = socket.id;
    socket.join(code);
    socket.emit('room-joined');
    socket.to(room.host).emit('peer-joined', { peerId: socket.id });
  });

  socket.on('signal', ({ target, signal }) => {
    io.to(target).emit('signal', { sender: socket.id, signal });
  });

  socket.on('disconnect', () => {
    for (const code in rooms) {
      const room = rooms[code];
      if (room.host === socket.id || room.peer === socket.id) {
        const otherId = room.host === socket.id ? room.peer : room.host;
        if (otherId) {
          io.to(otherId).emit('peer-disconnected');
        }
        delete rooms[code];
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));