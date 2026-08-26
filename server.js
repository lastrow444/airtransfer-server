const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// السماح بالاتصال من أي دومين (Netlify وغيره)
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const rooms = new Map();

function generateRoomCode() {
  let code;
  do {
    code = Math.floor(100000 + Math.random() * 900000).toString();
  } while (rooms.has(code));
  return code;
}

app.get('/', (req, res) => {
  res.send('AirTransfer Signaling Server is Running Live!');
});

io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  socket.on('create-room', () => {
    const code = generateRoomCode();
    rooms.set(code, { host: socket.id, peer: null });
    socket.join(code);
    socket.emit('room-created', { code });
  });

  socket.on('join-room', ({ code }) => {
    const room = rooms.get(code);
    if (!room) {
      return socket.emit('error-msg', 'كود الغرفة غير صحيح أو انتهت صلاحيته');
    }
    if (room.peer) {
      return socket.emit('error-msg', 'الغرفة مكتملة بالفعل');
    }

    room.peer = socket.id;
    socket.join(code);
    socket.emit('room-joined', { code });
    io.to(room.host).emit('peer-joined', { peerId: socket.id });
  });

  socket.on('signal', ({ target, signal }) => {
    io.to(target).emit('signal', { sender: socket.id, signal });
  });

  socket.on('disconnect', () => {
    for (const [code, room] of rooms.entries()) {
      if (room.host === socket.id || room.peer === socket.id) {
        io.to(code).emit('peer-disconnected');
        rooms.delete(code);
        break;
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Signaling Server running on port ${PORT}`);
});