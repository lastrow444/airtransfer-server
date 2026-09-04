const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// ملفات الواجهة من مجلد client
app.use(express.static(path.join(__dirname, "../client")));

// نقطة صحّة خفيفة — فاحص خارجي (UptimeRobot/cron-job.org) يضربها كل دقيقة
// حتى لا ينزل سيرفر Render المجاني إلى النوم ويأخذ 30-60 ثانية للاستيقاظ
// عند أول فتح للتطبيق. تمرير غير مشروط ولا يمس اتصالات Socket.IO.
app.get("/health", (req, res) => {
  res.status(200).send("ok");
});

const rooms = {};

const WAITING_ROOM_TIMEOUT = 60 * 1000;      // 1 دقيقة
const INACTIVITY_TIMEOUT   = 2 * 60 * 1000;  // 2 دقيقة
const PICKING_TIMEOUT      = 10 * 60 * 1000; // 10 دقائق

function generateCode() {
    let code;
    do {
        code = Math.floor(100000 + Math.random() * 900000).toString();
    } while (rooms[code]);
    return code;
}

function destroyRoom(code, reason = "room_destroyed") {
    const room = rooms[code];
    if (!room) {
        console.log("destroyRoom: Room already destroyed or not found:", code);
        return;
    }

    console.log("destroyRoom: Destroying room", code, "Reason:", reason);
    if (room.waitingTimer)   clearTimeout(room.waitingTimer);
    if (room.inactivityTimer) clearTimeout(room.inactivityTimer);
    if (room.pickingTimer)   clearTimeout(room.pickingTimer);

    delete rooms[code];

    io.to(code).emit("room-destroyed", { reason });
    console.log(`Room Destroyed: ${code} | Reason: ${reason}`);
}

function startWaitingTimer(code) {
    const room = rooms[code];
    if (!room) return;

    if (room.waitingTimer) clearTimeout(room.waitingTimer);

    room.waitingTimer = setTimeout(() => {
        const currentRoom = rooms[code];
        if (currentRoom) {
            io.to(code).emit("waiting-timeout");
            destroyRoom(code, "waiting_timeout");
        }
    }, WAITING_ROOM_TIMEOUT);
}

function stopWaitingTimer(code) {
    const room = rooms[code];
    if (!room || !room.waitingTimer) return;
    clearTimeout(room.waitingTimer);
    room.waitingTimer = null;
}

function startInactivityTimer(code) {
    const room = rooms[code];
    if (!room || !room.peer) {
        console.log("startInactivityTimer: Room not found or no peer", code);
        return;
    }

    if (room.inactivityTimer) {
        console.log("startInactivityTimer: Clearing existing timer for room", code);
        clearTimeout(room.inactivityTimer);
    }

    console.log("startInactivityTimer: Starting new timer for room", code, "Timeout:", INACTIVITY_TIMEOUT, "ms");
    room.inactivityTimer = setTimeout(() => {
        const currentRoom = rooms[code];
        if (currentRoom) {
            console.log("Inactivity timeout fired for room", code, "- destroying room");
            io.to(code).emit("inactivity-timeout");
            destroyRoom(code, "inactivity_timeout");
        } else {
            console.log("Inactivity timeout fired for room", code, "- but room already destroyed");
        }
    }, INACTIVITY_TIMEOUT);
    console.log("startInactivityTimer: Timer started for room", code);
}

function stopInactivityTimer(code) {
    const room = rooms[code];
    if (!room || !room.inactivityTimer) {
        console.log("stopInactivityTimer: No timer to stop for room", code);
        return;
    }
    console.log("stopInactivityTimer: Stopping timer for room", code);
    clearTimeout(room.inactivityTimer);
    room.inactivityTimer = null;
}

function startPickingTimer(code) {
    const room = rooms[code];
    if (!room) return;

    if (room.pickingTimer) {
        console.log("startPickingTimer: Clearing existing picking timer for room", code);
        clearTimeout(room.pickingTimer);
    }

    console.log("startPickingTimer: Starting picking timer for room", code, "Timeout:", PICKING_TIMEOUT, "ms");
    room.pickingTimer = setTimeout(() => {
        console.log("Picking timeout fired for room", code, "- destroying room");
        destroyRoom(code, "picking_timeout");
    }, PICKING_TIMEOUT);
}

function stopPickingTimer(code) {
    const room = rooms[code];
    if (!room || !room.pickingTimer) {
        console.log("stopPickingTimer: No picking timer to stop for room", code);
        return;
    }
    console.log("stopPickingTimer: Stopping picking timer for room", code);
    clearTimeout(room.pickingTimer);
    room.pickingTimer = null;
}

function findRoomBySocket(socketId) {
    for (const code in rooms) {
        const room = rooms[code];
        if (room.host === socketId || room.peer === socketId) {
            console.log("findRoomBySocket: Found room", code, "for socket", socketId);
            return code;
        }
    }
    console.log("findRoomBySocket: No room found for socket", socketId);
    return null;
}

io.on("connection", (socket) => {
    console.log("New client connected:", socket.id);

    // إنشاء غرفة
    socket.on("create-room", ({ deviceType }) => {
        const code = generateCode();
        rooms[code] = {
            host: socket.id,
            hostDevice: deviceType || "Unknown Host",
            peer: null,
            peerDevice: null,
            waitingTimer: null,
            inactivityTimer: null,
            pickingTimer: null
        };

        socket.join(code);
        socket.emit("room-created", { code });
        console.log("Room created:", code, "by host:", socket.id);
        startWaitingTimer(code);
    });

    // إلغاء الانتظار
    socket.on("cancel-waiting-room", ({ code }) => {
        if (code && rooms[code]) {
            io.to(code).emit("waiting-timeout");
            destroyRoom(code, "waiting_timeout");
        }
    });

    // الانضمام لغرفة
    socket.on("join-room", ({ code, deviceType }) => {
        const room = rooms[code];
        if (!room) {
            socket.emit("error-message", "الغرفة غير موجودة أو انتهت الجلسة");
            return;
        }

        if (!room.peer) {
            room.peer = socket.id;
            room.peerDevice = deviceType || "Unknown Peer";
            socket.join(code);

            console.log("join-room: Peer joined room", code, "Starting inactivity timer");
            stopWaitingTimer(code);
            // إيقاف أي عدادات أخرى وبدء عداد عدم النشاط
            if (room.inactivityTimer) clearTimeout(room.inactivityTimer);
            if (room.pickingTimer) clearTimeout(room.pickingTimer);
            startInactivityTimer(code);

            // إعلام جميع الأطراف بالاقتران
            io.to(code).emit("room-joined", { peerDevice: room.peerDevice });
            // إعلام الـ host بوجود طرف جديد لبدء اتصال WebRTC
            io.to(room.host).emit("peer-joined", { peerId: socket.id, peerDevice: room.peerDevice });
        } else {
            socket.emit("error-message", "الغرفة ممتلئة بالفعل");
        }
    });

    // WebRTC signaling
    socket.on("signal", ({ target, signal }) => {
        io.to(target).emit("signal", { sender: socket.id, signal });
    });

    // نشاط المستخدم - إعادة تعيين عداد عدم النشاط
    socket.on("session-activity", () => {
        console.log("session-activity received from socket:", socket.id);
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) {
            console.log("session-activity: No room found for socket", socket.id);
            console.log("Current rooms:", Object.keys(rooms));
            return;
        }
        console.log("session-activity: Resetting inactivity timer for room", roomCode);
        // إعادة تعيين عداد عدم النشاط عند أي نشاط
        startInactivityTimer(roomCode);
    });

    // بدء اختيار الملفات
    socket.on("picking-files-start", () => {
        console.log("picking-files-start received from socket:", socket.id);
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        stopInactivityTimer(roomCode);
        startPickingTimer(roomCode);
        io.to(roomCode).emit("picking-files-start");
    });

    // انتهاء اختيار الملفات
    socket.on("picking-files-end", () => {
        console.log("picking-files-end received from socket:", socket.id);
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        stopPickingTimer(roomCode);
        startInactivityTimer(roomCode);
        io.to(roomCode).emit("picking-files-end");
    });

    // إلغاء اختيار الملفات (عند إغلاق نافذة الملفات بدون اختيار)
    socket.on("picking-files-cancel", () => {
        console.log("picking-files-cancel received from socket:", socket.id);
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        stopPickingTimer(roomCode);
        startInactivityTimer(roomCode);
    });

    // تغيير حالة النقل
    socket.on("transfer-status-change", ({ transferring }) => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;

        if (transferring) stopInactivityTimer(roomCode);
        else startInactivityTimer(roomCode);

        io.to(roomCode).emit("transfer-status-change", { transferring });
    });

    // بدء النقل
    socket.on("transfer-start", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        // إيقاف عداد عدم النشاط أثناء النقل
        stopInactivityTimer(roomCode);
    });

    // انتهاء النقل
    socket.on("transfer-end", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        // إعادة تشغيل عداد عدم النشاط بعد انتهاء النقل
        startInactivityTimer(roomCode);
    });

    // إلغاء النقل
    socket.on("cancel-transfer", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        socket.to(roomCode).emit("peer-cancelled-transfer");
        startInactivityTimer(roomCode);
    });

    // مغادرة الغرفة
    socket.on("leave-room", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        socket.to(roomCode).emit("peer-disconnected");
        destroyRoom(roomCode, "peer_left");
    });

    // قطع الاتصال
    socket.on("disconnect", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        socket.to(roomCode).emit("peer-disconnected");
        destroyRoom(roomCode, "peer_disconnected");
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`AirTransfer Server running on port ${PORT}`);
});
