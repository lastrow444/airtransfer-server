const express = require("express");
const http = require("http");
const https = require("https");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// ---------- تحليلات استخدام مجهولة ----------
// نجمع: دولة (من السيرفر)، نوع الجهاز، النظام، أنواع الملفات، حجم/عدد/وقت النقل، الشبكة، الرفض/الانقطاع.
// لا تُخزَّن بيانات حساسة: لا IP، لا أسماء ملفات. تُكتب في Supabase عبر REST.
const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const SUPABASE_KEY = process.env.SUPABASE_KEY || "";
const GEO_CACHE = new Map(); // ip -> country: نتجنّب نداءات متكررة

function httpJson(url, options, bodyText) {
  return new Promise((resolve) => {
    const lib = /^https:/.test(url) ? https : http;
    const req = lib.request(url, options, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        if (!data) return resolve(null);
        try { resolve(JSON.parse(data)); } catch (err) { resolve(null); }
      });
    });
    req.on("error", () => resolve(null));
    if (bodyText) req.write(bodyText);
    req.end();
  });
}

function getClientIp(socket) {
  const xff = socket.handshake.headers["x-forwarded-for"];
  if (xff) {
    const first = String(xff).split(",")[0].trim();
    if (first) return first;
  }
  return socket.handshake.address;
}

async function resolveCountry(ip) {
  if (GEO_CACHE.has(ip)) return GEO_CACHE.get(ip);
  try {
    const data = await httpJson(`http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,country,countryCode&lang=ar`, { method: "GET" });
    const country = data && data.status === "success" ? (data.country || data.countryCode || "غير معروف") : "غير معروف";
    GEO_CACHE.set(ip, country);
    return country;
  } catch (err) {
    return "غير معروف";
  }
}

function writeAnalytics(event) {
  if (!SUPABASE_URL || !SUPABASE_KEY) return; // لم يُضبط الإعداد بعد — لا نكسر أي شيء
  const url = `${SUPABASE_URL}/rest/v1/events`;
  httpJson(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "apikey": SUPABASE_KEY,
      "Authorization": `Bearer ${SUPABASE_KEY}`
    }
  }, JSON.stringify([event]));
}

async function ingestAnalytics(socket, payload) {
  if (!payload || typeof payload !== "object") return;
  const country = await resolveCountry(getClientIp(socket));
  const event = {
    ts: new Date().toISOString(),
    type: String(payload.type || "unknown"),
    country,
    device: payload.device || null,
    os: payload.os || null,
    peer_device: payload.peerDevice || null,
    direction: payload.direction || null,
    file_count: Number.isFinite(payload.fileCount) ? payload.fileCount : null,
    total_size: Number.isFinite(payload.totalSize) ? payload.totalSize : null,
    file_types: Array.isArray(payload.fileTypes) ? payload.fileTypes : null,
    duration_ms: Number.isFinite(payload.durationMs) ? payload.durationMs : null,
    network: payload.network || null,
    reason: payload.reason || null
  };
  writeAnalytics(event); // حريق وانسَ — لا يمس أداء النقل أبداً
}

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

    // لا نغلق الغرفة ما دام لدى المستلم ملفات بانتظار تسليمها
    if (room.deliveryPending) {
        console.log("startInactivityTimer: Skipping - delivery pending for room", code);
        return;
    }

    // لا نفعّل عداد عدم النشاط أثناء نقل قائم مهما طال (نقل يتجاوز 2 دقيقة)
    if (room.transferring) {
        console.log("startInactivityTimer: Skipping - transfer in progress for room", code);
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
            pickingTimer: null,
            transferring: false
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

    // المستلم لديه ملفات بانتظار التسليم — نوقف عداد عدم النشاط حتى ينتهي
    socket.on("delivery-pending", ({ pending }) => {
        console.log("delivery-pending:", pending, "from socket:", socket.id);
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        const room = rooms[roomCode];
        if (!room) return;
        room.deliveryPending = !!pending;
        if (pending) {
            console.log("delivery-pending: Stopping inactivity timer for room", roomCode);
            stopInactivityTimer(roomCode);
        } else {
            console.log("delivery-pending: Resuming inactivity timer for room", roomCode);
            startInactivityTimer(roomCode);
        }
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
        const room = rooms[roomCode];
        if (!room) return;

        room.transferring = !!transferring;
        if (room.transferring) {
            stopInactivityTimer(roomCode);
            stopPickingTimer(roomCode);
        } else {
            stopPickingTimer(roomCode);
            startInactivityTimer(roomCode);
        }

        io.to(roomCode).emit("transfer-status-change", { transferring });
    });

    // بدء النقل
    socket.on("transfer-start", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        const room = rooms[roomCode];
        if (!room) return;
        // أثناء النقل: لا عدادات إطلاقاً مهما طال النقل
        room.transferring = true;
        stopInactivityTimer(roomCode);
        // دفاع: لو بقي عداد اختيار شغالاً لأي سبب، أوقفه
        stopPickingTimer(roomCode);
    });

    // انتهاء النقل
    socket.on("transfer-end", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        const room = rooms[roomCode];
        if (!room) return;
        room.transferring = false;
        stopPickingTimer(roomCode);
        startInactivityTimer(roomCode);
    });

    // إلغاء النقل
    socket.on("cancel-transfer", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        const room = rooms[roomCode];
        if (!room) return;
        room.transferring = false;
        socket.to(roomCode).emit("peer-cancelled-transfer");
        stopPickingTimer(roomCode);
        startInactivityTimer(roomCode);
    });

    // مغادرة الغرفة
    socket.on("leave-room", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        socket.to(roomCode).emit("peer-disconnected");
        destroyRoom(roomCode, "peer_left");
    });

    // تحليلات من العميل — حريق وانسَ (لا تُنتظر ولا تمس أداء النقل إطلاقاً)
    socket.on("track", (payload) => {
        ingestAnalytics(socket, payload);
    });

    // قطع الاتصال
    socket.on("disconnect", () => {
        const roomCode = findRoomBySocket(socket.id);
        if (!roomCode) return;
        const room = rooms[roomCode];
        // انقطاع مفاجئ أثناء نقل نشط لا يملك العميل وقت الإبلاغ عنه — نسجّله من طرف السيرفر
        if (room && room.transferring) {
            const device = room.host === socket.id ? (room.hostDevice || null) : (room.peerDevice || null);
            ingestAnalytics(socket, { type: "transfer_aborted", reason: "peer_disconnected", device });
        }
        socket.to(roomCode).emit("peer-disconnected");
        destroyRoom(roomCode, "peer_disconnected");
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`AirTransfer Server running on port ${PORT}`);
});
