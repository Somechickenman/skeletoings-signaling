"use strict";
// Skeletoings signaling server.
// A tiny WebSocket server that only introduces players to each other. It never sees game traffic:
// once two players have swapped their WebRTC offer/answer/ICE candidates through here, they talk
// directly. Zero dependencies (a minimal RFC 6455 WebSocket implementation is included below), so
// there is nothing to `npm install`.
//
// Protocol (JSON text messages):
//   client -> server   {type:"host"}                         create a room
//                      {type:"join", room:"BONE-7K2Q"}       join a room
//                      {type:"relay", to:<id>, data:{...}}   forward an offer/answer/candidate
//                      {type:"ping"}
//   server -> client   {type:"hosted", room, id:1}
//                      {type:"joined", room, id}             (id >= 2)
//                      {type:"peer_joined", id}              (to the host)
//                      {type:"peer_left", id}                (to the host)
//                      {type:"relay", from:<id>, data:{...}}
//                      {type:"host_left"}                    (room is closed)
//                      {type:"error", code}                  room_not_found | room_full | bad_message | busy | rate_limited
//                      {type:"pong"}

const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8787", 10);
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || "8", 10);      // per room, host included
const MAX_MESSAGE_BYTES = parseInt(process.env.MAX_MESSAGE_BYTES || "16384", 10);
const MAX_ROOMS = parseInt(process.env.MAX_ROOMS || "500", 10);
const ROOM_MAX_AGE_MS = parseInt(process.env.ROOM_MAX_AGE_HOURS || "12", 10) * 3600 * 1000;
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT || "120", 10);       // messages per 10 s per connection
const ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ";                   // no 0/O/1/I
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const rooms = new Map();       // code -> {host: Client, peers: Map<id, Client>, created: number, nextId: number}

function log(...a) { console.log(new Date().toISOString(), ...a); }

function makeCode() {
  for (let tries = 0; tries < 50; tries++) {
    let s = "";
    const bytes = crypto.randomBytes(4);
    for (let i = 0; i < 4; i++) s += ALPHABET[bytes[i] % ALPHABET.length];
    const code = "BONE-" + s;
    if (!rooms.has(code)) return code;
  }
  return null;
}

// ----------------------------------------------------------------------------- WebSocket (RFC 6455)
class Client {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragOpcode = 0;
    this.alive = true;
    this.room = null;
    this.id = 0;
    this.isHost = false;
    this.msgTimes = [];
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
  }

  send(obj) {
    if (!this.alive) return;
    this.sendFrame(0x1, Buffer.from(JSON.stringify(obj), "utf8"));
  }

  sendFrame(opcode, payload) {
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.from([0x80 | opcode, len]);
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2);
    }
    try { this.socket.write(Buffer.concat([header, payload])); } catch (e) { this.onClose(); }
  }

  close(code = 1000) {
    if (!this.alive) return;
    const p = Buffer.alloc(2); p.writeUInt16BE(code);
    this.sendFrame(0x8, p);
    this.alive = false;
    try { this.socket.end(); } catch (e) { /* ignore */ }
    this.onClose();
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    if (this.buffer.length > MAX_MESSAGE_BYTES * 4) { this.close(1009); return; }
    for (;;) {
      if (this.buffer.length < 2) return;
      const b0 = this.buffer[0], b1 = this.buffer[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (this.buffer.length < 4) return;
        len = this.buffer.readUInt16BE(2); off = 4;
      } else if (len === 127) {
        if (this.buffer.length < 10) return;
        const big = this.buffer.readBigUInt64BE(2);
        if (big > BigInt(MAX_MESSAGE_BYTES)) { this.close(1009); return; }
        len = Number(big); off = 10;
      }
      if (len > MAX_MESSAGE_BYTES) { this.close(1009); return; }
      if (!masked) { this.close(1002); return; }          // client frames must be masked
      if (this.buffer.length < off + 4 + len) return;
      const mask = this.buffer.slice(off, off + 4);
      const payload = Buffer.from(this.buffer.slice(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buffer = this.buffer.slice(off + 4 + len);
      this.onFrame(fin, opcode, payload);
      if (!this.alive) return;
    }
  }

  onFrame(fin, opcode, payload) {
    if (opcode === 0x8) { this.close(1000); return; }                       // close
    if (opcode === 0x9) { this.sendFrame(0xA, payload); return; }           // ping -> pong
    if (opcode === 0xA) return;                                             // pong
    if (opcode === 0x1 || opcode === 0x2) { this.fragOpcode = opcode; this.fragments = [payload]; }
    else if (opcode === 0x0) { this.fragments.push(payload); }
    else return;
    if (!fin) return;
    const whole = Buffer.concat(this.fragments);
    this.fragments = [];
    if (whole.length > MAX_MESSAGE_BYTES) { this.close(1009); return; }
    if (this.fragOpcode === 0x1) this.onMessage(whole.toString("utf8"));
  }

  onMessage(text) {
    const now = Date.now();
    this.msgTimes = this.msgTimes.filter((t) => now - t < 10000);
    this.msgTimes.push(now);
    if (this.msgTimes.length > RATE_LIMIT) { this.send({ type: "error", code: "rate_limited" }); return; }
    let m;
    try { m = JSON.parse(text); } catch (e) { this.send({ type: "error", code: "bad_message" }); return; }
    if (!m || typeof m.type !== "string") { this.send({ type: "error", code: "bad_message" }); return; }
    handle(this, m);
  }

  onClose() {
    if (this.closed) return;
    this.closed = true;
    this.alive = false;
    leave(this);
  }
}

// ----------------------------------------------------------------------------- rooms
function handle(c, m) {
  switch (m.type) {
    case "ping":
      c.send({ type: "pong" });
      return;
    case "host": {
      if (c.room) { c.send({ type: "error", code: "bad_message" }); return; }
      if (rooms.size >= MAX_ROOMS) { c.send({ type: "error", code: "busy" }); return; }
      const code = makeCode();
      if (!code) { c.send({ type: "error", code: "busy" }); return; }
      rooms.set(code, { host: c, peers: new Map(), created: Date.now(), nextId: 2 });
      c.room = code; c.id = 1; c.isHost = true;
      log("room created", code, "(rooms:", rooms.size + ")");
      c.send({ type: "hosted", room: code, id: 1 });
      return;
    }
    case "join": {
      if (c.room) { c.send({ type: "error", code: "bad_message" }); return; }
      const code = String(m.room || "").toUpperCase().replace(/\s+/g, "");
      const norm = code.startsWith("BONE-") ? code : "BONE-" + code.replace(/-/g, "");
      const room = rooms.get(norm);
      if (!room) { c.send({ type: "error", code: "room_not_found" }); return; }
      if (room.peers.size + 1 >= MAX_PLAYERS) { c.send({ type: "error", code: "room_full" }); return; }
      const id = room.nextId++;
      room.peers.set(id, c);
      c.room = norm; c.id = id;
      log("peer", id, "joined", norm);
      c.send({ type: "joined", room: norm, id: id });
      room.host.send({ type: "peer_joined", id: id });
      return;
    }
    case "relay": {
      const room = c.room ? rooms.get(c.room) : null;
      if (!room) { c.send({ type: "error", code: "bad_message" }); return; }
      const to = parseInt(m.to, 10);
      let target = null;
      if (c.isHost) target = room.peers.get(to);            // the host may talk to any joiner
      else if (to === 1) target = room.host;                // joiners may only talk to the host (star topology)
      if (!target || m.data === undefined) { c.send({ type: "error", code: "bad_message" }); return; }
      target.send({ type: "relay", from: c.id, data: m.data });
      return;
    }
    default:
      c.send({ type: "error", code: "bad_message" });
  }
}

function leave(c) {
  if (!c.room) return;
  const room = rooms.get(c.room);
  const code = c.room;
  c.room = null;
  if (!room) return;
  if (c.isHost) {
    log("room closed", code, "(host left)");
    rooms.delete(code);
    for (const p of room.peers.values()) {
      p.send({ type: "host_left" });
      p.room = null;
      p.close(1000);
    }
  } else {
    room.peers.delete(c.id);
    log("peer", c.id, "left", code);
    room.host.send({ type: "peer_left", id: c.id });
  }
}

// rooms never outlive their host, but also expire after a long time as a safety net
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.created > ROOM_MAX_AGE_MS) {
      log("room expired", code);
      room.host.close(1000);
    }
  }
}, 60000).unref();

// ----------------------------------------------------------------------------- HTTP + upgrade
const server = http.createServer((req, res) => {
  // health check (Render / Fly.io probes, and a human poking it with a browser)
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Skeletoings signaling server: ok (" + rooms.size + " rooms)\n");
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers["upgrade"] || "").toLowerCase() !== "websocket") {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
    "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
  );
  socket.setNoDelay(true);
  socket.setTimeout(0);
  new Client(socket);
});

server.listen(PORT, () => log("signaling server listening on port", PORT));
