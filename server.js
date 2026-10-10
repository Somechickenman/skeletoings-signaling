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
//                      {type:"relay_on"}                    joiner: the direct link failed, carry my game traffic through here
//                      {type:"kick", id}                     host: drop a joiner
//                      binary frame: host -> [to id u16 BE][payload]; joiner (after relay_on) -> [any 2 bytes][payload]
//                      {type:"ping"}
//                      {type:"presence", name, ver, id}      "I am online" (counts towards players online; id = an anonymous install id, optional)
//                      {type:"list"}                         ask for the public parties
//                      {type:"set_public", public, info}     host: list / unlist this room, with its details
//   server -> client   {type:"online", count}                players online (sent on presence + whenever it changes)
//                      {type:"rooms", rooms:[{room,name,mode,arena,players,max,ver}]}
//                         {type:"hosted", room, id:1}
//                      {type:"joined", room, id}             (id >= 2)
//                      {type:"peer_joined", id}              (to the host)
//                      {type:"peer_left", id}                (to the host)
//                      {type:"relay", from:<id>, data:{...}}
//                      {type:"relay_ready"}                  (to the joiner after relay_on)
//                      {type:"relay_peer", id}               (to the host: this joiner now talks through the server)
//                      binary frame: [from id u16 BE][payload]
//                      {type:"host_left"}                    (room is closed)
//                      {type:"error", code}                  room_not_found | room_full | bad_message | busy | rate_limited
//                      {type:"pong"}

const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8787", 10);
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || "8", 10);      // per room, host included
const MAX_MESSAGE_BYTES = parseInt(process.env.MAX_MESSAGE_BYTES || "16384", 10);     // text (JSON) messages
const MAX_FRAME_BYTES = parseInt(process.env.MAX_FRAME_BYTES || "65536", 10);          // binary relay packets
const RELAY_BYTES_PER_SEC = parseInt(process.env.RELAY_BYTES_PER_SEC || "300000", 10); // per connection
const MAX_ROOMS = parseInt(process.env.MAX_ROOMS || "500", 10);
const ROOM_MAX_AGE_MS = parseInt(process.env.ROOM_MAX_AGE_HOURS || "12", 10) * 3600 * 1000;
const RATE_LIMIT = parseInt(process.env.RATE_LIMIT || "120", 10);       // messages per 10 s per connection
const ALPHABET = "123456789ABDEFGHJKLMNPQRSTUVWXYZ";                   // no C/O/0/I (they read alike)
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const DEAD_MS = parseInt(process.env.DEAD_SECONDS || "75", 10) * 1000;      // no message and no pong for this long = connection is dead
const MAX_PRESENCE = parseInt(process.env.MAX_PRESENCE || "3000", 10);
const PRESENCE_IDLE_MS = parseInt(process.env.PRESENCE_IDLE_SECONDS || "120", 10) * 1000;
const presence = new Set();    // connections that said "presence": every running copy of the game
let onlineTimer = null;

function scheduleOnlineBroadcast() {
  if (onlineTimer) return;
  onlineTimer = setTimeout(() => {
    onlineTimer = null;
    const n = presence.size;
    for (const c of presence) c.send({ type: "online", count: n });
  }, 1500);
}

function clean(v, max) {
  return String(v === undefined || v === null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, "").slice(0, max);
}

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
    this.presence = false;
    this.lastSeen = Date.now();
    this.relay = false;
    this.byteWindow = 0;
    this.byteCount = 0;
    allClients.add(this);
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
    if (this.buffer.length > MAX_FRAME_BYTES * 4) { this.close(1009); return; }
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
        if (big > BigInt(MAX_FRAME_BYTES)) { this.close(1009); return; }
        len = Number(big); off = 10;
      }
      if (len > MAX_FRAME_BYTES) { this.close(1009); return; }
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
    if (opcode === 0xA) { this.lastSeen = Date.now(); return; }             // pong: still alive
    if (opcode === 0x1 || opcode === 0x2) { this.fragOpcode = opcode; this.fragments = [payload]; }
    else if (opcode === 0x0) { this.fragments.push(payload); }
    else return;
    if (!fin) return;
    const whole = Buffer.concat(this.fragments);
    this.fragments = [];
    if (this.fragOpcode === 0x1) {
      if (whole.length > MAX_MESSAGE_BYTES) { this.close(1009); return; }
      this.onMessage(whole.toString("utf8"));
    } else {
      this.onBinary(whole);
    }
  }

  // game traffic of players whose direct link failed: forwarded between the host and that joiner
  onBinary(buf) {
    const now = Date.now();
    this.lastSeen = now;
    if (now - this.byteWindow > 1000) { this.byteWindow = now; this.byteCount = 0; }
    this.byteCount += buf.length;
    if (this.byteCount > RELAY_BYTES_PER_SEC) return;           // over budget: drop (game traffic is lossy anyway)
    const room = this.room ? rooms.get(this.room) : null;
    if (!room || buf.length < 2) return;
    let target = null;
    if (this.isHost) target = room.peers.get(buf.readUInt16BE(0));
    else if (this.relay) target = room.host;
    if (!target || (!this.isHost ? false : !target.relay)) return;
    const out = Buffer.alloc(buf.length);
    out.writeUInt16BE(this.isHost ? 1 : this.id, 0);
    buf.copy(out, 2, 2);
    target.sendFrame(0x2, out);
  }

  onMessage(text) {
    const now = Date.now();
    this.lastSeen = now;
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
    allClients.delete(this);
    leave(this);
  }
}

// ----------------------------------------------------------------------------- player stats (anonymous)
// Every copy of the game may send a random install id with its presence. We keep one row per id: first seen, last seen, version,
// number of visits. No names, no IPs. Stored in a free Turso (libSQL) database over its HTTP API when TURSO_URL + TURSO_TOKEN are
// set (so it survives restarts); otherwise in memory only. GET /stats (with ADMIN_TOKEN) shows the numbers.
const TURSO_URL = (process.env.TURSO_URL || "").replace(/^libsql:/, "https:").replace(/\/+$/, "");
const TURSO_TOKEN = process.env.TURSO_TOKEN || "";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "";
const ID_RE = /^[0-9a-f]{32}$/;
const VISIT_GAP_S = 30 * 60;                      // reconnects inside this window are the same visit
const memStats = new Map();                       // fallback store: id -> {first, last, ver, sessions}
const recentIds = new Map();                      // id -> last time (s) we wrote it
let tableReady = false;
let statsChain = Promise.resolve();

function sqlArg(v) {
  return typeof v === "number" ? { type: "integer", value: String(Math.trunc(v)) } : { type: "text", value: String(v) };
}

async function dbExec(stmts) {
  const body = { requests: stmts.map(([sql, args]) => ({ type: "execute", stmt: { sql, args: (args || []).map(sqlArg) } })).concat([{ type: "close" }]) };
  const r = await fetch(TURSO_URL + "/v2/pipeline", {
    method: "POST",
    headers: { "Authorization": "Bearer " + TURSO_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(8000),
  });
  if (!r.ok) throw new Error("turso http " + r.status);
  const j = await r.json();
  return j.results.slice(0, stmts.length).map((x) => {
    if (x.type !== "ok") throw new Error("turso error " + JSON.stringify(x.error || x));
    const res = x.response.result;
    return (res.rows || []).map((row) => row.map((cell) => (cell.type === "integer" ? Number(cell.value) : cell.value)));
  });
}

async function ensureTable() {
  if (tableReady || !TURSO_URL) return;
  await dbExec([["CREATE TABLE IF NOT EXISTS players (id TEXT PRIMARY KEY, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, version TEXT, sessions INTEGER NOT NULL DEFAULT 1)"]]);
  tableReady = true;
}

function recordPlayer(id, ver) {
  if (!ID_RE.test(id)) return;
  const now = Math.floor(Date.now() / 1000);
  const seen = recentIds.get(id);
  if (seen !== undefined && now - seen < VISIT_GAP_S) return;
  recentIds.set(id, now);
  if (recentIds.size > 20000) recentIds.clear();
  ver = String(ver || "").slice(0, 16);
  const m = memStats.get(id);
  if (m) { m.last = now; m.ver = ver; m.sessions += 1; } else { memStats.set(id, { first: now, last: now, ver, sessions: 1 }); }
  if (!TURSO_URL || !TURSO_TOKEN) return;
  statsChain = statsChain.then(async () => {
    await ensureTable();
    await dbExec([["INSERT INTO players (id, first_seen, last_seen, version, sessions) VALUES (?, ?, ?, ?, 1) ON CONFLICT(id) DO UPDATE SET last_seen = excluded.last_seen, version = excluded.version, sessions = sessions + 1", [id, now, now, ver]]]);
  }).catch((e) => { log("stats write failed:", e.message); });
}

async function loadStats() {
  const now = Math.floor(Date.now() / 1000);
  const day = 86400;
  if (!TURSO_URL || !TURSO_TOKEN) {
    const rows = [...memStats.values()];
    const by = {};
    for (const r of rows) by[r.ver || "?"] = (by[r.ver || "?"] || 0) + 1;
    const dayKey = (t) => new Date(t * 1000).toISOString().slice(0, 10);
    const newPerDay = {}, activePerDay = {};
    for (const r of rows) { newPerDay[dayKey(r.first)] = (newPerDay[dayKey(r.first)] || 0) + 1; activePerDay[dayKey(r.last)] = (activePerDay[dayKey(r.last)] || 0) + 1; }
    return {
      store: "memory only (set TURSO_URL and TURSO_TOKEN to keep it across restarts)",
      total: rows.length,
      active24h: rows.filter((r) => now - r.last < day).length,
      active7d: rows.filter((r) => now - r.last < 7 * day).length,
      active30d: rows.filter((r) => now - r.last < 30 * day).length,
      versions: Object.entries(by).sort((a, b) => b[1] - a[1]),
      newPerDay: Object.entries(newPerDay).sort().reverse().slice(0, 30),
      activePerDay: Object.entries(activePerDay).sort().reverse().slice(0, 30),
      recent: [...memStats.entries()].sort((a, b) => b[1].last - a[1].last).slice(0, 15).map(([id, r]) => [id.slice(0, 6), r.last, r.sessions, r.ver]),
    };
  }
  await statsChain;
  await ensureTable();
  const [tot, a1, a7, a30, ver, np, ap, rec] = await dbExec([
    ["SELECT COUNT(*) FROM players"],
    ["SELECT COUNT(*) FROM players WHERE last_seen > ?", [now - day]],
    ["SELECT COUNT(*) FROM players WHERE last_seen > ?", [now - 7 * day]],
    ["SELECT COUNT(*) FROM players WHERE last_seen > ?", [now - 30 * day]],
    ["SELECT COALESCE(version,'?'), COUNT(*) FROM players GROUP BY version ORDER BY 2 DESC"],
    ["SELECT date(first_seen, 'unixepoch') d, COUNT(*) FROM players GROUP BY d ORDER BY d DESC LIMIT 30"],
    ["SELECT date(last_seen, 'unixepoch') d, COUNT(*) FROM players GROUP BY d ORDER BY d DESC LIMIT 30"],
    ["SELECT substr(id, 1, 6), last_seen, sessions, version FROM players ORDER BY last_seen DESC LIMIT 15"],
  ]);
  return { store: "Turso database", total: tot[0][0], active24h: a1[0][0], active7d: a7[0][0], active30d: a30[0][0], versions: ver, newPerDay: np, activePerDay: ap, recent: rec };
}

function statsPage(st) {
  const esc = (x) => String(x).replace(/[&<>]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[ch]));
  const when = (t) => { const d = Math.floor(Date.now() / 1000) - t; return d < 90 ? "just now" : d < 5400 ? Math.round(d / 60) + " min ago" : d < 129600 ? Math.round(d / 3600) + " h ago" : Math.round(d / 86400) + " days ago"; };
  const tbl = (head, rows) => "<table><tr>" + head.map((h) => "<th>" + h + "</th>").join("") + "</tr>" + rows.map((r) => "<tr>" + r.map((c) => "<td>" + esc(c) + "</td>").join("") + "</tr>").join("") + "</table>";
  return "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Skeletoings players</title>" +
    "<style>body{background:#0e0a14;color:#f4eddb;font:16px/1.4 system-ui,sans-serif;max-width:760px;margin:24px auto;padding:0 16px}h1{color:#e8c468}" +
    ".big{display:flex;gap:12px;flex-wrap:wrap}.card{background:#1d1628;border:2px solid #8c6d2c;padding:12px 18px;min-width:120px}.card b{display:block;font-size:34px;color:#e8c468}" +
    "table{border-collapse:collapse;margin:8px 0 20px}td,th{border:1px solid #3b3050;padding:4px 10px;text-align:left}th{color:#e8c468}small{color:#b3a9c0}</style>" +
    "<h1>Skeletoings players</h1><div class=big>" +
    "<div class=card><b>" + st.total + "</b>players ever</div><div class=card><b>" + st.active24h + "</b>last 24 h</div>" +
    "<div class=card><b>" + st.active7d + "</b>last 7 days</div><div class=card><b>" + st.active30d + "</b>last 30 days</div></div>" +
    "<h3>By version</h3>" + tbl(["Version", "Players"], st.versions) +
    "<h3>New players per day</h3>" + tbl(["Day", "New"], st.newPerDay) +
    "<h3>Players last seen per day</h3>" + tbl(["Day", "Players"], st.activePerDay) +
    "<h3>Most recent (anonymous ids)</h3>" + tbl(["Id", "Last played", "Visits", "Version"], st.recent.map((r) => [r[0] + "...", when(r[1]), r[2], r[3]])) +
    "<small>Storage: " + esc(st.store) + ". One id = one install, not one person.</small>";
}

function tokenOk(given) {
  if (!ADMIN_TOKEN || !given) return false;
  const a = crypto.createHash("sha256").update(String(given)).digest();
  const b = crypto.createHash("sha256").update(ADMIN_TOKEN).digest();
  return crypto.timingSafeEqual(a, b);
}

// ----------------------------------------------------------------------------- rooms
function handle(c, m) {
  switch (m.type) {
    case "ping":
      c.send({ type: "pong" });
      return;
    case "presence": {
      if (c.room) { c.send({ type: "error", code: "bad_message" }); return; }
      if (!presence.has(c) && presence.size >= MAX_PRESENCE) { c.send({ type: "error", code: "busy" }); return; }
      presence.add(c);
      c.presence = true;
      if (typeof m.id === "string" && !c.counted) { c.counted = true; recordPlayer(m.id, m.ver); }
      c.send({ type: "online", count: presence.size });
      scheduleOnlineBroadcast();
      return;
    }
    case "list": {
      const out = [];
      for (const [code, room] of rooms) {
        if (!room.public || !room.info || room.info.phase !== "lobby") continue;
        const players = room.peers.size + 1;
        if (players >= MAX_PLAYERS) continue;
        out.push({ room: code, name: room.info.name, mode: room.info.mode, arena: room.info.arena,
                   players: players, max: MAX_PLAYERS, ver: room.info.ver });
      }
      out.sort((a, b) => b.players - a.players);
      c.send({ type: "rooms", rooms: out.slice(0, 40) });
      return;
    }
    case "set_public": {
      const room = c.room ? rooms.get(c.room) : null;
      if (!room || !c.isHost) { c.send({ type: "error", code: "bad_message" }); return; }
      const i = m.info || {};
      room.public = m.public === true;
      room.info = { name: clean(i.name, 24) || "Party", mode: clean(i.mode, 12), arena: clean(i.arena, 12),
                    phase: clean(i.phase, 8), ver: clean(i.ver, 16) };
      return;
    }
    case "host": {
      if (c.room) { c.send({ type: "error", code: "bad_message" }); return; }
      if (rooms.size >= MAX_ROOMS) { c.send({ type: "error", code: "busy" }); return; }
      const code = makeCode();
      if (!code) { c.send({ type: "error", code: "busy" }); return; }
      rooms.set(code, { host: c, peers: new Map(), created: Date.now(), nextId: 2, public: false, info: null });
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
    case "relay_on": {
      const room = c.room ? rooms.get(c.room) : null;
      if (!room || c.isHost) { c.send({ type: "error", code: "bad_message" }); return; }
      c.relay = true;
      log("peer", c.id, "switched to the server relay in", c.room);
      c.send({ type: "relay_ready" });
      room.host.send({ type: "relay_peer", id: c.id });
      return;
    }
    case "kick": {
      const room = c.room ? rooms.get(c.room) : null;
      if (!room || !c.isHost) { c.send({ type: "error", code: "bad_message" }); return; }
      const t = room.peers.get(parseInt(m.id, 10));
      if (t) { t.room = null; room.peers.delete(t.id); t.close(1000); }
      return;
    }
    default:
      c.send({ type: "error", code: "bad_message" });
  }
}

function leave(c) {
  if (c.presence) {
    c.presence = false;
    presence.delete(c);
    scheduleOnlineBroadcast();
  }
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

// WebSocket ping frames every 25 s keep connections alive through idle proxies (clients answer automatically)
const allClients = new Set();
setInterval(() => {
  for (const c of allClients) if (c.alive) c.sendFrame(0x9, Buffer.alloc(0));
}, 25000).unref();

// rooms never outlive their host, but also expire after a long time as a safety net
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.created > ROOM_MAX_AGE_MS) {
      log("room expired", code);
      room.host.close(1000);
    }
  }
  for (const c of presence) {
    if (now - c.lastSeen > PRESENCE_IDLE_MS) { log("presence timed out"); c.close(1000); }
  }
  // dead connections (a crashed game, a dropped network) never send a close: no pong in 75 s = gone, and its room with it
  for (const c of allClients) {
    if (c.alive && now - c.lastSeen > DEAD_MS) { log("dropping silent connection", c.room || (c.presence ? "(presence)" : "(none)")); c.close(1001); }
  }
}, 15000).unref();

// ----------------------------------------------------------------------------- HTTP + upgrade
const server = http.createServer((req, res) => {
  const url = new URL(req.url || "/", "http://x");
  if (url.pathname === "/stats") {
    const auth = (req.headers["authorization"] || "").replace(/^Bearer /, "") || url.searchParams.get("token");
    if (!ADMIN_TOKEN) { res.writeHead(404); res.end("not found"); return; }
    if (!tokenOk(auth)) { res.writeHead(401, { "Content-Type": "text/plain" }); res.end("unauthorized"); return; }
    loadStats().then((st) => {
      if (url.searchParams.get("json")) { res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify(st)); }
      else { res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" }); res.end(statsPage(st)); }
    }).catch((e) => { res.writeHead(500, { "Content-Type": "text/plain" }); res.end("stats failed: " + e.message); });
    return;
  }
  // health check (Render / Fly.io probes, and a human poking it with a browser)
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("ok\nSkeletoings signaling server (" + rooms.size + " rooms, " + presence.size + " online)\n");
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
