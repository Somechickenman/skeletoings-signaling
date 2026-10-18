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
//                      {type:"claim_name", id, name}         reserve a player name for this install (after presence)
//                      {type:"stats", id, play_s, wins, board}   this install's totals; board = 1 shows it on the leaderboards
//                      {type:"board", which, id}             a leaderboard: which = "wins" | "play"
//   server -> client   {type:"online", count}                players online (sent on presence + whenever it changes)
//                      {type:"rooms", rooms:[{room,name,mode,arena,players,max,ver}]}
//                      {type:"name", ok, name, reason}       reason: taken | blocked | bad | error
//                      {type:"board", which, top:[[name, value]], me:{rank, value} | null}
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
  if (v === null || v === undefined) return { type: "null" };
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

const EXTRA_COLS = [["name", "TEXT"], ["name_key", "TEXT"], ["play_s", "INTEGER NOT NULL DEFAULT 0"], ["online_s", "INTEGER NOT NULL DEFAULT 0"],
                    ["wins", "INTEGER NOT NULL DEFAULT 0"], ["board", "INTEGER NOT NULL DEFAULT 0"]];

async function ensureTable() {
  if (tableReady || !TURSO_URL) return;
  await dbExec([["CREATE TABLE IF NOT EXISTS players (id TEXT PRIMARY KEY, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, version TEXT, sessions INTEGER NOT NULL DEFAULT 1)"]]);
  for (const [col, type] of EXTRA_COLS) {
    try { await dbExec([["ALTER TABLE players ADD COLUMN " + col + " " + type]]); } catch (e) { /* already there */ }
  }
  await dbExec([["CREATE UNIQUE INDEX IF NOT EXISTS players_name_key ON players(name_key)"]]);
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
  if (m) { m.last = now; m.ver = ver; m.sessions += 1; } else { memStats.set(id, memRow(now, ver)); }
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
      playTotal: rows.reduce((t, r) => t + r.play, 0),
      onlineTotal: rows.reduce((t, r) => t + r.online, 0),
      named: rows.filter((r) => r.name).length,
      onBoards: rows.filter((r) => r.board && r.name).length,
      people: [...memStats.entries()].sort((a, b) => b[1].play - a[1].play).slice(0, 50)
        .map(([id, r]) => [r.name || id.slice(0, 6) + "...", r.play, r.online, r.wins, r.sessions, r.last, r.ver, r.board ? "yes" : ""]),
    };
  }
  await statsChain;
  await ensureTable();
  const [tot, a1, a7, a30, ver, np, ap, rec, sums, ppl] = await dbExec([
    ["SELECT COUNT(*) FROM players"],
    ["SELECT COUNT(*) FROM players WHERE last_seen > ?", [now - day]],
    ["SELECT COUNT(*) FROM players WHERE last_seen > ?", [now - 7 * day]],
    ["SELECT COUNT(*) FROM players WHERE last_seen > ?", [now - 30 * day]],
    ["SELECT COALESCE(version,'?'), COUNT(*) FROM players GROUP BY version ORDER BY 2 DESC"],
    ["SELECT date(first_seen, 'unixepoch') d, COUNT(*) FROM players GROUP BY d ORDER BY d DESC LIMIT 30"],
    ["SELECT date(last_seen, 'unixepoch') d, COUNT(*) FROM players GROUP BY d ORDER BY d DESC LIMIT 30"],
    ["SELECT substr(id, 1, 6), last_seen, sessions, version FROM players ORDER BY last_seen DESC LIMIT 15"],
    ["SELECT COALESCE(SUM(play_s), 0), COALESCE(SUM(online_s), 0), COUNT(name), SUM(CASE WHEN board = 1 AND name IS NOT NULL THEN 1 ELSE 0 END) FROM players"],
    ["SELECT COALESCE(name, substr(id, 1, 6) || '...'), play_s, online_s, wins, sessions, last_seen, version, CASE WHEN board = 1 THEN 'yes' ELSE '' END FROM players ORDER BY play_s DESC LIMIT 50"],
  ]);
  return { store: "Turso database", total: tot[0][0], active24h: a1[0][0], active7d: a7[0][0], active30d: a30[0][0], versions: ver, newPerDay: np, activePerDay: ap, recent: rec,
           playTotal: sums[0][0], onlineTotal: sums[0][1], named: sums[0][2], onBoards: sums[0][3] || 0, people: ppl };
}

function statsPage(st) {
  const esc = (x) => String(x).replace(/[&<>]/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[ch]));
  const when = (t) => { const d = Math.floor(Date.now() / 1000) - t; return d < 90 ? "just now" : d < 5400 ? Math.round(d / 60) + " min ago" : d < 129600 ? Math.round(d / 3600) + " h ago" : Math.round(d / 86400) + " days ago"; };
  const hours = (sec) => (Number(sec || 0) / 3600).toFixed(1);
  const tbl = (head, rows) => "<table><tr>" + head.map((h) => "<th>" + h + "</th>").join("") + "</tr>" + rows.map((r) => "<tr>" + r.map((c) => "<td>" + esc(c) + "</td>").join("") + "</tr>").join("") + "</table>";
  return "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Skeletoings players</title>" +
    "<style>body{background:#0e0a14;color:#f4eddb;font:16px/1.4 system-ui,sans-serif;max-width:760px;margin:24px auto;padding:0 16px}h1{color:#e8c468}" +
    ".big{display:flex;gap:12px;flex-wrap:wrap}.card{background:#1d1628;border:2px solid #8c6d2c;padding:12px 18px;min-width:120px}.card b{display:block;font-size:34px;color:#e8c468}" +
    "table{border-collapse:collapse;margin:8px 0 20px}td,th{border:1px solid #3b3050;padding:4px 10px;text-align:left}th{color:#e8c468}small{color:#b3a9c0}</style>" +
    "<h1>Skeletoings players</h1><div class=big>" +
    "<div class=card><b>" + st.total + "</b>players ever</div><div class=card><b>" + st.active24h + "</b>last 24 h</div>" +
    "<div class=card><b>" + st.active7d + "</b>last 7 days</div><div class=card><b>" + st.active30d + "</b>last 30 days</div></div>" +
    "<div class=big style='margin-top:12px'><div class=card><b>" + hours(st.playTotal) + "</b>hours played (all players)</div>" +
    "<div class=card><b>" + (st.total ? hours(st.playTotal / st.total) : "0") + "</b>hours per player</div>" +
    "<div class=card><b>" + hours(st.onlineTotal) + "</b>hours online (measured here)</div>" +
    "<div class=card><b>" + st.named + "</b>names taken</div><div class=card><b>" + st.onBoards + "</b>on the leaderboards</div></div>" +
    "<h3>Players by time played (top 50)</h3>" + tbl(["Name", "Played", "Online", "Wins", "Visits", "Last played", "Version", "Boards"],
      st.people.map((r) => [r[0], hours(r[1]) + " h", hours(r[2]) + " h", r[3], r[4], when(r[5]), r[6], r[7]])) +
    "<h3>By version</h3>" + tbl(["Version", "Players"], st.versions) +
    "<h3>New players per day</h3>" + tbl(["Day", "New"], st.newPerDay) +
    "<h3>Players last seen per day</h3>" + tbl(["Day", "Players"], st.activePerDay) +
    "<h3>Most recent (anonymous ids)</h3>" + tbl(["Id", "Last played", "Visits", "Version"], st.recent.map((r) => [r[0] + "...", when(r[1]), r[2], r[3]])) +
    "<small>Storage: " + esc(st.store) + ". One id = one install, not one person. \"Played\" is what the game counts (time it was open, also offline); " +
    "\"Online\" is how long it was connected to this server. Names only show for players who claimed one.</small>";
}

function tokenOk(given) {
  if (!ADMIN_TOKEN || !given) return false;
  const a = crypto.createHash("sha256").update(String(given)).digest();
  const b = crypto.createHash("sha256").update(ADMIN_TOKEN).digest();
  return crypto.timingSafeEqual(a, b);
}

// ----------------------------------------------------------------------------- names, playtime, wins, leaderboards
// One name per install, first come first served: the name's key (lower case, letters and digits only) is unique, so "Bones", "bones" and
// "B.o.n.e.s" are the same name. A name whose owner has not been seen for NAME_EXPIRE_DAYS can be taken by someone else.
// Names with slurs or hate terms are refused (name_filter.json, the same list the game checks as you type).
const fs = require("fs");
const path = require("path");
const NAME_EXPIRE_S = parseInt(process.env.NAME_EXPIRE_DAYS || "180", 10) * 86400;
const BOARD_CACHE_MS = 60 * 1000;
let FILTER = { anywhere: [], word: [] };
try { FILTER = JSON.parse(fs.readFileSync(path.join(__dirname, "name_filter.json"), "utf8")); }
catch (e) { log("name_filter.json not found: names are only checked for length and characters"); }
const LEET = { "0": "o", "1": "i", "2": "z", "3": "e", "4": "a", "5": "s", "6": "g", "7": "t", "8": "b", "9": "g", "@": "a", "$": "s", "!": "i", "|": "i", "+": "t" };
const squeeze = (x) => x.replace(/(.)\1+/g, "$1");
const deleet = (x) => x.toLowerCase().split("").map((ch) => LEET[ch] || ch).join("");
const ANY = (FILTER.anywhere || []).map((w) => squeeze(deleet(w).replace(/[^a-z]/g, ""))).filter((w) => w.length > 0);
const WORDS = new Set((FILTER.word || []).map((w) => w.toLowerCase()));
const NAME_RE = /^[\p{L}\p{N} _.'-]{2,20}$/u;
const nameIndex = new Map();                      // memory store: name key -> id

function memRow(now, ver) {
  return { first: now, last: now, ver, sessions: 1, name: "", key: "", play: 0, online: 0, wins: 0, board: 0, discord: "" };
}

function nameBlocked(name) {
  const low = name.toLowerCase();
  const raw = low.split(/[^a-z0-9]+/).filter(Boolean);
  const lt = deleet(low).split(/[^a-z]+/).filter(Boolean);
  for (const t of raw.concat(lt, lt.map(squeeze))) if (WORDS.has(t)) return true;
  const flat = squeeze(deleet(low).replace(/[^a-z]/g, ""));
  for (const w of ANY) if (flat.includes(w)) return true;
  return false;
}

function nameKey(name) {
  return name.toLowerCase().normalize("NFKD").replace(/[^\p{L}\p{N}]/gu, "");
}

// The name as it will be kept: trimmed, single spaces. "" if it breaks the rules.
function tidyName(name) {
  const n = String(name || "").trim().replace(/\s+/g, " ");
  return NAME_RE.test(n) && nameKey(n).length >= 2 ? n : "";
}

let nameChain = Promise.resolve();               // claims run one at a time, so two people can't take the same name in the same instant

function claimName(id, asked) {
  const name = tidyName(asked);
  if (!name) return Promise.resolve({ ok: false, reason: "bad", name: "" });
  if (nameBlocked(name)) return Promise.resolve({ ok: false, reason: "blocked", name: "" });
  const key = nameKey(name);
  const run = nameChain.then(() => claimNow(id, name, key));
  nameChain = run.catch(() => {});
  return run;
}

async function claimNow(id, name, key) {
  const now = Math.floor(Date.now() / 1000);
  if (!TURSO_URL || !TURSO_TOKEN) {
    const owner = nameIndex.get(key);
    if (owner && owner !== id) {
      const o = memStats.get(owner);
      if (o && now - o.last < NAME_EXPIRE_S) return { ok: false, reason: "taken", name: "" };
      if (o) { o.name = ""; o.key = ""; }
    }
    let r = memStats.get(id);
    if (!r) { r = memRow(now, ""); memStats.set(id, r); }
    if (r.key && r.key !== key) nameIndex.delete(r.key);
    r.name = name; r.key = key; r.last = now;
    nameIndex.set(key, id);
    return { ok: true, name };
  }
  await statsChain;
  await ensureTable();
  const [rows] = await dbExec([["SELECT id, last_seen FROM players WHERE name_key = ?", [key]]]);
  if (rows.length > 0 && rows[0][0] !== id) {
    if (now - rows[0][1] < NAME_EXPIRE_S) return { ok: false, reason: "taken", name: "" };
    await dbExec([["UPDATE players SET name = NULL, name_key = NULL WHERE id = ?", [rows[0][0]]]]);   // long gone: the name is free again
  }
  try {
    await dbExec([["INSERT INTO players (id, first_seen, last_seen, version, sessions, name, name_key) VALUES (?, ?, ?, '', 1, ?, ?) " +
                   "ON CONFLICT(id) DO UPDATE SET name = excluded.name, name_key = excluded.name_key, last_seen = excluded.last_seen", [id, now, now, name, key]]]);
  } catch (e) {
    if (/UNIQUE/i.test(e.message)) return { ok: false, reason: "taken", name: "" };
    throw e;
  }
  boardCache.clear();
  return { ok: true, name };
}

// The game's own totals (it counts time played and wins in its save). Only ever goes up.
function recordStats(id, play, wins, board, ver) {
  play = Math.max(0, Math.min(Math.floor(play), 10 * 365 * 86400));
  wins = Math.max(0, Math.min(Math.floor(wins), 1000000));
  const now = Math.floor(Date.now() / 1000);
  let r = memStats.get(id);
  if (!r) { r = memRow(now, ver); memStats.set(id, r); }
  r.play = Math.max(r.play, play); r.wins = Math.max(r.wins, wins); r.board = board; r.last = now;
  boardCache.clear();
  if (!TURSO_URL || !TURSO_TOKEN) return;
  statsChain = statsChain.then(async () => {
    await ensureTable();
    await dbExec([["INSERT INTO players (id, first_seen, last_seen, version, sessions, play_s, wins, board) VALUES (?, ?, ?, ?, 1, ?, ?, ?) " +
                   "ON CONFLICT(id) DO UPDATE SET play_s = MAX(play_s, excluded.play_s), wins = MAX(wins, excluded.wins), board = excluded.board, last_seen = excluded.last_seen",
                   [id, now, now, ver, play, wins, board]]]);
  }).catch((e) => { log("stats write failed:", e.message); });
}

// How long a copy of the game stayed connected (measured here, so it can't be made up).
function addOnline(id, secs) {
  secs = Math.max(0, Math.min(Math.floor(secs), 7 * 86400));
  if (secs < 1) return;
  const r = memStats.get(id);
  if (r) r.online += secs;
  if (!TURSO_URL || !TURSO_TOKEN) return;
  statsChain = statsChain.then(async () => {
    await ensureTable();
    await dbExec([["UPDATE players SET online_s = online_s + ? WHERE id = ?", [secs, id]]]);
  }).catch((e) => { log("online time write failed:", e.message); });
}

const boardCache = new Map();                    // which -> {at, top}

async function loadBoard(which, id) {
  const col = which === "wins" ? "wins" : "play_s";
  const field = which === "wins" ? "wins" : "play";
  let top;
  const cached = boardCache.get(which);
  if (cached && Date.now() - cached.at < BOARD_CACHE_MS) top = cached.top;
  let me = null;
  if (!TURSO_URL || !TURSO_TOKEN) {
    const rows = [...memStats.entries()].filter(([, r]) => r.board && r.name && r.discord).sort((a, b) => b[1][field] - a[1][field]);
    if (!top) top = rows.slice(0, 10).map(([, r]) => [r.name, r[field]]);
    const mine = memStats.get(id);
    if (mine && mine.board && mine.name && mine.discord) me = { rank: rows.filter(([, r]) => r[field] > mine[field]).length + 1, value: mine[field] };
  } else {
    await statsChain;
    await ensureAccounts();
    const linked = "board = 1 AND name IS NOT NULL AND discord_id IS NOT NULL";          // only players with a linked Discord account
    const stmts = [];
    if (!top) stmts.push(["SELECT name, " + col + " FROM players WHERE " + linked + " ORDER BY " + col + " DESC, first_seen ASC LIMIT 10"]);
    if (id) stmts.push(["SELECT " + col + ", (SELECT COUNT(*) FROM players p WHERE p." + linked.replace(/ AND /g, " AND p.") + " AND p." + col + " > players." + col + ") + 1 FROM players WHERE id = ? AND " + linked, [id]]);
    const res = stmts.length ? await dbExec(stmts) : [];
    if (!top) top = res.shift();
    const mine = id ? res.shift() : null;
    if (mine && mine.length) me = { rank: mine[0][1], value: mine[0][0] };
  }
  boardCache.set(which, { at: cached && cached.top === top ? cached.at : Date.now(), top });
  return { top, me };
}

// ----------------------------------------------------------------------------- Discord accounts
// Optional: a player links their Discord account to their copy of the game. No email, no password: Discord's own login page, in the
// browser. We only ask Discord who they are (scope "identify") and whether they're in the Skeletoings server ("guilds.members.read").
//   1. the game makes a random state (32 hex) and opens  PUBLIC_URL/auth/discord?state=...  in the browser
//   2. that redirects to Discord; Discord sends the browser back to  /auth/discord/callback  with a one-time code
//   3. we swap the code for a short-lived Discord token, read the user and their membership, and keep the result under the state
//   4. the game, polling with {type: "auth_poll", state, id}, gets {type: "auth", ok, token, name}: its login token (kept in its save)
// A linked account can hold a name, appear on the leaderboards, earn event rewards, and claim the welcome gift (500 Teeth and the
// members' hat) once: once per Discord account AND once per install, and only for Discord accounts older than GIFT_MIN_AGE_DAYS.
// Needs DISCORD_CLIENT_ID, DISCORD_CLIENT_SECRET, DISCORD_GUILD_ID and PUBLIC_URL; without them linking is simply off.
const DISCORD_ID = process.env.DISCORD_CLIENT_ID || "";
const DISCORD_SECRET = process.env.DISCORD_CLIENT_SECRET || "";
const DISCORD_GUILD = process.env.DISCORD_GUILD_ID || "";
const DISCORD_INVITE = process.env.DISCORD_INVITE || "https://discord.com/invite/cMzf7zB6GD";
const PUBLIC_URL = (process.env.PUBLIC_URL || "").replace(/\/+$/, "");
const GIFT_TEETH = parseInt(process.env.GIFT_TEETH || "500", 10);
const GIFT_HAT = process.env.GIFT_HAT || "member_beret";
const GIFT_MIN_AGE_S = parseInt(process.env.GIFT_MIN_AGE_DAYS || "7", 10) * 86400;
const AUTH_TTL_MS = 10 * 60 * 1000;
const STATE_RE = /^[0-9a-f]{32}$/;
const TOKEN_RE = /^[0-9a-f]{64}$/;
const pendingAuth = new Map();                    // state -> {at, result?}
const memAccounts = new Map();                    // discord id -> {name, stats, linked, gift}   (no Turso: tests and local runs)
const memSessions = new Map();                    // sha256(token) -> discord id
const memGiftInstalls = new Set();
let accountsReady = false;

function linkingOn() { return !!(DISCORD_ID && DISCORD_SECRET && DISCORD_GUILD && PUBLIC_URL); }
function sha(t) { return crypto.createHash("sha256").update(String(t)).digest("hex"); }
function snowflakeAge(id) {                       // seconds since the Discord account was made (the id holds its creation time)
  try { return Math.floor(Date.now() / 1000) - Math.floor((Number(BigInt(id) >> 22n) + 1420070400000) / 1000); } catch (e) { return 0; }
}

async function ensureAccounts() {
  if (accountsReady || !TURSO_URL) return;
  await ensureTable();
  await dbExec([
    ["CREATE TABLE IF NOT EXISTS accounts (discord_id TEXT PRIMARY KEY, discord_name TEXT, stats_id TEXT, linked_at INTEGER NOT NULL, last_seen INTEGER NOT NULL, gift_at INTEGER)"],
    ["CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, discord_id TEXT NOT NULL, created INTEGER NOT NULL)"],
    ["CREATE TABLE IF NOT EXISTS gift_installs (stats_id TEXT PRIMARY KEY, discord_id TEXT NOT NULL, at INTEGER NOT NULL)"],
  ]);
  try { await dbExec([["ALTER TABLE players ADD COLUMN discord_id TEXT"]]); } catch (e) { /* already there */ }
  accountsReady = true;
}

// Tie a Discord account to this install: the install row gets the Discord id (its name moves over from the account's old install, if
// it had one), a new login token is made. Returns the token.
async function linkAccount(discordId, discordName, statsId) {
  const now = Math.floor(Date.now() / 1000);
  const token = crypto.randomBytes(32).toString("hex");
  if (!TURSO_URL || !TURSO_TOKEN) {
    const a = memAccounts.get(discordId) || { name: discordName, stats: "", linked: now, gift: 0 };
    const old = a.stats && a.stats !== statsId ? memStats.get(a.stats) : null;
    let r = memStats.get(statsId);
    if (!r) { r = memRow(now, ""); memStats.set(statsId, r); }
    if (old && old.name && !r.name) { r.name = old.name; r.key = old.key; nameIndex.set(old.key, statsId); old.name = ""; old.key = ""; }
    for (const [, x] of memStats) if (x.discord === discordId) x.discord = "";
    r.discord = discordId;
    a.name = discordName; a.stats = statsId;
    memAccounts.set(discordId, a);
    memSessions.set(sha(token), discordId);
    boardCache.clear();
    return token;
  }
  await statsChain;
  await ensureAccounts();
  const [prev] = await dbExec([["SELECT a.stats_id, p.name, p.name_key FROM accounts a LEFT JOIN players p ON p.id = a.stats_id WHERE a.discord_id = ?", [discordId]]]);
  const [mine] = await dbExec([["SELECT name FROM players WHERE id = ?", [statsId]]]);
  const stmts = [
    ["INSERT INTO players (id, first_seen, last_seen, version, sessions) VALUES (?, ?, ?, '', 1) ON CONFLICT(id) DO NOTHING", [statsId, now, now]],
    ["UPDATE players SET discord_id = NULL WHERE discord_id = ?", [discordId]],
  ];
  // the account's name comes along from its old install (unless this install already has one): free it there first, then take it here
  if (prev.length && prev[0][0] && prev[0][0] !== statsId && prev[0][1] && !(mine.length && mine[0][0])) {
    stmts.push(["UPDATE players SET name = NULL, name_key = NULL WHERE id = ?", [prev[0][0]]]);
    stmts.push(["UPDATE players SET name = ?, name_key = ? WHERE id = ?", [prev[0][1], prev[0][2], statsId]]);
  }
  stmts.push(
    ["UPDATE players SET discord_id = ? WHERE id = ?", [discordId, statsId]],
    ["INSERT INTO accounts (discord_id, discord_name, stats_id, linked_at, last_seen) VALUES (?, ?, ?, ?, ?) " +
     "ON CONFLICT(discord_id) DO UPDATE SET discord_name = excluded.discord_name, stats_id = excluded.stats_id, last_seen = excluded.last_seen",
     [discordId, discordName, statsId, now, now]],
    ["INSERT INTO sessions (token_hash, discord_id, created) VALUES (?, ?, ?)", [sha(token), discordId, now]],
  );
  await dbExec(stmts);
  boardCache.clear();
  return token;
}

// Who a login token belongs to: {discordId, name, gift} or null.
async function sessionAccount(token) {
  if (!TOKEN_RE.test(String(token || ""))) return null;
  if (!TURSO_URL || !TURSO_TOKEN) {
    const id = memSessions.get(sha(token));
    const a = id ? memAccounts.get(id) : null;
    return a ? { discordId: id, name: a.name, gift: a.gift, stats: a.stats } : null;
  }
  await ensureAccounts();
  const [rows] = await dbExec([["SELECT a.discord_id, a.discord_name, a.gift_at, a.stats_id FROM sessions s JOIN accounts a ON a.discord_id = s.discord_id WHERE s.token_hash = ?", [sha(token)]]]);
  return rows.length ? { discordId: rows[0][0], name: rows[0][1], gift: rows[0][2] || 0, stats: rows[0][3] } : null;
}

// The welcome gift: once per Discord account, once per install, Discord accounts a week old or more.
async function claimGift(acct, statsId) {
  if (snowflakeAge(acct.discordId) < GIFT_MIN_AGE_S) return { ok: false, reason: "too_new" };
  const now = Math.floor(Date.now() / 1000);
  if (!TURSO_URL || !TURSO_TOKEN) {
    const a = memAccounts.get(acct.discordId);
    if (!a || a.gift) return { ok: false, reason: "claimed" };
    if (memGiftInstalls.has(statsId)) return { ok: false, reason: "install" };
    a.gift = now;
    memGiftInstalls.add(statsId);
    return { ok: true, teeth: GIFT_TEETH, hat: GIFT_HAT };
  }
  await ensureAccounts();
  const [mine, inst] = await dbExec([["SELECT gift_at FROM accounts WHERE discord_id = ?", [acct.discordId]], ["SELECT discord_id FROM gift_installs WHERE stats_id = ?", [statsId]]]);
  if (!mine.length || mine[0][0]) return { ok: false, reason: "claimed" };
  if (inst.length) return { ok: false, reason: "install" };
  try {
    await dbExec([["INSERT INTO gift_installs (stats_id, discord_id, at) VALUES (?, ?, ?)", [statsId, acct.discordId, now]],
                  ["UPDATE accounts SET gift_at = ? WHERE discord_id = ? AND gift_at IS NULL", [now, acct.discordId]]]);
  } catch (e) {
    if (/UNIQUE/i.test(e.message)) return { ok: false, reason: "install" };
    throw e;
  }
  return { ok: true, teeth: GIFT_TEETH, hat: GIFT_HAT };
}

function authPage(title, body) {
  return "<!doctype html><html><head><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Skeletoings</title>" +
    "<style>body{background:#1a1423;color:#f4eddb;font:18px system-ui,sans-serif;display:grid;place-items:center;min-height:100vh;margin:0;padding:16px}" +
    "main{max-width:460px;text-align:center}h1{color:#f2d264}a{color:#8dff7a}</style></head><body><main><h1>" + title + "</h1>" + body + "</main></body></html>";
}

async function discordCallback(code, state) {
  const p = pendingAuth.get(state);
  if (!p) return { page: authPage("That link has expired", "<p>Go back to Skeletoings and press <b>Link Discord</b> again.</p>") };
  const tr = await fetch("https://discord.com/api/oauth2/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ client_id: DISCORD_ID, client_secret: DISCORD_SECRET, grant_type: "authorization_code", code,
                                redirect_uri: PUBLIC_URL + "/auth/discord/callback" }),
    signal: AbortSignal.timeout(8000),
  });
  if (!tr.ok) throw new Error("token " + tr.status);
  const tok = await tr.json();
  const auth = { "Authorization": "Bearer " + tok.access_token };
  const ur = await fetch("https://discord.com/api/v10/users/@me", { headers: auth, signal: AbortSignal.timeout(8000) });
  if (!ur.ok) throw new Error("user " + ur.status);
  const user = await ur.json();
  const mr = await fetch("https://discord.com/api/v10/users/@me/guilds/" + DISCORD_GUILD + "/member", { headers: auth, signal: AbortSignal.timeout(8000) });
  const name = clean(user.global_name || user.username, 40);
  if (mr.status !== 200) {
    p.result = { ok: false, reason: "not_member" };
    return { page: authPage("Almost there, " + name + "!", "<p>Join the Skeletoings Discord server first:</p><p><a href='" + DISCORD_INVITE + "'>" + DISCORD_INVITE +
      "</a></p><p>Then go back to the game and press <b>Link Discord</b> again.</p>") };
  }
  p.result = { ok: true, discordId: String(user.id), name };
  return { page: authPage("Linked!", "<p>Welcome, <b>" + name + "</b>. You can close this tab and go back to Skeletoings.</p>") };
}

setInterval(() => {
  const now = Date.now();
  for (const [s, p] of pendingAuth) if (now - p.at > AUTH_TTL_MS) pendingAuth.delete(s);
}, 60000).unref();

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
      if (typeof m.id === "string" && !c.counted) { c.counted = true; recordPlayer(m.id, m.ver); if (ID_RE.test(m.id)) { c.statsId = m.id; c.since = Date.now(); } }
      c.send({ type: "online", count: presence.size });
      scheduleOnlineBroadcast();
      return;
    }
    case "list": {
      const out = [];
      for (const [code, room] of rooms) {
        if (!room.public || !room.info) continue;       // parties already in a match are listed too: joiners spectate until the next round
        const players = room.peers.size + 1;
        if (players >= MAX_PLAYERS) continue;
        out.push({ room: code, name: room.info.name, mode: room.info.mode, arena: room.info.arena,
                   players: players, max: MAX_PLAYERS, ver: room.info.ver, phase: room.info.phase });
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
    case "claim_name": {
      if (!c.presence || typeof m.id !== "string" || !ID_RE.test(m.id)) { c.send({ type: "error", code: "bad_message" }); return; }
      c.claims = (c.claims || 0) + 1;
      if (c.claims > 30) { c.send({ type: "name", ok: false, reason: "busy", name: "" }); return; }
      const asked = clean(m.name, 40);
      sessionAccount(m.token).then((acct) => {
        if (!acct || acct.stats !== m.id) return { ok: false, reason: "link", name: "" };        // names belong to linked Discord accounts
        return claimName(m.id, asked);
      }).then((r) => c.send({ type: "name", ok: r.ok, name: r.name, reason: r.reason || "", asked }))
        .catch((e) => { log("claim failed:", e.message); c.send({ type: "name", ok: false, reason: "error", name: "", asked }); });
      return;
    }
    case "stats": {
      if (!c.presence || typeof m.id !== "string" || !ID_RE.test(m.id)) { c.send({ type: "error", code: "bad_message" }); return; }
      if (!c.statsId) { c.statsId = m.id; c.since = Date.now(); }
      recordStats(m.id, Number(m.play_s) || 0, Number(m.wins) || 0, m.board ? 1 : 0, clean(m.ver, 16));
      return;
    }
    case "auth_poll": {
      if (!c.presence || !STATE_RE.test(String(m.state || "")) || typeof m.id !== "string" || !ID_RE.test(m.id)) { c.send({ type: "error", code: "bad_message" }); return; }
      c.polls = (c.polls || 0) + 1;
      if (c.polls > 400) { c.send({ type: "auth", ok: false, reason: "busy" }); return; }
      let p = pendingAuth.get(m.state);
      if (!p) {                                          // the game asked before the browser got here: start waiting for it
        if (pendingAuth.size >= 5000) { c.send({ type: "auth", ok: false, reason: "busy" }); return; }
        p = { at: Date.now() };
        pendingAuth.set(m.state, p);
      }
      if (!p.result) { c.send({ type: "auth", pending: true }); return; }
      pendingAuth.delete(m.state);
      if (!p.result.ok) { c.send({ type: "auth", ok: false, reason: p.result.reason }); return; }
      const res = p.result;
      linkAccount(res.discordId, res.name, m.id)
        .then((token) => sessionAccount(token).then((acct) => c.send({ type: "auth", ok: true, token, name: res.name, gift: !(acct && acct.gift) })))
        .catch((e) => { log("link failed:", e.message); c.send({ type: "auth", ok: false, reason: "error" }); });
      return;
    }
    case "account": {
      if (!c.presence || typeof m.id !== "string" || !ID_RE.test(m.id)) { c.send({ type: "error", code: "bad_message" }); return; }
      sessionAccount(m.token).then((acct) => {
        if (!acct || acct.stats !== m.id) { c.send({ type: "account", ok: false }); return; }      // unknown, or linked to another install since
        c.discordId = acct.discordId;
        c.send({ type: "account", ok: true, name: acct.name, gift: !acct.gift });
      }).catch((e) => { log("account check failed:", e.message); c.send({ type: "account", ok: false, error: true }); });
      return;
    }
    case "claim_gift": {
      if (!c.presence || typeof m.id !== "string" || !ID_RE.test(m.id)) { c.send({ type: "error", code: "bad_message" }); return; }
      sessionAccount(m.token).then((acct) => {
        if (!acct || acct.stats !== m.id) return { ok: false, reason: "link" };
        return claimGift(acct, m.id);
      }).then((r) => c.send(Object.assign({ type: "gift" }, r)))
        .catch((e) => { log("gift failed:", e.message); c.send({ type: "gift", ok: false, reason: "error" }); });
      return;
    }
    case "auth_test": {                                  // server_test.js only (AUTH_TEST=1): pretend Discord answered for this state
      if (process.env.AUTH_TEST !== "1" || !STATE_RE.test(String(m.state || ""))) { c.send({ type: "error", code: "bad_message" }); return; }
      pendingAuth.set(m.state, { at: Date.now(), result: m.member === false ? { ok: false, reason: "not_member" } : { ok: true, discordId: String(m.discord_id), name: clean(m.name, 40) } });
      c.send({ type: "auth_test", ok: true });
      return;
    }
    case "board": {
      const which = m.which === "wins" ? "wins" : m.which === "play" ? "play" : "";
      if (!c.presence || !which) { c.send({ type: "error", code: "bad_message" }); return; }
      const id = typeof m.id === "string" && ID_RE.test(m.id) ? m.id : "";
      loadBoard(which, id).then((r) => c.send({ type: "board", which, top: r.top, me: r.me }))
        .catch((e) => { log("board failed:", e.message); c.send({ type: "board", which, top: [], me: null, error: true }); });
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
  if (c.statsId && c.since) {
    addOnline(c.statsId, (Date.now() - c.since) / 1000);
    c.since = 0;
  }
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
  if (url.pathname === "/auth/discord") {
    const state = String(url.searchParams.get("state") || "");
    const page = (code, html) => { res.writeHead(code, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" }); res.end(html); };
    if (!linkingOn()) { page(503, authPage("Not ready yet", "<p>Discord linking isn't switched on on this server yet.</p>")); return; }
    if (!STATE_RE.test(state)) { page(400, authPage("Something's off", "<p>Go back to Skeletoings and press <b>Link Discord</b> again.</p>")); return; }
    if (!pendingAuth.has(state) && pendingAuth.size < 5000) pendingAuth.set(state, { at: Date.now() });
    const q = new URLSearchParams({ client_id: DISCORD_ID, response_type: "code", redirect_uri: PUBLIC_URL + "/auth/discord/callback",
                                    scope: "identify guilds.members.read", state, prompt: "consent" });
    res.writeHead(302, { "Location": "https://discord.com/oauth2/authorize?" + q.toString(), "Cache-Control": "no-store" });
    res.end();
    return;
  }
  if (url.pathname === "/auth/discord/callback") {
    const page = (code, html) => { res.writeHead(code, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" }); res.end(html); };
    const code = String(url.searchParams.get("code") || "");
    const state = String(url.searchParams.get("state") || "");
    if (!linkingOn() || !code || !STATE_RE.test(state)) { page(400, authPage("Not linked", "<p>You cancelled, or the link was broken. Go back to the game and try again.</p>")); return; }
    discordCallback(code, state).then((r) => page(200, r.page))
      .catch((e) => { log("discord callback failed:", e.message); page(502, authPage("Discord didn't answer", "<p>Try again in a minute.</p>")); });
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
