// Break Room Arcade server.
// 1) Serves the game page.
// 2) Relays "presence" between players in named rooms (used by the live table games).
// 3) Keeps today's leaderboards for the daily challenges, in memory.
// It holds no game logic. Everything resets if the server restarts; players' browsers
// re-send their own results for today when they reconnect, so boards fill back in.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DAILY_TZ = process.env.DAILY_TZ || 'Europe/Rome'; // the day rolls over at midnight here
const PAGE = path.join(__dirname, 'index.html');
const MAX_PEERS_PER_ROOM = 40;
const MAX_ROOMS_PER_CONN = 4;
const MAX_PRESENCE_BYTES = 4096;
const NAME_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const GAMES = new Set(['wordle', 'facts']);

function today() {
  try { return new Intl.DateTimeFormat('en-CA', { timeZone: DAILY_TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date()); }
  catch (e) { return new Date().toISOString().slice(0, 10); }
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (req.url === '/' || req.url.startsWith('/?')) {
    fs.readFile(PAGE, (err, buf) => {
      if (err) { res.writeHead(500); return res.end('Page missing'); }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(buf);
    });
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
const rooms = new Map(); // name -> Map(peerId -> { ws, presence })

/* ---------- daily boards ---------- */
const boards = new Map(); // day -> { wordle: Map(pid -> row), facts: Map(pid -> row) }
function boardFor(day, game) {
  let d = boards.get(day);
  if (!d) { d = { wordle: new Map(), facts: new Map() }; boards.set(day, d);
    for (const k of [...boards.keys()]) if (k < day && boards.size > 4) boards.delete(k); }
  return d[game];
}
function rowsFor(day, game, pid) {
  const rows = [...boardFor(day, game)].map(([id, r]) => Object.assign({ me: id === pid }, r));
  if (game === 'wordle') rows.sort((a, b) => (b.ok - a.ok) || (a.ms - b.ms) || (a.tries - b.tries));
  else rows.sort((a, b) => (b.score - a.score) || (a.ms - b.ms));
  return rows.slice(0, 100);
}
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, Math.round(Number(v) || 0)));
const clean = (s, n) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').slice(0, n);

/* ---------- relay ---------- */
function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function broadcast(room, obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const [id, p] of room) if (id !== exceptId && p.ws.readyState === 1) p.ws.send(msg);
}
function leaveRoom(ws, name) {
  const room = rooms.get(name); if (!room) return;
  const id = ws.id; ws.rooms.delete(name);
  if (room.get(id) && room.get(id).ws === ws) { room.delete(id); broadcast(room, { t: 'left', room: name, peer: id }); }
  if (room.size === 0) rooms.delete(name);
}

wss.on('connection', ws => {
  ws.isAlive = true; ws.rooms = new Set(); ws.id = null; ws.count = 0; ws.windowStart = Date.now();
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', data => {
    ws.isAlive = true;
    const now = Date.now();
    if (now - ws.windowStart > 1000) { ws.windowStart = now; ws.count = 0; }
    if (++ws.count > 120) return; // simple flood guard
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'hello') {
      if (typeof m.id === 'string' && /^[a-z0-9]{6,24}$/.test(m.id)) ws.id = m.id;
      send(ws, { t: 'day', day: today() });
      return;
    }
    if (m.t === 'board' || m.t === 'score') {
      if (!GAMES.has(m.game)) return;
      const day = today(), pid = clean(m.pid, 40);
      if (m.t === 'score') {
        if (m.day !== day || !pid) return;
        const b = boardFor(day, m.game);
        if (!b.has(pid) && b.size < 500) {
          const row = { n: clean(m.n, 18) || 'Player', e: clean(m.e, 4) || '🙂', ms: clamp(m.ms, 0, 3600000) };
          if (m.game === 'wordle') { row.ok = m.ok ? 1 : 0; row.tries = clamp(m.tries, 1, 6); }
          else { row.score = clamp(m.score, 0, 10); }
          b.set(pid, row);
        }
      }
      send(ws, { t: 'board', game: m.game, day, rows: rowsFor(day, m.game, pid) });
      return;
    }

    if (!ws.id || typeof m.room !== 'string' || !NAME_RE.test(m.room)) return;
    if (m.t === 'join') {
      if (ws.rooms.size >= MAX_ROOMS_PER_CONN && !ws.rooms.has(m.room)) return;
      let room = rooms.get(m.room); if (!room) { room = new Map(); rooms.set(m.room, room); }
      if (room.size >= MAX_PEERS_PER_ROOM && !room.has(ws.id)) { send(ws, { t: 'full', room: m.room }); return; }
      const old = room.get(ws.id);
      if (old && old.ws !== ws) { try { old.ws.rooms.delete(m.room); } catch (e) {} }
      room.set(ws.id, { ws, presence: old ? old.presence : {} });
      ws.rooms.add(m.room);
      send(ws, { t: 'peers', room: m.room, peers: [...room].map(([id, p]) => ({ peer: id, presence: p.presence })) });
      broadcast(room, { t: 'p', room: m.room, peer: ws.id, presence: room.get(ws.id).presence }, ws.id);
    } else if (m.t === 'pres') {
      const room = rooms.get(m.room); const me = room && room.get(ws.id);
      if (!me || me.ws !== ws || !m.patch || typeof m.patch !== 'object') return;
      const next = Object.assign({}, me.presence);
      for (const k in m.patch) { if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k]; }
      if (Buffer.byteLength(JSON.stringify(next)) > MAX_PRESENCE_BYTES) return;
      me.presence = next;
      broadcast(room, { t: 'p', room: m.room, peer: ws.id, presence: next }, ws.id);
    } else if (m.t === 'leave') {
      leaveRoom(ws, m.room);
    }
  });
  ws.on('close', () => { for (const name of [...ws.rooms]) leaveRoom(ws, name); });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) {} }
}, 25000);

server.listen(PORT, () => console.log('Break Room Arcade listening on ' + PORT));
