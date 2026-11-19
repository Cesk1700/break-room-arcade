// Break Room Arcade server: serves the game page and relays "presence" between players.
// It holds no game logic. Rooms live in memory and disappear when everyone leaves.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PAGE = path.join(__dirname, 'index.html');
const MAX_PEERS_PER_ROOM = 40;
const MAX_ROOMS_PER_CONN = 4;
const MAX_PRESENCE_BYTES = 4096;
const NAME_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (req.url === '/' || req.url.startsWith('/?') || req.url.startsWith('/#')) {
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
