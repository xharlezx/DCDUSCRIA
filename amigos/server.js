// Servidor de sinalização: só apresenta os navegadores uns aos outros.
// O áudio, o vídeo e a tela vão direto entre os participantes (WebRTC).
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ACCESS_CODE = process.env.ACCESS_CODE || ''; // opcional: senha para entrar
const MAX_PER_ROOM = 6;

const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
if (process.env.TURN_URL) {
  iceServers.push({
    urls: process.env.TURN_URL,
    username: process.env.TURN_USER,
    credential: process.env.TURN_PASS,
  });
}

const server = http.createServer((req, res) => {
  if (req.url === '/ice') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers, needsCode: !!ACCESS_CODE }));
  }
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('Erro'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const rooms = new Map(); // sala -> Map(id -> { ws, name })
let nextId = 1;
const send = (ws, msg) => ws.readyState === 1 && ws.send(JSON.stringify(msg));

const wss = new WebSocketServer({ server });
wss.on('connection', (ws) => {
  let room = null;
  let me = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join' && !room) {
      if (ACCESS_CODE && msg.code !== ACCESS_CODE) {
        return send(ws, { type: 'error', message: 'Código de acesso incorreto.' });
      }
      const roomName = String(msg.room || 'geral').slice(0, 40);
      const members = rooms.get(roomName) || new Map();
      if (members.size >= MAX_PER_ROOM) {
        return send(ws, { type: 'error', message: `Sala cheia (máximo ${MAX_PER_ROOM}).` });
      }
      room = roomName;
      me = String(nextId++);
      const name = String(msg.name || 'Alguém').slice(0, 24);
      send(ws, {
        type: 'joined', id: me,
        peers: [...members].map(([id, m]) => ({ id, name: m.name })),
      });
      members.forEach((m) => send(m.ws, { type: 'peer-joined', id: me, name }));
      members.set(me, { ws, name });
      rooms.set(room, members);
      return;
    }

    if (!room) return;
    const members = rooms.get(room);
    if (msg.type === 'signal' || msg.type === 'screen') {
      const out = { ...msg, from: me };
      if (msg.to) {
        const target = members.get(msg.to);
        if (target) send(target.ws, out);
      } else {
        members.forEach((m, id) => id !== me && send(m.ws, out));
      }
    }
  });

  ws.on('close', () => {
    if (!room) return;
    const members = rooms.get(room);
    members.delete(me);
    members.forEach((m) => send(m.ws, { type: 'peer-left', id: me }));
    if (!members.size) rooms.delete(room);
  });
});

server.listen(PORT, () => console.log('Rodando na porta ' + PORT));
