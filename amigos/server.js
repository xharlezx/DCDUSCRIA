// Servidor do Quintal: canais de texto + sala de voz (sinalização WebRTC).
// Áudio, vídeo e tela vão direto entre os amigos; o servidor só apresenta e guarda o chat.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const CODE = process.env.ACCESS_CODE || '';
const MAX_VOICE = 6;
const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
if (process.env.TURN_URL) iceServers.push({ urls: process.env.TURN_URL, username: process.env.TURN_USER, credential: process.env.TURN_PASS });

const server = http.createServer((req, res) => {
  if (req.url === '/ice') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ iceServers, needsCode: !!CODE }));
  }
  fs.readFile(path.join(__dirname, 'public', 'index.html'), (err, data) => {
    if (err) { res.writeHead(500); return res.end('Erro'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const channels = new Map([['geral', []], ['jogos', []]]); // histórico fica na memória
const clients = new Map(); // id -> { ws, name, voice }
let next = 1;
const send = (ws, m) => ws.readyState === 1 && ws.send(JSON.stringify(m));
const all = (m) => clients.forEach((c) => send(c.ws, m));
const members = () => ({ type: 'members', list: [...clients].map(([id, c]) => ({ id, name: c.name, voice: c.voice })) });

new WebSocketServer({ server }).on('connection', (ws) => {
  let me = null;

  function leaveVoice() {
    const c = clients.get(me);
    if (!c || !c.voice) return;
    c.voice = false;
    clients.forEach((x) => x.voice && send(x.ws, { type: 'peer-left', id: me }));
    all(members());
  }

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'hello' && !me) {
      if (CODE && m.code !== CODE) return send(ws, { type: 'error', message: 'Código de acesso incorreto.' });
      me = String(next++);
      clients.set(me, { ws, name: String(m.name || 'Alguém').slice(0, 24), voice: false });
      send(ws, { type: 'ready', id: me, channels: [...channels.keys()] });
      return all(members());
    }
    if (!me) return;
    const c = clients.get(me);

    if (m.type === 'history') {
      send(ws, { type: 'history', channel: m.channel, msgs: channels.get(m.channel) || [] });
    } else if (m.type === 'create-channel') {
      const n = String(m.name || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 24);
      if (n && !channels.has(n) && channels.size < 20) {
        channels.set(n, []);
        all({ type: 'channels', list: [...channels.keys()] });
      }
    } else if (m.type === 'chat') {
      const msgs = channels.get(m.channel);
      const text = String(m.text || '').slice(0, 500);
      if (!msgs || !text.trim()) return;
      const msg = { from: me, name: c.name, text, ts: Date.now() };
      msgs.push(msg);
      if (msgs.length > 200) msgs.shift();
      all({ type: 'chat', channel: m.channel, msg });
    } else if (m.type === 'voice-join') {
      const inVoice = [...clients].filter(([, x]) => x.voice);
      if (c.voice || inVoice.length >= MAX_VOICE) return send(ws, { type: 'voice-full' });
      send(ws, { type: 'voice-peers', peers: inVoice.map(([id, x]) => ({ id, name: x.name })) });
      inVoice.forEach(([, x]) => send(x.ws, { type: 'peer-joined', id: me, name: c.name }));
      c.voice = true;
      all(members());
    } else if (m.type === 'voice-leave') {
      leaveVoice();
    } else if (m.type === 'signal' || m.type === 'screen') {
      const out = { ...m, from: me };
      clients.forEach((x, id) => { if (id !== me && x.voice && (!m.to || m.to === id)) send(x.ws, out); });
    }
  });

  ws.on('close', () => {
    if (!me) return;
    leaveVoice();
    clients.delete(me);
    all(members());
  });
});

server.listen(PORT, () => console.log('Quintal rodando na porta ' + PORT));
