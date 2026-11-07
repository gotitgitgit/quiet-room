'use strict';
/*
 * 아무도 듣지 않는 방 - 접속 중계 서버
 *
 * 하는 일: 같은 시간에 들어온 사람들의 위치와 앉은 자리, 번호표 순서만 서로에게 전달해요.
 * 글, 이름, 계정 정보는 받지도 저장하지도 않아요. 디스크에 아무것도 쓰지 않아요.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const MAX_PER_ROOM = Number(process.env.MAX_PER_ROOM) || 40; // 한 방에 동시에 들어가는 사람 수
const MAX_ROOMS = Number(process.env.MAX_ROOMS) || 25;       // 방이 가득 차면 새 방을 열어요
const MAX_PER_IP = Number(process.env.MAX_PER_IP) || 6;      // 같은 주소에서 동시에 열 수 있는 탭 수
const TRUST_PROXY = process.env.TRUST_PROXY !== '0';          // 호스팅 서비스 뒤에서는 그대로 두세요
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map((s) => s.trim()).filter(Boolean);
const TICK_MS = 100;

/* ---------- 정적 파일 (페이지 하나와 three.js) ---------- */
const ROUTES = {
  '/': { file: path.join(__dirname, 'public', 'index.html'), type: 'text/html; charset=utf-8', cache: 'no-cache' },
  '/index.html': { file: path.join(__dirname, 'public', 'index.html'), type: 'text/html; charset=utf-8', cache: 'no-cache' },
  '/three.min.js': { file: require.resolve('three/build/three.min.js'), type: 'application/javascript; charset=utf-8', cache: 'public, max-age=86400' },
};
const CSP = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline'",
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  'font-src https://fonts.gstatic.com',
  "img-src 'self' data: blob:",
  "media-src 'self' blob:",
  "connect-src 'self' ws: wss:",
  "frame-ancestors 'none'",
].join('; ');

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  const route = ROUTES[url];
  if (!route || (req.method !== 'GET' && req.method !== 'HEAD')) {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found');
    return;
  }
  fs.readFile(route.file, (err, buf) => {
    if (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('error');
      return;
    }
    res.writeHead(200, {
      'Content-Type': route.type,
      'Cache-Control': route.cache,
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer',
      'Content-Security-Policy': CSP,
    });
    res.end(req.method === 'HEAD' ? undefined : buf);
  });
});

/* ---------- 방 ---------- */
const rooms = []; // { conns: Map(id -> conn), dirty: Set(conn), left: [id] }
const ipCount = new Map();

function pickRoom() {
  for (const r of rooms) if (r.conns.size < MAX_PER_ROOM) return r;
  if (rooms.length < MAX_ROOMS) {
    const r = { conns: new Map(), dirty: new Set(), left: [] };
    rooms.push(r);
    return r;
  }
  return null;
}

function clientIp(req) {
  if (TRUST_PROXY) {
    const xf = req.headers['x-forwarded-for'];
    if (typeof xf === 'string' && xf.length) return xf.split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

/* ---------- 받은 상태 정리: 정해진 칸만, 정해진 범위만 통과시켜요 ---------- */
function num(v, a, b) {
  return typeof v === 'number' && Number.isFinite(v) ? Math.max(a, Math.min(b, v)) : null;
}
const r2 = (v) => Math.round(v * 100) / 100;
const SEAT_KEY = /^[LR][0-4]:[0-3]$/;

function sanitize(raw, c) {
  const s = raw.s;
  if (s !== 0 && s !== 1 && s !== 2) return null;
  const x = num(raw.x, -4.6, 4.6);
  const z = num(raw.z, -7, 6);
  if (x === null || z === null) return null;
  const yaw = num(raw.yaw, -7, 7);
  const out = { s, x: r2(x), z: r2(z), yaw: r2(yaw === null ? 0 : yaw) };
  if (s === 1 && typeof raw.k === 'string' && SEAT_KEY.test(raw.k)) out.k = raw.k;
  // 번호표 시각은 서버 시계로 찍어요. 사람이 보낸 값은 "있다/없다"로만 읽어요.
  if (raw.q && s !== 2) {
    if (!c.q) c.q = Date.now();
    out.q = c.q;
  } else {
    c.q = 0;
    c.tt = 0;
  }
  if (c.q && raw.tt) {
    if (!c.tt) c.tt = Date.now();
    out.tt = c.tt;
  } else {
    c.tt = 0;
  }
  return out;
}

/* ---------- 접속 ---------- */
const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: 1024,
  verifyClient: (info) => {
    const origin = info.origin;
    if (!origin) return true;
    if (ALLOWED_ORIGINS.length) return ALLOWED_ORIGINS.includes(origin);
    try {
      return new URL(origin).host === info.req.headers.host;
    } catch (e) {
      return false;
    }
  },
});

wss.on('connection', (ws, req) => {
  const ip = clientIp(req);
  const room = pickRoom();
  if (!room || (ipCount.get(ip) || 0) >= MAX_PER_IP) {
    ws.send(JSON.stringify({ t: 'full' }));
    ws.close(1013, 'full');
    return;
  }
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);

  const c = {
    id: crypto.randomBytes(6).toString('hex'),
    ws, room, ip,
    p: null, str: '', q: 0, tt: 0,
    alive: true, win: Date.now(), n: 0,
  };
  room.conns.set(c.id, c);

  const peers = [];
  for (const o of room.conns.values()) if (o !== c && o.p) peers.push({ id: o.id, p: o.p });
  ws.send(JSON.stringify({ t: 'hello', id: c.id, sn: Date.now(), peers }));

  ws.on('pong', () => { c.alive = true; });

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    if (now - c.win > 1000) { c.win = now; c.n = 0; }
    c.n += 1;
    if (c.n > 200) { ws.close(1008, 'rate'); return; }
    if (c.n > 60) return;
    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || m.t !== 'p' || !m.p || typeof m.p !== 'object') return;
    const clean = sanitize(m.p, c);
    if (!clean) return;
    const str = JSON.stringify(clean);
    if (str === c.str) return;
    c.str = str;
    c.p = clean;
    room.dirty.add(c);
  });

  const leave = () => {
    if (!room.conns.delete(c.id)) return;
    room.dirty.delete(c);
    if (c.p) room.left.push(c.id);
    const n = (ipCount.get(ip) || 1) - 1;
    if (n <= 0) ipCount.delete(ip); else ipCount.set(ip, n);
    if (room.conns.size === 0 && rooms.length > 1) {
      const i = rooms.indexOf(room);
      if (i >= 0) rooms.splice(i, 1);
    }
  };
  ws.on('close', leave);
  ws.on('error', leave);
});

/* 100ms마다 바뀐 사람만 모아서 한 번에 보내요 */
setInterval(() => {
  const sn = Date.now();
  for (const r of rooms) {
    if (!r.dirty.size && !r.left.length) continue;
    const u = [];
    for (const c of r.dirty) if (c.p) u.push({ id: c.id, p: c.p });
    const msg = JSON.stringify({ t: 's', sn, u, l: r.left });
    r.dirty.clear();
    r.left = [];
    for (const c of r.conns.values()) if (c.ws.readyState === 1) c.ws.send(msg);
  }
}, TICK_MS);

/* 응답 없는 연결은 정리해요 */
setInterval(() => {
  for (const r of rooms) {
    for (const c of Array.from(r.conns.values())) {
      if (!c.alive) { c.ws.terminate(); continue; }
      c.alive = false;
      try { c.ws.ping(); } catch (e) { /* ignore */ }
    }
  }
}, 30000);

server.listen(PORT, () => {
  console.log('quiet room listening on :' + PORT);
});

function shutdown() {
  wss.clients.forEach((ws) => ws.close(1001, 'restart'));
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
