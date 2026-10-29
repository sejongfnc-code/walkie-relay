// Push-to-listen walkie-talkie relay server.
// One listener + MULTIPLE sources per room. The listener selects which source
// it is currently listening to; the server forwards that source's audio/location
// only. Control (start/stop) is sent to a specific target source.
//
// Run:  PORT=8080 node server.js

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// room -> { listener: ws|null, sources: Map<sourceId, ws>, selected: sourceId|null }
const rooms = new Map();

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { listener: null, sources: new Map(), selected: null });
  return rooms.get(code);
}

function sourceList(r) {
  return Array.from(r.sources.entries()).map(([id, s]) => ({ id, name: s.name || id }));
}

function sendSources(r) {
  if (r.listener && r.listener.readyState === WebSocket.OPEN) {
    r.listener.send(JSON.stringify({
      type: 'sources',
      list: sourceList(r),
      selected: r.selected,
    }));
  }
}

// ---- Error reports (POST /report, GET /reports?key=...) ----
const fs = require('fs');
const path = require('path');
const REPORT_KEY = process.env.REPORT_KEY || 'A1tiDmozBZnNhM846ZrzzYnfcyVn9tPU';
const REPORT_FILE = process.env.REPORT_FILE || path.join(__dirname, 'reports.jsonl');
const reports = [];
try {
  if (fs.existsSync(REPORT_FILE)) {
    for (const l of fs.readFileSync(REPORT_FILE, 'utf8').split('\n').slice(-200)) {
      if (l.trim()) try { reports.push(JSON.parse(l)); } catch {}
    }
  }
} catch {}

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (req.method === 'POST' && url.pathname === '/report') {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 200000) req.destroy(); });
    req.on('end', () => {
      let o;
      try { o = JSON.parse(body); } catch { return json(res, 400, { ok: false, error: 'bad json' }); }
      if (o.key !== REPORT_KEY) return json(res, 403, { ok: false });
      delete o.key;
      o.receivedAt = new Date().toISOString();
      reports.push(o);
      while (reports.length > 200) reports.shift();
      try { fs.appendFileSync(REPORT_FILE, JSON.stringify(o) + '\n'); } catch {}
      console.log(`[report] ${o.device} ${o.role} ${o.kind}: ${String(o.text).slice(0, 120)}`);
      json(res, 200, { ok: true });
    });
    return;
  }
  if (req.method === 'GET' && url.pathname === '/reports') {
    if (url.searchParams.get('key') !== REPORT_KEY) return json(res, 403, { ok: false });
    return json(res, 200, { ok: true, count: reports.length, reports });
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('walkie relay ok\n');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.role = null;
  ws.room = null;
  ws.sourceId = null;
  ws.name = null;

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // Audio frame: source -> listener, only if this source is the selected one.
      if (ws.role === 'source' && ws.room) {
        const r = rooms.get(ws.room);
        if (r && r.selected === ws.sourceId &&
            r.listener && r.listener.readyState === WebSocket.OPEN) {
          r.listener.send(data, { binary: true });
        }
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === 'join') {
      ws.role = msg.role;                 // 'listener' or 'source'
      ws.room = msg.room || 'default';
      const r = getRoom(ws.room);
      if (ws.role === 'source') {
        ws.sourceId = msg.sourceId || ('src-' + Math.random().toString(36).slice(2, 8));
        ws.name = msg.name || ws.sourceId;
        r.sources.set(ws.sourceId, ws);
        console.log(`[join] source id=${ws.sourceId} name=${ws.name} room=${ws.room}`);
      } else {
        r.listener = ws;
        console.log(`[join] listener room=${ws.room}`);
      }
      // keep listener's source list fresh
      sendSources(r);
      return;
    }

    // Listener picks which source to listen to.
    if (msg.type === 'select') {
      if (ws.role === 'listener' && ws.room) {
        const r = rooms.get(ws.room);
        if (r) {
          r.selected = msg.sourceId || null;
          console.log(`[select] -> ${r.selected} room=${ws.room}`);
          sendSources(r);
        }
      }
      return;
    }

    // Location: source -> listener, only if selected. Tag with sourceId.
    if (msg.type === 'location') {
      if (ws.role === 'source' && ws.room) {
        const r = rooms.get(ws.room);
        if (r && r.selected === ws.sourceId &&
            r.listener && r.listener.readyState === WebSocket.OPEN) {
          msg.sourceId = ws.sourceId;
          r.listener.send(JSON.stringify(msg));
          console.log(`[loc] ${ws.sourceId} -> listener room=${ws.room} ${msg.lat},${msg.lng}`);
        }
      }
      return;
    }

    // Control: start/stop from listener -> a specific target source.
    if (msg.type === 'start' || msg.type === 'stop') {
      if (ws.role === 'listener' && ws.room) {
        const r = rooms.get(ws.room);
        if (r) {
          const targetId = msg.sourceId || r.selected;
          const target = targetId ? r.sources.get(targetId) : null;
          if (target && target.readyState === WebSocket.OPEN) {
            target.send(JSON.stringify({ type: msg.type }));
            console.log(`[ctrl] ${msg.type} -> source ${targetId} room=${ws.room}`);
          }
        }
      }
      return;
    }
  });

  ws.on('close', () => {
    if (ws.room && rooms.has(ws.room)) {
      const r = rooms.get(ws.room);
      if (ws.role === 'source') {
        if (r.sources.get(ws.sourceId) === ws) r.sources.delete(ws.sourceId);
        if (r.selected === ws.sourceId) r.selected = null;
        sendSources(r);
      } else if (ws.role === 'listener') {
        if (r.listener === ws) r.listener = null;
      }
      console.log(`[close] role=${ws.role} id=${ws.sourceId} room=${ws.room}`);
    }
  });

  ws.on('error', () => {});
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Walkie relay listening on 0.0.0.0:${PORT}`);
});
