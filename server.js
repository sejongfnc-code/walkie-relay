// Push-to-listen walkie-talkie relay server.
// Forwards control messages (JSON text) and audio frames (binary PCM) between
// a "listener" and a "source" that share the same room code.
//
// Run:  PORT=8080 node server.js
// The server must be reachable from both phones over the internet.

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// room -> { listener: ws|null, source: ws|null }
const rooms = new Map();

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, { listener: null, source: null });
  return rooms.get(code);
}

const server = http.createServer((req, res) => {
  // simple health check
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('walkie relay ok\n');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.role = null;
  ws.room = null;

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // Audio frame: source -> listener only.
      if (ws.role === 'source' && ws.room) {
        const r = rooms.get(ws.room);
        if (r && r.listener && r.listener.readyState === WebSocket.OPEN) {
          r.listener.send(data, { binary: true });
        }
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === 'join') {
      ws.role = msg.role;           // 'listener' or 'source'
      ws.room = msg.room || 'default';
      const r = getRoom(ws.room);
      r[ws.role] = ws;
      console.log(`[join] role=${ws.role} room=${ws.room}`);
      // tell both peers about presence
      notifyPeer(ws, JSON.stringify({ type: 'peer', role: ws.role, status: 'joined' }));
      return;
    }

    // Location: source -> listener (forward JSON through).
    if (msg.type === 'location') {
      if (ws.role === 'source' && ws.room) {
        const r = rooms.get(ws.room);
        if (r && r.listener && r.listener.readyState === WebSocket.OPEN) {
          r.listener.send(JSON.stringify(msg));
          console.log(`[loc] -> listener room=${ws.room} ${msg.lat},${msg.lng}`);
        }
      }
      return;
    }

    // Control: start/stop from listener -> source.
    if (msg.type === 'start' || msg.type === 'stop') {
      if (ws.role === 'listener' && ws.room) {
        const r = rooms.get(ws.room);
        if (r && r.source && r.source.readyState === WebSocket.OPEN) {
          r.source.send(JSON.stringify({ type: msg.type }));
          console.log(`[ctrl] ${msg.type} -> source room=${ws.room}`);
        }
      }
    }
  });

  ws.on('close', () => {
    if (ws.room && rooms.has(ws.room)) {
      const r = rooms.get(ws.room);
      if (r[ws.role] === ws) r[ws.role] = null;
      // if source drops, nothing to clean beyond this
      console.log(`[close] role=${ws.role} room=${ws.room}`);
    }
  });

  ws.on('error', () => {});
});

function notifyPeer(ws, text) {
  const r = rooms.get(ws.room);
  if (!r) return;
  const other = ws.role === 'listener' ? r.source : r.listener;
  if (other && other.readyState === WebSocket.OPEN) other.send(text);
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Walkie relay listening on 0.0.0.0:${PORT}`);
});
