// Crownfield Arena Build 98 — stateful multiplayer relay.
// Private room codes and Quick Match use the same Durable Object coordination point.
// The relay stores room-code -> host mapping in Durable Object storage so a join is
// not dependent on a transient in-memory lookup alone.

const peers = new Map();
const links = new Map();
const rooms = new Map();
const seekers = new Map();
const PID = /^[A-Za-z0-9-]{6,100}$/;
const CID = /^[A-Za-z0-9]{6,40}$/;
const ROOM = /^[A-HJ-NP-Z2-9]{5}$/;
const GAMES = new Set(['chess', 'pool', 'whot', 'ludo']);
const NAMEOK = s => String(s == null ? '' : s).replace(/[<>&"'`]/g, '').trim().slice(0, 14);
const GRACE_MS = 45000;
const DEAD_MS = 70000;
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function newCode() {
  const b = new Uint8Array(5); crypto.getRandomValues(b);
  let s = ''; for (const x of b) s += ALPHA[x % ALPHA.length];
  return s;
}

// Server-side cleanup: sockets that went silent (half-open mobile links) are closed so
// nobody is matched with a ghost and dead peers do not linger.
let sweeper = 0;
function sweep() {
  const now = Date.now();
  for (const s of [...seekers.values()]) {
    if (now - s.seen > DEAD_MS) { try { s.ws.close(); } catch (_) {} if (seekers.delete(s.id) && s.g) pushList(s.g); }
  }
  for (const p of [...peers.values()]) {
    if (!p.away && now - p.seen > DEAD_MS) { try { p.kill && p.kill(); } catch (_) {} }
  }
}
function ensureSweeper() { if (!sweeper) sweeper = setInterval(sweep, 15000); }

export function stats() {
  return { peers: peers.size, links: links.size, rooms: rooms.size, seekers: seekers.size };
}

async function roomPut(storage, code, hostId) {
  if (!storage || !ROOM.test(code)) return;
  rooms.set(code, hostId);
  try { await storage.put('room:' + code, { hostId, updatedAt: Date.now() }); } catch (_) {}
}

async function roomGet(storage, code) {
  if (!ROOM.test(code)) return null;
  const local = rooms.get(code);
  if (local) return local;
  if (!storage) return null;
  try {
    const v = await storage.get('room:' + code);
    if (v && v.hostId) { rooms.set(code, v.hostId); return v.hostId; }
  } catch (_) {}
  return null;
}

async function roomDelete(storage, code, hostId) {
  if (!ROOM.test(code)) return;
  if (rooms.get(code) === hostId) rooms.delete(code);
  if (!storage) return;
  try {
    const v = await storage.get('room:' + code);
    if (!v || v.hostId === hostId) await storage.delete('room:' + code);
  } catch (_) {}
}

export async function peerAttach(ws, id, k, storage) {
  ws.accept(); ensureSweeper();
  const send = o => { try { ws.send(JSON.stringify(o)); } catch (_) {} };
  if (!id) id = 'cfa-r-' + crypto.randomUUID().replace(/-/g, '').slice(0, 16);
  if (!PID.test(id) || k.length < 12 || k.length > 100) {
    send({ t: 'err', type: 'server-error' }); try { ws.close(); } catch (_) {} return;
  }

  const old = peers.get(id);
  if (old && old.k !== k) { send({ t: 'err', type: 'unavailable-id' }); try { ws.close(); } catch (_) {} return; }

  const me = { id, k, ws, send: null, cids: new Set(), n: 0, t: 0, dials: [], room: id.startsWith('cfa-') ? id.slice(4) : '', seen: Date.now(), away: 0, buf: [] };
  me.send = o => {
    if (me.away) { if (me.buf.length < 300) me.buf.push(o); return; }
    try { me.ws.send(JSON.stringify(o)); } catch (_) {}
  };
  let resumed = [];
  if (old) {
    // Same player reconnecting: TAKE OVER the existing links so the opponent's session survives.
    try { old.ws.close(); } catch (_) {}
    old.dead = true;
    for (const cid of old.cids) {
      const l = links.get(cid); if (!l) continue;
      if (l.a === old) l.a = me; if (l.b === old) l.b = me;
      me.cids.add(cid); resumed.push(cid);
    }
    me.buf = old.buf || [];
    old.cids.clear();
    clearTimeout(old.grace);
  }
  peers.set(id, me);

  if (ROOM.test(me.room)) {
    // Register the room before advertising readiness. The in-memory map makes the
    // same-instance path immediate; storage makes the mapping durable.
    await roomPut(storage, me.room, id);
  }
  const sendNow = o => { try { ws.send(JSON.stringify(o)); } catch (_) {} };
  sendNow({ t: 'open', id, v: 3, room: ROOM.test(me.room) ? me.room : '', resumed });
  { const b = me.buf; me.buf = []; for (const o of b) sendNow(o); }

  ws.addEventListener('message', async ev => {
    if (me.dead) return;
    if (typeof ev.data !== 'string' || ev.data.length > 40000) return;
    const now = Date.now() / 1000 | 0;
    if (me.t !== now) { me.t = now; me.n = 0; }
    if (++me.n > 240) return;
    let m; try { m = JSON.parse(ev.data); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    me.seen = Date.now();
    if (m.t === 'p') return sendNow({ t: 'p', s: Date.now() });

    if (m.t === 'dial') {
      const cid = String(m.cid || ''), requestedTo = String(m.to || '');
      const nw = Date.now(); me.dials = me.dials.filter(x => nw - x < 10000);
      if (me.dials.length >= 40 || !CID.test(cid) || me.cids.size >= 8) {
        return send({ t: 'err', type: 'server-error', cid });
      }
      me.dials.push(nw);

      let targetId = requestedTo;
      let o = peers.get(targetId);
      // Room-code joins use cfa-XXXXX. Resolve that code through DO storage.
      if (!o && /^cfa-[A-HJ-NP-Z2-9]{5}$/.test(requestedTo)) {
        const hostId = await roomGet(storage, requestedTo.slice(4));
        if (hostId) { targetId = hostId; o = peers.get(hostId); }
      }
      if (!o || o === me) return send({ t: 'err', type: 'peer-unavailable', cid, to: requestedTo });
      if (links.has(cid)) return;

      links.set(cid, { a: me, b: o, acc: false });
      me.cids.add(cid); o.cids.add(cid);
      const meta = m.meta && typeof m.meta === 'object' && JSON.stringify(m.meta).length < 500 ? m.meta : null;
      o.send({ t: 'in', cid, from: me.id, meta });
      return;
    }

    const l = links.get(String(m.cid || '')); if (!l) return;
    const other = l.a === me ? l.b : l.b === me ? l.a : null; if (!other) return;
    if (m.t === 'acc') {
      if (l.b === me && !l.acc) { l.acc = true; l.a.send({ t: 'acc', cid: m.cid }); }
      return;
    }
    if (m.t === 'd') { if (l.acc) other.send({ t: 'd', cid: m.cid, m: m.m }); return; }
    if (m.t === 'x') {
      links.delete(m.cid); l.a.cids.delete(m.cid); l.b.cids.delete(m.cid);
      other.send({ t: 'x', cid: m.cid });
    }
  });

  const gone = () => {
    if (me.dead || peers.get(id) !== me) return;
    // Do NOT tear down links immediately: mobile sockets flap. Keep the session for a grace period.
    me.away = Date.now();
    clearTimeout(me.grace);
    me.grace = setTimeout(() => {
      if (peers.get(id) !== me || !me.away) return;
      peers.delete(id);
      peerDrop(me, false, storage);
    }, GRACE_MS);
  };
  me.kill = () => { try { ws.close(); } catch (_) {} gone(); };
  ws.addEventListener('close', gone); ws.addEventListener('error', gone);
}

function peerDrop(me, quiet, storage) {
  if (peers.get(me.id) === me) peers.delete(me.id);
  if (ROOM.test(me.room)) roomDelete(storage, me.room, me.id);
  for (const cid of [...me.cids]) {
    const l = links.get(cid); links.delete(cid);
    if (l) {
      const o = l.a === me ? l.b : l.a;
      o.cids.delete(cid);
      if (!quiet) o.send({ t: 'x', cid });
    }
  }
  me.cids.clear();
}

function compat(a, b) { return a.g === b.g && a.d === b.d; }
function pushList(g) {
  const all = [...seekers.values()].filter(x => x.g === g);
  for (const x of all) {
    const active = all.filter(y => y.q && compat(x, y));
    const players = active.filter(p => p.id !== x.id).map(p => ({ id: p.id, name: p.name, lvl: p.lvl, d: p.d }));
    x.send({ t: 'list', g, d: x.d, n: active.length, players });
  }
}

function pair(a, b) {
  if (!compat(a, b) || a.id === b.id) return;
  seekers.delete(a.id); seekers.delete(b.id);
  let code = newCode();
  while (rooms.has(code)) code = newCode();
  a.send({ t: 'found', code, role: 'h', name: b.name, g: a.g, d: a.d });
  b.send({ t: 'found', code, role: 'j', name: a.name, g: b.g, d: b.d });
  pushList(a.g);
}

export function matchAttach(ws) {
  ws.accept(); ensureSweeper();
  const id = 'm' + crypto.randomUUID().replace(/-/g, '').slice(0, 10);
  const send = o => { try { ws.send(JSON.stringify(o)); } catch (_) {} };
  const me = { id, send, g: '', d: '', q: false, name: 'Player', lvl: 1, n: 0, t: 0, seen: Date.now(), ws };
  send({ t: 'hello', id, online: seekers.size, v: 2 });

  const leave = () => {
    const had = seekers.delete(id);
    if (had && me.g) pushList(me.g);
  };

  ws.addEventListener('message', ev => {
    if (typeof ev.data !== 'string' || ev.data.length > 2000) return;
    const now = Date.now() / 1000 | 0;
    if (me.t !== now) { me.t = now; me.n = 0; }
    if (++me.n > 30) return;
    let m; try { m = JSON.parse(ev.data); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    me.seen = Date.now();
    if (m.t === 'p') return send({ t: 'p' });

    if (m.t === 'watch' || m.t === 'find') {
      if (!GAMES.has(m.g)) return send({ t: 'err', error: 'Unsupported game' });
      // Ludo REQUIRES an explicit dice mode ('1' or '2'); it is enforced here, not just in the UI.
      let d = '';
      if (m.g === 'ludo') {
        d = String(m.d == null ? '' : m.d);
        if (d !== '1' && d !== '2') return send({ t: 'err', error: 'Choose 1 die or 2 dice first' });
      }
      const old = me.g;
      me.g = m.g; me.d = d;
      me.q = m.t === 'find';
      me.name = NAMEOK(m.name) || me.name;
      me.lvl = Math.max(1, Math.min(999, Number(m.lvl) || me.lvl));
      seekers.set(id, me);
      if (old && old !== me.g) pushList(old);
      if (me.q) {
        const other = [...seekers.values()].find(x => x.q && x.id !== id && compat(x, me));
        if (other) return pair(other, me);
        send({ t: 'searching', g: me.g, d: me.d });
      }
      return pushList(me.g);
    }

    if (m.t === 'pick') {
      const o = seekers.get(String(m.id || ''));
      if (!o || !o.q || o.id === id || !me.g || !compat(o, me)) return send({ t: 'err', error: 'That player is no longer available' });
      return pair(o, me);
    }

    if (m.t === 'cancel') {
      me.q = false; send({ t: 'cancelled' }); if (me.g) pushList(me.g);
    }
  });
  ws.addEventListener('close', leave); ws.addEventListener('error', leave);
}
