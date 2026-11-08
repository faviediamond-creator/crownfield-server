// Crownfield Arena Build 98 — production Worker + Durable Object multiplayer backend.
import { peerAttach, matchAttach, stats } from './relay.js';

export class Lobby {
  constructor(state, env) { this.state = state; this.env = env; }
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === '/health') {
      return json({ ok: true, service: 'crownfield-arena-relay', architecture: 'durable-object', build: 98, multiplayer: 'ready', ...stats() });
    }
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('Expected WebSocket', { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    if (u.pathname === '/match') matchAttach(server);
    else await peerAttach(server, u.searchParams.get('id') || '', u.searchParams.get('k') || '', this.state.storage);
    return new Response(null, { status: 101, webSocket: client });
  }
}

const CORS = { 'access-control-allow-origin': '*', 'cache-control': 'no-store' };
function json(o) { return new Response(JSON.stringify(o), { headers: { 'content-type': 'application/json', ...CORS } }); }

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const stub = () => env.LOBBY.get(env.LOBBY.idFromName('main'));
    if (u.pathname === '/health') return stub().fetch(req);
    if (u.pathname === '/peer' || u.pathname === '/ws' || u.pathname === '/match') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('Expected WebSocket', { status: 426, headers: CORS });
      const allow = (env.ALLOWED_ORIGINS || '*').split(',').map(x => x.trim());
      const origin = req.headers.get('Origin');
      if (origin && !allow.includes('*') && !allow.includes(origin)) return new Response('Origin not allowed', { status: 403 });
      return stub().fetch(new Request(new URL((u.pathname === '/match' ? '/match' : '/peer') + u.search, req.url), req));
    }
    if (env.ASSETS) return env.ASSETS.fetch(req);
    return new Response('Crownfield Arena online server is running.', { headers: CORS });
  }
};
