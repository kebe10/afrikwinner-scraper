// src/server.js
// ─────────────────────────────────────────────────────────────────────────────
// Serveur HTTP léger pour :
//   - Health check (Railway/Render en ont besoin pour savoir que le worker tourne)
//   - Déclencher un scrape manuellement via API
//   - Consulter les stats
// ─────────────────────────────────────────────────────────────────────────────

import { createServer } from 'http';
import { triggerManualRun } from './queue/scheduler.js';
import { getStats } from './db/supabase.js';
import { logger } from './utils/logger.js';

const PORT = parseInt(process.env.PORT || '3001');

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data, null, 2));
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try { resolve(body ? JSON.parse(body) : {}); }
      catch { reject(new Error('Invalid JSON')); }
    });
  });
}

export function startServer() {
  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${PORT}`);

    // ── GET /health ─────────────────────────────
    if (req.method === 'GET' && url.pathname === '/health') {
      return sendJson(res, 200, {
        status: 'ok',
        uptime: Math.round(process.uptime()),
        timestamp: new Date().toISOString(),
        version: '1.0.0',
      });
    }

    // ── GET /stats ──────────────────────────────
    if (req.method === 'GET' && url.pathname === '/stats') {
      try {
        const stats = await getStats();
        return sendJson(res, 200, { ok: true, ...stats });
      } catch (err) {
        return sendJson(res, 500, { ok: false, error: err.message });
      }
    }

    // ── POST /scrape ─────────────────────────────
    // Body JSON: { keyword, country, niche, limit }
    if (req.method === 'POST' && url.pathname === '/scrape') {
      // Sécurité basique : bearer token
      const auth = req.headers.authorization;
      const expectedToken = process.env.WORKER_SECRET;
      if (expectedToken && auth !== `Bearer ${expectedToken}`) {
        return sendJson(res, 401, { ok: false, error: 'Unauthorized' });
      }

      try {
        const body = await parseBody(req);
        logger.info('[Server] Scrape manuel déclenché', body);

        // Lance le scrape en arrière-plan (pas de await ici)
        triggerManualRun(body).catch(err =>
          logger.error(`[Server] Manual scrape failed: ${err.message}`)
        );

        return sendJson(res, 202, { ok: true, message: 'Scrape lancé en arrière-plan', params: body });
      } catch (err) {
        return sendJson(res, 400, { ok: false, error: err.message });
      }
    }

    // ── 404 ──────────────────────────────────────
    return sendJson(res, 404, { ok: false, error: 'Not found' });
  });

  server.listen(PORT, () => {
    logger.info(`[Server] Worker HTTP sur port ${PORT}`);
    logger.info(`[Server] Endpoints: GET /health | GET /stats | POST /scrape`);
  });

  return server;
}