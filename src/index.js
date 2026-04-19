// src/index.js
// ─────────────────────────────────────────────────────────────────────────────
// Point d'entrée du worker
// ─────────────────────────────────────────────────────────────────────────────

import 'dotenv/config';
import { startScheduler } from './queue/scheduler.js';
import { startServer } from './server.js';
import { logger } from './utils/logger.js';

// Vérification des variables d'environnement critiques
const REQUIRED_ENV = ['SUPABASE_URL', 'SUPABASE_SERVICE_KEY'];
const missing = REQUIRED_ENV.filter(key => !process.env[key]);

if (missing.length > 0) {
  logger.error(`Variables d'env manquantes: ${missing.join(', ')}`);
  logger.error('Copie .env.example → .env et remplis les valeurs');
  process.exit(1);
}

if (!process.env.META_ACCESS_TOKEN && process.env.USE_PLAYWRIGHT_FALLBACK !== 'true') {
  logger.warn('⚠ META_ACCESS_TOKEN non défini et Playwright fallback désactivé');
  logger.warn('⚠ Aucune source de scraping disponible !');
}

logger.info('╔══════════════════════════════════════╗');
logger.info('║     Afriwinner Worker — Démarrage       ║');
logger.info('╚══════════════════════════════════════╝');
logger.info(`Environnement: ${process.env.NODE_ENV || 'development'}`);
logger.info(`Supabase: ${process.env.SUPABASE_URL}`);
logger.info(`Meta API: ${process.env.META_ACCESS_TOKEN ? '✓ configuré' : '✗ non configuré'}`);
logger.info(`Playwright fallback: ${process.env.USE_PLAYWRIGHT_FALLBACK === 'true' ? '✓ activé' : '✗ désactivé'}`);

// Démarre le serveur HTTP (health checks + triggers manuels)
startServer();

// Démarre le planificateur de scraping
startScheduler();