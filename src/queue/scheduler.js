// src/queue/scheduler.js
// ─────────────────────────────────────────────────────────────────────────────
// Planificateur de jobs — tourne en continu sur le worker
// Exécute les scrapes toutes les X heures selon la config
// ─────────────────────────────────────────────────────────────────────────────

import cron from 'node-cron';
import PQueue from 'p-queue';
import { runScraperJob, runMultiCountryJob } from '../processors/dispatcher.js';
import { getStats } from '../db/supabase.js';
import { logger } from '../utils/logger.js';

// ─────────────────────────────────────────────
// Config depuis .env
// ─────────────────────────────────────────────
const INTERVAL_HOURS = parseInt(process.env.SCRAPE_INTERVAL_HOURS || '5');
const TARGET_COUNTRIES = (process.env.TARGET_COUNTRIES || 'CI,SN,CM,BF,ML,TG').split(',').map(s => s.trim());
const TARGET_NICHES = (process.env.TARGET_NICHES || 'mode,beauté,santé,électronique').split(',').map(s => s.trim());
const ADS_PER_RUN = parseInt(process.env.ADS_PER_RUN || '200');

// ─────────────────────────────────────────────
// Mots-clés par niche adaptés au marché africain
// ─────────────────────────────────────────────
const KEYWORDS_BY_NICHE = {
  mode: ['robe africaine', 'wax mode', 'pagne tendance', 'mode femme afrique', 'ensemble ankara', 'dashiki', 'tenue mariage africain'],
  beauté: ['crème éclaircissante', 'produit beauté afrique', 'sérum visage', 'huile naturelle cheveux', 'cosmétique naturel', 'soin peau noire'],
  santé: ['produit minceur afrique', 'complément naturel', 'tisane santé', 'produit bien-être', 'perte de poids rapide'],
  électronique: ['téléphone pas cher afrique', 'accessoire smartphone', 'écouteur bluetooth', 'chargeur rapide', 'montre connectée prix'],
  cuisine: ['ustensile cuisine afrique', 'épice naturelle', 'produit culinaire', 'machine cuisine', 'article ménager'],
  fitness: ['équipement sport maison', 'corde à sauter', 'bande résistance fitness', 'protéine musculation', 'tapis yoga'],
  maison: ['article maison pas cher', 'décoration africaine', 'meuble abordable', 'rangement maison', 'literie'],
  enfants: ['jouet enfant afrique', 'vêtement bébé', 'article puériculture', 'fourniture scolaire', 'livre enfant'],
  business: ['formation business ligne', 'outil entrepreneur', 'logiciel gestion', 'dropshipping afrique', 'e-commerce formation'],
};

// Queue pour éviter de saturer l'API (max 1 job simultané)
const queue = new PQueue({ concurrency: 1, interval: 1000, intervalCap: 1 });

// ─────────────────────────────────────────────
// Génère le plan de scraping pour un run complet
// ─────────────────────────────────────────────
function buildScrapingPlan(targetNiches, targetCountries, totalAds) {
  const plan = [];
  const adsPerNiche = Math.floor(totalAds / targetNiches.length);

  for (const niche of targetNiches) {
    const keywords = KEYWORDS_BY_NICHE[niche] || [niche];
    const adsPerKeyword = Math.ceil(adsPerNiche / keywords.length);

    for (const keyword of keywords) {
      // Pour chaque mot-clé : scrape ALL + quelques pays spécifiques prioritaires
      plan.push({ keyword, country: 'ALL', niche, limit: adsPerKeyword });

      // Pays prioritaires pour certaines niches
      if (['mode', 'beauté'].includes(niche)) {
        plan.push({ keyword, country: 'CI', niche, limit: Math.ceil(adsPerKeyword / 2) });
        plan.push({ keyword, country: 'SN', niche, limit: Math.ceil(adsPerKeyword / 2) });
      }
    }
  }

  return plan;
}

// ─────────────────────────────────────────────
// Exécute un run complet
// ─────────────────────────────────────────────
async function runFullScrape() {
  const startTime = Date.now();
  logger.info('\n' + '═'.repeat(60));
  logger.info(`DÉMARRAGE RUN COMPLET — ${new Date().toISOString()}`);
  logger.info(`Niches: ${TARGET_NICHES.join(', ')}`);
  logger.info(`Pays: ${TARGET_COUNTRIES.join(', ')}`);
  logger.info(`Total pubs visées: ${ADS_PER_RUN}`);

  const plan = buildScrapingPlan(TARGET_NICHES, TARGET_COUNTRIES, ADS_PER_RUN);
  logger.info(`Plan: ${plan.length} jobs planifiés`);

  let totalFound = 0;
  let totalInserted = 0;
  let errors = 0;

  for (let i = 0; i < plan.length; i++) {
    const job = plan[i];
    logger.info(`\n[${i + 1}/${plan.length}] "${job.keyword}" (${job.country})`);

    try {
      await queue.add(async () => {
        const result = await runScraperJob(job);
        totalFound += result.ads_found || 0;
        totalInserted += result.ads_inserted || 0;
      });
    } catch (err) {
      errors++;
      logger.error(`Job failed: ${err.message}`);
    }

    // Pause entre jobs pour être respectueux des APIs
    if (i < plan.length - 1) {
      await new Promise(r => setTimeout(r, 2000));
    }
  }

  const duration = Math.round((Date.now() - startTime) / 1000);
  const stats = await getStats().catch(() => ({ total_ads: '?', ads_last_24h: '?' }));

  logger.info('\n' + '═'.repeat(60));
  logger.info(`RUN TERMINÉ en ${duration}s`);
  logger.info(`Trouvées: ${totalFound} | Insérées: ${totalInserted} | Erreurs: ${errors}`);
  logger.info(`Total en base: ${stats.total_ads} | Dernières 24h: ${stats.ads_last_24h}`);
  logger.info('═'.repeat(60) + '\n');
}

// ─────────────────────────────────────────────
// Démarre le planificateur
// ─────────────────────────────────────────────
export function startScheduler() {
  // Convertit les heures en expression cron
  // Ex: 5h = toutes les 5 heures = "0 */5 * * *"
  const cronExpression = `0 */${INTERVAL_HOURS} * * *`;
  logger.info(`[Scheduler] Cron: "${cronExpression}" (toutes les ${INTERVAL_HOURS}h)`);

  // Premier run immédiat au démarrage
  logger.info('[Scheduler] Premier run immédiat au démarrage...');
  runFullScrape().catch(err => logger.error(`[Scheduler] Run initial failed: ${err.message}`));

  // Puis selon le cron
  const task = cron.schedule(cronExpression, () => {
    logger.info(`[Scheduler] Cron déclenché — ${new Date().toISOString()}`);
    runFullScrape().catch(err => logger.error(`[Scheduler] Run cron failed: ${err.message}`));
  });

  logger.info('[Scheduler] ✓ Planificateur démarré');

  // Graceful shutdown
  process.on('SIGTERM', () => {
    logger.info('[Scheduler] SIGTERM reçu — arrêt gracieux');
    task.stop();
    process.exit(0);
  });

  process.on('SIGINT', () => {
    logger.info('[Scheduler] SIGINT reçu — arrêt gracieux');
    task.stop();
    process.exit(0);
  });

  return task;
}

// ─────────────────────────────────────────────
// Trigger manuel (via API HTTP)
// ─────────────────────────────────────────────
export async function triggerManualRun(opts = {}) {
  const { keyword, country = 'ALL', niche = '', limit = 50 } = opts;

  if (keyword) {
    return runScraperJob({ keyword, country, niche, limit });
  }
  return runFullScrape();
}