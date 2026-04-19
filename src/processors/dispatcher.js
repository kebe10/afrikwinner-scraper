// src/processors/dispatcher.js
// ─────────────────────────────────────────────────────────────────────────────
// Orchestrateur intelligent : choisit Meta API ou Playwright selon la config
// Gère la déduplication, le filtrage et l'upsert en base
// ─────────────────────────────────────────────────────────────────────────────

import { scrapeWithMetaApi, scrapeMultiCountry } from '../scrapers/meta-api.js';
import { scrapeWithPlaywright } from '../scrapers/playwright-scraper.js';
import { upsertAds, getExistingArchiveIds, logScraperRun } from '../db/supabase.js';
import { logger } from '../utils/logger.js';

// ─────────────────────────────────────────────
// Filtre les pubs de mauvaise qualité
// ─────────────────────────────────────────────
function filterQuality(ads) {
  return ads.filter(ad => {
    // Texte trop court = peu utile
    if (ad.ad_text.length < 15) return false;
    // Spam évident
    if (/test|lorem ipsum/i.test(ad.ad_text)) return false;
    // Doit avoir une date de début (sinon score impossible)
    // (on garde quand même mais on log)
    return true;
  });
}

// ─────────────────────────────────────────────
// Dédoublonner un tableau de pubs par archive_id
// ─────────────────────────────────────────────
function deduplicateAds(ads) {
  const seen = new Set();
  return ads.filter(ad => {
    if (!ad.archive_id || seen.has(ad.archive_id)) return false;
    seen.add(ad.archive_id);
    return true;
  });
}

// ─────────────────────────────────────────────
// Exécute un job de scraping complet pour un mot-clé
// ─────────────────────────────────────────────
export async function runScraperJob({ keyword, country = 'ALL', niche = '', limit = 50, source = 'auto' }) {
  const startTime = Date.now();
  let rawAds = [];
  let usedSource = source;
  let errorMsg = null;

  logger.info(`\n${'─'.repeat(50)}`);
  logger.info(`Job: "${keyword}" | ${country} | ${niche} | source: ${source}`);

  try {
    // ── Sélection de la source ──────────────────
    if (source === 'auto' || source === 'meta_api') {
      try {
        rawAds = await scrapeWithMetaApi({ keyword, country, niche, limit });
        usedSource = 'meta_api';
      } catch (err) {
        logger.warn(`[Dispatcher] Meta API failed: ${err.message}`);
        if (source === 'auto' && process.env.USE_PLAYWRIGHT_FALLBACK === 'true') {
          logger.info('[Dispatcher] Fallback → Playwright');
          rawAds = await scrapeWithPlaywright({ keyword, country, niche, limit });
          usedSource = 'playwright';
        } else {
          throw err;
        }
      }
    } else if (source === 'playwright') {
      rawAds = await scrapeWithPlaywright({ keyword, country, niche, limit });
      usedSource = 'playwright';
    }

    logger.info(`[Dispatcher] ${rawAds.length} pubs brutes récupérées via ${usedSource}`);

    // ── Déduplication interne ───────────────────
    const deduplicated = deduplicateAds(rawAds);

    // ── Filtrage qualité ────────────────────────
    const filtered = filterQuality(deduplicated);

    logger.info(`[Dispatcher] Après filtre: ${filtered.length}/${rawAds.length} pubs valides`);

    if (filtered.length === 0) {
      logger.warn(`[Dispatcher] Aucune pub valide pour "${keyword}" — job ignoré`);
      return { ads_found: 0, ads_inserted: 0, source: usedSource };
    }

    // ── Vérification des doublons en base ───────
    const archiveIds = filtered.map(a => a.archive_id);
    const existingIds = await getExistingArchiveIds(archiveIds);
    const newAds = filtered.filter(a => !existingIds.has(a.archive_id));
    const updatedAds = filtered.filter(a => existingIds.has(a.archive_id));

    logger.info(`[Dispatcher] ${newAds.length} nouvelles pubs | ${updatedAds.length} à mettre à jour`);

    // ── Upsert en base ──────────────────────────
    let upserted = 0;
    if (filtered.length > 0) {
      const result = await upsertAds(filtered);
      upserted = result.upserted;
    }

    const duration = Date.now() - startTime;

    await logScraperRun({
      keyword,
      country,
      niche,
      source: usedSource,
      ads_found: rawAds.length,
      ads_inserted: upserted,
      duration_ms: duration,
    });

    logger.info(`[Dispatcher] ✓ Job terminé en ${duration}ms | ${upserted} pubs upsertées`);

    return {
      ads_found: rawAds.length,
      ads_valid: filtered.length,
      ads_inserted: upserted,
      source: usedSource,
      duration_ms: duration,
    };
  } catch (err) {
    errorMsg = err.message;
    logger.error(`[Dispatcher] Job failed: ${err.message}`, { keyword, country, niche });

    await logScraperRun({
      keyword, country, niche, source: usedSource,
      ads_found: rawAds.length, ads_inserted: 0,
      duration_ms: Date.now() - startTime, error_msg: errorMsg,
    });

    throw err;
  }
}

// ─────────────────────────────────────────────
// Run multi-pays pour un mot-clé donné
// ─────────────────────────────────────────────
export async function runMultiCountryJob({ keyword, countries, niche, limitPerCountry = 30 }) {
  logger.info(`[Dispatcher] Multi-pays: "${keyword}" | ${countries.join(', ')}`);
  const results = [];

  for (const country of countries) {
    try {
      const result = await runScraperJob({ keyword, country, niche, limit: limitPerCountry });
      results.push({ country, ...result });
    } catch (err) {
      logger.error(`[Dispatcher] Pays ${country} failed: ${err.message}`);
      results.push({ country, error: err.message });
    }

    // Pause entre pays pour ne pas surcharger
    await new Promise(r => setTimeout(r, 1500));
  }

  return results;
}