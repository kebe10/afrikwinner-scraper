// src/db/supabase.js
import { createClient } from '@supabase/supabase-js';
import { logger } from '../utils/logger.js';

// Service role key = accès complet, à utiliser uniquement côté worker (jamais exposé au frontend)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  {
    auth: { persistSession: false },
    db: { schema: 'public' },
  }
);

// ─────────────────────────────────────────────
// UPSERT des pubs (insert ou update si archive_id existe déjà)
// ─────────────────────────────────────────────
export async function upsertAds(ads) {
  if (!ads || ads.length === 0) return { inserted: 0, updated: 0 };

  const { data, error } = await supabase
    .from('ads')
    .upsert(ads, {
      onConflict: 'archive_id',
      ignoreDuplicates: false, // met à jour si la pub existe déjà (score, etc.)
    })
    .select('archive_id');

  if (error) {
    logger.error('Supabase upsertAds error', { error: error.message, count: ads.length });
    throw error;
  }

  logger.info(`Upserted ${data?.length ?? 0} ads into Supabase`);
  return { upserted: data?.length ?? 0 };
}

// ─────────────────────────────────────────────
// Récupère les archive_ids déjà en base (pour éviter de re-scraper)
// ─────────────────────────────────────────────
export async function getExistingArchiveIds(archiveIds) {
  if (!archiveIds || archiveIds.length === 0) return new Set();

  const { data, error } = await supabase
    .from('ads')
    .select('archive_id')
    .in('archive_id', archiveIds);

  if (error) {
    logger.error('Supabase getExistingArchiveIds error', { error: error.message });
    return new Set();
  }

  return new Set(data.map(row => row.archive_id));
}

// ─────────────────────────────────────────────
// Log d'un run de scraper (pour monitoring)
// ─────────────────────────────────────────────
export async function logScraperRun({ keyword, country, niche, source, ads_found, ads_inserted, duration_ms, error_msg }) {
  const { error } = await supabase.from('scraper_runs').insert({
    keyword,
    country,
    niche,
    source,
    ads_found: ads_found ?? 0,
    ads_inserted: ads_inserted ?? 0,
    duration_ms: duration_ms ?? 0,
    error_msg: error_msg ?? null,
    status: error_msg ? 'error' : 'success',
  });

  if (error) logger.warn('Failed to log scraper run', { error: error.message });
}

// ─────────────────────────────────────────────
// Stats globales (pour le dashboard de monitoring)
// ─────────────────────────────────────────────
export async function getStats() {
  const [totalAds, recentAds, topNiches] = await Promise.all([
    supabase.from('ads').select('*', { count: 'exact', head: true }),
    supabase.from('ads')
      .select('*', { count: 'exact', head: true })
      .gte('created_at', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()),
    supabase.from('ads')
      .select('niche')
      .not('niche', 'is', null)
      .limit(1000),
  ]);

  return {
    total_ads: totalAds.count ?? 0,
    ads_last_24h: recentAds.count ?? 0,
  };
}

export default supabase;