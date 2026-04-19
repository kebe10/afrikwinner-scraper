// src/scrapers/meta-api.js
// ─────────────────────────────────────────────────────────────────────────────
// MÉTHODE PRINCIPALE — Meta Ad Library API (officielle, gratuite, légale)
// Doc: https://developers.facebook.com/docs/marketing-api/reference/ads_archive
//
// Prérequis :
//   1. Créer une app Meta sur developers.facebook.com
//   2. Activer le produit "Marketing API"
//   3. Générer un token d'accès (User Token ou System User Token)
//   4. Le token doit avoir la permission : ads_read
// ─────────────────────────────────────────────────────────────────────────────

import pRetry from 'p-retry';
import pLimit from 'p-limit';
import { logger } from '../utils/logger.js';
import { MetaApiResponseSchema } from '../utils/schemas.js';

const BASE_URL = 'https://graph.facebook.com/v21.0/ads_archive';
const TOKEN = process.env.META_ACCESS_TOKEN;
const DELAY_MS = parseInt(process.env.META_API_DELAY_MS || '1000');

// Champs à récupérer depuis l'API Meta
const AD_FIELDS = [
  'id',
  'page_name',
  'ad_creative_bodies',
  'ad_creative_link_captions',
  'ad_creative_link_descriptions',
  'ad_creative_link_urls',
  'ad_snapshot_url',
  'ad_delivery_start_time',
  'ad_delivery_stop_time',
  'currency',
  'publisher_platforms',
  'bylines',
].join(',');

// ─────────────────────────────────────────────
// Sleep helper
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ─────────────────────────────────────────────
// Appel API Meta avec retry automatique
// ─────────────────────────────────────────────
async function fetchMetaApi(params) {
  const url = new URL(BASE_URL);
  url.searchParams.set('access_token', TOKEN);
  url.searchParams.set('fields', AD_FIELDS);
  url.searchParams.set('ad_active_status', 'ACTIVE');

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await pRetry(
    async () => {
      const res = await fetch(url.toString());

      if (res.status === 429) {
        logger.warn('Meta API rate limit hit, waiting 60s...');
        await sleep(60_000);
        throw new pRetry.AbortError('Rate limited'); // ne pas retrier immédiatement
      }

      if (!res.ok) {
        const body = await res.text();
        throw new Error(`Meta API ${res.status}: ${body}`);
      }

      return res.json();
    },
    {
      retries: 3,
      minTimeout: 2000,
      maxTimeout: 10_000,
      onFailedAttempt: (err) => {
        logger.warn(`Meta API attempt ${err.attemptNumber} failed: ${err.message}`);
      },
    }
  );

  return response;
}

// ─────────────────────────────────────────────
// Scrape une page de résultats
// ─────────────────────────────────────────────
async function fetchPage(searchTerm, country, limit, afterCursor = null) {
  const params = {
    search_terms: searchTerm,
    ad_reached_countries: country === 'ALL' ? '' : `["${country}"]`,
    limit: Math.min(limit, 100), // max 100 par page Meta API
    ad_active_status: 'ACTIVE',
  };

  if (afterCursor) params.after = afterCursor;

  const data = await fetchMetaApi(params);

  // Validation Zod
  const parsed = MetaApiResponseSchema.safeParse(data);
  if (!parsed.success) {
    logger.warn('Meta API response schema mismatch', { errors: parsed.error.issues.slice(0, 3) });
    return { ads: data.data || [], nextCursor: null };
  }

  const nextCursor = parsed.data.paging?.cursors?.after || null;
  const hasNextPage = !!parsed.data.paging?.next;

  return {
    ads: parsed.data.data,
    nextCursor: hasNextPage ? nextCursor : null,
  };
}

// ─────────────────────────────────────────────
// Normalise une pub brute Meta API → format interne
// ─────────────────────────────────────────────
function normalizeMetaAd(raw, country, niche) {
  const adText = [
    ...(raw.ad_creative_bodies || []),
    ...(raw.ad_creative_link_descriptions || []),
    ...(raw.ad_creative_link_captions || []),
  ].filter(Boolean).join('\n').trim();

  // Extraire l'URL produit
  const productLinks = (raw.ad_creative_link_urls || []).filter(u =>
    u && !u.includes('facebook.com') && !u.includes('fb.com')
  );

  // Calculer les jours actifs
  let daysActive = 0;
  if (raw.ad_delivery_start_time) {
    const startDate = new Date(raw.ad_delivery_start_time);
    daysActive = Math.floor((Date.now() - startDate.getTime()) / 86_400_000);
  }

  return {
    archive_id: raw.id,
    platform: 'facebook',
    page_name: raw.page_name || '',
    ad_text: adText,
    ad_image_urls: [], // Les images nécessitent ad_snapshot_url (voir enrichisseur)
    ad_video_urls: [],
    start_date: raw.ad_delivery_start_time || null,
    days_active: Math.max(0, daysActive),
    country: country || 'ALL',
    niche: niche || '',
    product_name: extractProductName(adText),
    product_link: productLinks[0] || null,
    score: calculateScore(daysActive),
    source: 'meta_api',
  };
}

// ─────────────────────────────────────────────
// Algorithme de scoring basé sur la longévité
// ─────────────────────────────────────────────
function calculateScore(daysActive) {
  if (daysActive > 90) return 130;  // 🚀 Evergreen — winner prouvé
  if (daysActive > 45) return 90;   // 🔥 Winner — très fort signal
  if (daysActive > 20) return 60;   // 📈 Prometteur
  if (daysActive > 7)  return 30;
  return Math.max(1, daysActive * 2.5); // Nouveau — on ne sait pas encore
}

// ─────────────────────────────────────────────
// Extrait un nom de produit approximatif du texte
// ─────────────────────────────────────────────
function extractProductName(text) {
  if (!text) return null;
  // Heuristique simple : première ligne non vide, tronquée à 80 chars
  const firstLine = text.split('\n').find(l => l.trim().length > 3);
  return firstLine ? firstLine.trim().slice(0, 80) : null;
}

// ─────────────────────────────────────────────
// FONCTION PRINCIPALE — scrape un mot-clé complet
// avec pagination automatique
// ─────────────────────────────────────────────
export async function scrapeWithMetaApi({ keyword, country = 'ALL', niche = '', limit = 50 }) {
  if (!TOKEN) throw new Error('META_ACCESS_TOKEN non défini dans .env');

  logger.info(`[MetaAPI] Scraping "${keyword}" | pays: ${country} | niche: ${niche} | limite: ${limit}`);

  const allAds = [];
  let cursor = null;
  let page = 1;
  const maxPages = Math.ceil(limit / 100);

  while (allAds.length < limit && page <= maxPages) {
    logger.debug(`[MetaAPI] Page ${page}/${maxPages} | ${allAds.length}/${limit} pubs`);

    try {
      const { ads: rawAds, nextCursor } = await fetchPage(keyword, country, limit - allAds.length, cursor);

      if (!rawAds || rawAds.length === 0) {
        logger.info(`[MetaAPI] Plus de résultats à la page ${page}`);
        break;
      }

      // Normaliser chaque pub
      const normalized = rawAds
        .map(raw => {
          try { return normalizeMetaAd(raw, country, niche); }
          catch (e) { logger.warn(`[MetaAPI] Normalisation échouée pour ${raw.id}: ${e.message}`); return null; }
        })
        .filter(Boolean)
        .filter(ad => ad.ad_text.length > 10); // Filtrer les pubs sans contenu

      allAds.push(...normalized);
      cursor = nextCursor;
      page++;

      if (!nextCursor) break; // Pas d'autre page

      await sleep(DELAY_MS); // Respect du rate limit Meta
    } catch (err) {
      logger.error(`[MetaAPI] Erreur page ${page}: ${err.message}`);
      break;
    }
  }

  logger.info(`[MetaAPI] "${keyword}" → ${allAds.length} pubs récupérées`);
  return allAds;
}

// ─────────────────────────────────────────────
// Scrape plusieurs pays en parallèle (limité)
// ─────────────────────────────────────────────
export async function scrapeMultiCountry({ keyword, countries, niche, limitPerCountry = 30 }) {
  const limit = pLimit(2); // Max 2 pays en parallèle pour ne pas saturer le rate limit
  const results = await Promise.all(
    countries.map(country =>
      limit(() =>
        scrapeWithMetaApi({ keyword, country, niche, limit: limitPerCountry })
          .catch(err => {
            logger.error(`[MetaAPI] Pays ${country} failed: ${err.message}`);
            return [];
          })
      )
    )
  );
  return results.flat();
}