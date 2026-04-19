// src/scrapers/meta-api.js
import pRetry from 'p-retry';
import pLimit from 'p-limit';
import { logger } from '../utils/logger.js';

const BASE_URL = 'https://graph.facebook.com/v21.0/ads_archive';
const TOKEN = process.env.META_ACCESS_TOKEN;
const DELAY_MS = parseInt(process.env.META_API_DELAY_MS || '1000');

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
].join(',');

// Pays supportés par l'API Meta
const SUPPORTED_COUNTRIES = ['CI', 'SN', 'CM', 'BF', 'ML', 'TG', 'GN', 'CD', 'MG', 'NE'];

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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
        throw new Error('Rate limited');
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

async function fetchPage(searchTerm, country, limit, afterCursor = null) {
  // ── CORRECTION : ad_reached_countries doit toujours être un pays valide ──
  // On utilise CI par défaut si ALL est passé
  const countryCode = (!country || country === 'ALL') ? 'CI' : country;

  const params = {
    search_terms: searchTerm,
    ad_reached_countries: `["${countryCode}"]`,
    limit: Math.min(limit, 100),
    ad_active_status: 'ACTIVE',
  };

  if (afterCursor) params.after = afterCursor;

  const data = await fetchMetaApi(params);

  const nextCursor = data.paging?.cursors?.after || null;
  const hasNextPage = !!data.paging?.next;

  return {
    ads: data.data || [],
    nextCursor: hasNextPage ? nextCursor : null,
    country: countryCode,
  };
}

function calculateScore(daysActive) {
  if (daysActive > 90) return 130;
  if (daysActive > 45) return 90;
  if (daysActive > 20) return 60;
  if (daysActive > 7)  return 30;
  return Math.max(1, Math.round(daysActive * 2.5));
}

function extractProductName(text) {
  if (!text) return null;
  const firstLine = text.split('\n').find(l => l.trim().length > 3);
  return firstLine ? firstLine.trim().slice(0, 80) : null;
}

function normalizeMetaAd(raw, country, niche) {
  const adText = [
    ...(raw.ad_creative_bodies || []),
    ...(raw.ad_creative_link_descriptions || []),
    ...(raw.ad_creative_link_captions || []),
  ].filter(Boolean).join('\n').trim();

  const productLinks = (raw.ad_creative_link_urls || []).filter(u =>
    u && !u.includes('facebook.com') && !u.includes('fb.com')
  );

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
    ad_image_urls: [],
    ad_video_urls: [],
    start_date: raw.ad_delivery_start_time || null,
    days_active: Math.max(0, daysActive),
    country: country || 'CI',
    niche: niche || '',
    product_name: extractProductName(adText),
    product_link: productLinks[0] || null,
    score: calculateScore(daysActive),
    source: 'meta_api',
  };
}

// ── FONCTION PRINCIPALE ──────────────────────────────────────────────────────
export async function scrapeWithMetaApi({ keyword, country = 'CI', niche = '', limit = 50 }) {
  if (!TOKEN) throw new Error('META_ACCESS_TOKEN non défini');

  // Si ALL → scrape les pays africains principaux un par un
  if (!country || country === 'ALL') {
    return scrapeMultiCountry({ keyword, countries: ['CI', 'SN', 'CM'], niche, limitPerCountry: Math.ceil(limit / 3) });
  }

  logger.info(`[MetaAPI] Scraping "${keyword}" | pays: ${country} | niche: ${niche} | limite: ${limit}`);

  const allAds = [];
  let cursor = null;
  let page = 1;
  const maxPages = Math.ceil(limit / 100);

  while (allAds.length < limit && page <= maxPages) {
    try {
      const { ads: rawAds, nextCursor, country: usedCountry } = await fetchPage(keyword, country, limit - allAds.length, cursor);

      if (!rawAds || rawAds.length === 0) break;

      const normalized = rawAds
        .map(raw => {
          try { return normalizeMetaAd(raw, usedCountry, niche); }
          catch (e) { return null; }
        })
        .filter(Boolean)
        .filter(ad => ad.ad_text.length > 10);

      allAds.push(...normalized);
      cursor = nextCursor;
      page++;

      if (!nextCursor) break;
      await sleep(DELAY_MS);
    } catch (err) {
      logger.error(`[MetaAPI] Erreur page ${page}: ${err.message}`);
      break;
    }
  }

  logger.info(`[MetaAPI] "${keyword}" → ${allAds.length} pubs récupérées`);
  return allAds;
}

// ── Multi-pays ───────────────────────────────────────────────────────────────
export async function scrapeMultiCountry({ keyword, countries, niche, limitPerCountry = 30 }) {
  const validCountries = countries.filter(c => c && c !== 'ALL');
  const limit = pLimit(2);

  const results = await Promise.all(
    validCountries.map(country =>
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