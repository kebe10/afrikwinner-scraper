// src/scrapers/playwright-scraper.js
// ─────────────────────────────────────────────────────────────────────────────
// MÉTHODE FALLBACK — Playwright avec interception GraphQL
// Plus robuste que le querySelector DOM (Facebook change le DOM souvent)
// On intercepte les appels réseau GraphQL pour extraire les données directement
// ─────────────────────────────────────────────────────────────────────────────

import { chromium } from 'playwright';
import { logger } from '../utils/logger.js';

const HEADLESS = process.env.PLAYWRIGHT_HEADLESS !== 'false';
const DELAY_MS = parseInt(process.env.PLAYWRIGHT_DELAY_MS || '3000');
const PROXY_LIST = (process.env.PROXY_LIST || '').split(',').filter(Boolean);

// ─────────────────────────────────────────────
// Rotation de proxies
// ─────────────────────────────────────────────
let proxyIndex = 0;
function getNextProxy() {
  if (PROXY_LIST.length === 0) return null;
  const proxy = PROXY_LIST[proxyIndex % PROXY_LIST.length];
  proxyIndex++;
  try {
    const url = new URL(proxy);
    return {
      server: `${url.protocol}//${url.hostname}:${url.port}`,
      username: url.username || undefined,
      password: url.password || undefined,
    };
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────
// Normalise une pub brute Playwright → format interne
// ─────────────────────────────────────────────
function normalizePlaywrightAd(raw, country, niche) {
  let daysActive = 0;
  if (raw.start_date) {
    // Gère les formats : "1 jan. 2024", "2024-01-01", etc.
    const parsed = new Date(raw.start_date);
    if (!isNaN(parsed.getTime())) {
      daysActive = Math.floor((Date.now() - parsed.getTime()) / 86_400_000);
    }
  }

  return {
    archive_id: raw.archive_id,
    platform: 'facebook',
    page_name: raw.page_name || '',
    ad_text: raw.ad_text || '',
    ad_image_urls: raw.ad_image_urls || [],
    ad_video_urls: raw.ad_video_urls || [],
    start_date: raw.start_date || null,
    days_active: Math.max(0, daysActive),
    country: country || 'ALL',
    niche: niche || '',
    product_name: (raw.ad_text || '').split('\n')[0]?.slice(0, 80) || null,
    product_link: raw.product_link || null,
    score: calculateScore(daysActive),
    source: 'playwright',
  };
}

function calculateScore(daysActive) {
  if (daysActive > 90) return 130;
  if (daysActive > 45) return 90;
  if (daysActive > 20) return 60;
  if (daysActive > 7)  return 30;
  return Math.max(1, daysActive * 2.5);
}

// ─────────────────────────────────────────────
// Extraire les pubs depuis les réponses GraphQL interceptées
// ─────────────────────────────────────────────
function extractAdsFromGraphQL(responseBody) {
  const ads = [];
  try {
    const json = JSON.parse(responseBody);

    // Facebook peut retourner plusieurs formats GraphQL
    // On cherche récursivement des objets qui ressemblent à des pubs
    const findAds = (obj, depth = 0) => {
      if (depth > 10 || !obj || typeof obj !== 'object') return;

      // Signal d'une pub : présence d'archive_id ou collation_id
      if (obj.archive_id || obj.collation_id) {
        const archiveId = String(obj.archive_id || obj.collation_id || '');
        if (archiveId.length > 3) {
          const adBody = obj.snapshot || obj;
          ads.push({
            archive_id: archiveId,
            page_name: obj.page_name || adBody.page_name || obj.byline || '',
            ad_text: [
              adBody.body?.markup?.__html || adBody.body?.text || '',
              adBody.caption || '',
              adBody.title || '',
              adBody.link_description || '',
            ].filter(Boolean).join('\n').replace(/<[^>]+>/g, '').trim(),
            ad_image_urls: (adBody.images || [])
              .map(img => img?.original_image_url || img?.resized_image_url || '')
              .filter(Boolean),
            ad_video_urls: (adBody.videos || [])
              .map(v => v?.video_preview_image_url || '')
              .filter(Boolean),
            start_date: obj.ad_delivery_start_time
              ? new Date(obj.ad_delivery_start_time * 1000).toISOString()
              : null,
            product_link: adBody.link_url || adBody.page_link || null,
          });
        }
      }

      for (const val of Object.values(obj)) {
        if (val && typeof val === 'object') findAds(val, depth + 1);
      }
    };

    findAds(json);
  } catch (e) {
    // Pas du JSON valide ou format non reconnu — normal pour certaines requêtes
  }
  return ads;
}

// ─────────────────────────────────────────────
// Scrape par interception GraphQL (méthode stable)
// ─────────────────────────────────────────────
async function scrapeViaGraphQLInterception(page, keyword, country, limit) {
  const interceptedAds = [];

  // Intercepte toutes les réponses GraphQL/XHR de Facebook
  page.on('response', async (response) => {
    const url = response.url();
    if (
      !url.includes('facebook.com') ||
      (!url.includes('graphql') && !url.includes('ads/library'))
    ) return;

    try {
      const body = await response.text();
      if (body.length < 100) return;
      const ads = extractAdsFromGraphQL(body);
      if (ads.length > 0) {
        logger.debug(`[Playwright] GraphQL: ${ads.length} pubs interceptées`);
        interceptedAds.push(...ads);
      }
    } catch {
      // Réponse non lisible (binaire, CORS, etc.) — on ignore
    }
  });

  const countryParam = country !== 'ALL' ? `&country=${country}` : '&country=ALL';
  const url = `https://www.facebook.com/ads/library/?active_status=active&ad_type=all&q=${encodeURIComponent(keyword)}${countryParam}&media_type=all&search_type=keyword_unordered`;

  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  await page.waitForTimeout(DELAY_MS);

  // Scroll progressif pour déclencher le chargement des pubs
  let scrollCount = 0;
  const maxScrolls = Math.ceil(limit / 10);

  while (interceptedAds.length < limit && scrollCount < maxScrolls) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(2000);
    scrollCount++;
    logger.debug(`[Playwright] Scroll ${scrollCount}/${maxScrolls} | ${interceptedAds.length} pubs`);
  }

  return interceptedAds;
}

// ─────────────────────────────────────────────
// Scrape fallback par DOM (si GraphQL échoue)
// ─────────────────────────────────────────────
async function scrapeViaDOM(page) {
  return page.evaluate(() => {
    const results = [];

    // Facebook Ads Library : plusieurs sélecteurs possibles selon la version du DOM
    // On utilise une approche "large" et on filtre après
    const containers = document.querySelectorAll(
      'div[data-testid="ad-archive-renderer"], ' +
      'div._7jyg, ' +
      'div.x1ywc1zp' // classes générées — peuvent changer
    );

    for (const el of containers) {
      try {
        // Cherche l'ID d'archive
        const archiveLink = el.querySelector('a[href*="id="]');
        const archiveId = archiveLink?.href?.match(/id=(\d+)/)?.[1];
        if (!archiveId) continue;

        // Texte de la pub
        const textEls = el.querySelectorAll('div[style*="white-space"]');
        const adText = Array.from(textEls).map(e => e.innerText).join('\n').trim();

        // Page
        const pageEl = el.querySelector('a[href*="facebook.com/"] span, strong');
        const pageName = pageEl?.textContent?.trim() || '';

        // Images
        const images = Array.from(el.querySelectorAll('img[src*="scontent"]'))
          .map(img => img.src).filter(Boolean).slice(0, 5);

        // Date de début
        const allSpans = Array.from(el.querySelectorAll('span'));
        const dateSpan = allSpans.find(s =>
          s.textContent?.match(/\d{1,2}\s+\w+\s+\d{4}/) ||
          s.textContent?.includes('Started running')
        );
        const startDate = dateSpan?.textContent?.trim() || null;

        results.push({ archive_id: archiveId, page_name: pageName, ad_text: adText, ad_image_urls: images, ad_video_urls: [], start_date: startDate });
      } catch {
        // Élément malformé — on skip
      }
    }
    return results;
  });
}

// ─────────────────────────────────────────────
// FONCTION PRINCIPALE
// ─────────────────────────────────────────────
export async function scrapeWithPlaywright({ keyword, country = 'ALL', niche = '', limit = 40 }) {
  logger.info(`[Playwright] Scraping "${keyword}" | pays: ${country} | limite: ${limit}`);

  const proxy = getNextProxy();
  const launchOptions = {
    headless: HEADLESS,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
    ],
    ...(proxy ? { proxy } : {}),
  };

  const browser = await chromium.launch(launchOptions);

  try {
    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      locale: 'fr-FR',
      timezoneId: 'Africa/Abidjan',
      viewport: { width: 1280, height: 800 },
      // Bloque les ressources non nécessaires pour accélérer le scraping
      extraHTTPHeaders: { 'Accept-Language': 'fr-FR,fr;q=0.9' },
    });

    // Bloque images, polices, médias pour aller plus vite
    await context.route('**/*', (route) => {
      const type = route.request().resourceType();
      if (['image', 'media', 'font', 'stylesheet'].includes(type)) {
        return route.abort();
      }
      return route.continue();
    });

    const page = await context.newPage();

    // Essaie d'abord l'interception GraphQL (plus fiable)
    let rawAds = await scrapeViaGraphQLInterception(page, keyword, country, limit);

    // Si pas assez de résultats, fallback DOM
    if (rawAds.length < 5) {
      logger.info('[Playwright] GraphQL insuffisant, fallback DOM...');
      rawAds = await scrapeViaDOM(page);
    }

    // Dédoublonner par archive_id
    const seen = new Set();
    const unique = rawAds.filter(ad => {
      if (!ad.archive_id || seen.has(ad.archive_id)) return false;
      seen.add(ad.archive_id);
      return true;
    });

    // Normaliser
    const normalized = unique
      .map(ad => {
        try { return normalizePlaywrightAd(ad, country, niche); }
        catch { return null; }
      })
      .filter(Boolean)
      .filter(ad => ad.ad_text.length > 10)
      .slice(0, limit);

    logger.info(`[Playwright] "${keyword}" → ${normalized.length} pubs (${rawAds.length} brutes)`);
    return normalized;
  } finally {
    await browser.close();
  }
}