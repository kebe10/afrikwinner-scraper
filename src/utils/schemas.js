// src/utils/schemas.js
import { z } from 'zod';

// Schéma d'une pub brute (avant traitement)
export const RawAdSchema = z.object({
  archive_id: z.string().min(1),
  page_name: z.string().default(''),
  ad_text: z.string().default(''),
  ad_image_urls: z.array(z.string().url()).default([]),
  ad_video_urls: z.array(z.string().url()).default([]),
  start_date: z.string().nullable().default(null),
  end_date: z.string().nullable().default(null),
  country: z.string().default('ALL'),
  niche: z.string().default(''),
  product_link: z.string().nullable().default(null),
  currency: z.string().default('XOF'),
  source: z.enum(['meta_api', 'playwright']).default('meta_api'),
});

// Schéma d'une pub traitée (prête pour Supabase)
export const ProcessedAdSchema = z.object({
  archive_id: z.string().min(1),
  platform: z.string().default('facebook'),
  page_name: z.string(),
  ad_text: z.string(),
  ad_image_urls: z.array(z.string()),
  ad_video_urls: z.array(z.string()),
  start_date: z.string().nullable(),
  country: z.string(),
  niche: z.string(),
  product_name: z.string().nullable(),
  product_link: z.string().nullable(),
  score: z.number().min(0).max(200),
  source: z.string(),
});

// Schéma de la réponse Meta API
export const MetaApiResponseSchema = z.object({
  data: z.array(z.object({
    id: z.string(),
    page_name: z.string().optional(),
    ad_creative_bodies: z.array(z.string()).optional(),
    ad_creative_link_captions: z.array(z.string()).optional(),
    ad_creative_link_urls: z.array(z.string()).optional(),
    ad_snapshot_url: z.string().optional(),
    ad_delivery_start_time: z.string().optional(),
    ad_delivery_stop_time: z.string().optional().nullable(),
    images: z.array(z.string()).optional(),
    videos: z.array(z.object({ video_preview_image_url: z.string().optional() })).optional(),
    currency: z.string().optional(),
    publisher_platforms: z.array(z.string()).optional(),
  })),
  paging: z.object({
    cursors: z.object({
      before: z.string().optional(),
      after: z.string().optional(),
    }).optional(),
    next: z.string().optional(),
  }).optional(),
});

export const ScraperJobSchema = z.object({
  keyword: z.string().min(1),
  country: z.string().default('ALL'),
  niche: z.string().default(''),
  limit: z.number().min(1).max(500).default(50),
  source: z.enum(['meta_api', 'playwright', 'auto']).default('auto'),
});