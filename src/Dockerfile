# ═══════════════════════════════════════════════
# Afriwinner Worker — Dockerfile
# Optimisé pour Railway, Render, Fly.io
# ═══════════════════════════════════════════════

FROM node:22-slim

# Dépendances système pour Playwright (Chromium)
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-noto \
    fonts-noto-cjk \
    fonts-liberation \
    ca-certificates \
    --no-install-recommends \
  && rm -rf /var/lib/apt/lists/*

# Indique à Playwright d'utiliser le Chromium système (pas de téléchargement)
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/bin
ENV PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium

WORKDIR /app

# Copie package.json en premier (optimisation cache Docker)
COPY package.json ./

# Installe les dépendances Node (sans télécharger les browsers Playwright)
RUN npm install --omit=dev && npx playwright install-deps chromium

# Copie le code source
COPY src/ ./src/

# Expose le port HTTP du worker
EXPOSE 3001

# Démarre le worker
CMD ["node", "src/index.js"]