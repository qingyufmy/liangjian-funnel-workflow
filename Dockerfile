FROM node:20-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3210 \
    TZ=Asia/Shanghai \
    LIANGJIAN_ROOT=/app \
    LIANGJIAN_PYTHON_BIN=/opt/liangjian-venv/bin/python \
    LIANGJIAN_WEB_DIST=dist/web

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app
RUN python3 -m venv /opt/liangjian-venv \
    && /opt/liangjian-venv/bin/pip install --no-cache-dir . \
    && npm ci \
    && npm run build \
    && npm prune --omit=dev

VOLUME ["/app/state", "/app/storage", "/app/outputs", "/app/cache"]
EXPOSE 3210
CMD ["node", "dist/server/index.js"]
