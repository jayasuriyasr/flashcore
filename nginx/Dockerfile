# Build context is the repo root (see docker-compose.yml) so both frontend/ and nginx/ are available.
FROM node:22-alpine AS build
WORKDIR /src
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-fund --no-audit
COPY frontend/ ./
RUN npm run build

# 1.28+: the `resolve` parameter on upstream servers is available in open-source nginx.
FROM nginx:1.28-alpine
COPY nginx/nginx.conf /etc/nginx/nginx.conf
COPY --from=build /src/dist /usr/share/nginx/html
