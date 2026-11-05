ARG NODE_VERSION=26
ARG NGINX_VERSION=1.31
FROM node:${NODE_VERSION}-slim AS build
WORKDIR /app/report
COPY report/package.json report/package-lock.json ./
RUN npm ci --ignore-scripts
COPY report/ ./
COPY results/sample /app/results/sample
RUN npm run build

FROM nginx:${NGINX_VERSION}-alpine
COPY report/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/report/dist /usr/share/nginx/html
