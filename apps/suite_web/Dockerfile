# --- build stage ---
FROM node:20-alpine AS build
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .
# build a production bundle (fast, no HMR, low memory at runtime)
RUN npm run build


# --- runtime stage ---
FROM nginx:alpine AS runtime

# Nginx config to serve SPA and proxy /api to backend
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Vite build output
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
