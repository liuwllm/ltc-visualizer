FROM node:24-slim

WORKDIR /app

# Dependencies first so code changes don't invalidate this layer.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

CMD ["npm", "start"]
