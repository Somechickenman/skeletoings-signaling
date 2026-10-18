# Used by Fly.io (Render does not need this; it runs "node server.js" directly).
FROM node:20-alpine
WORKDIR /app
COPY package.json server.js name_filter.json ./
ENV PORT=8080
EXPOSE 8080
CMD ["node", "server.js"]
