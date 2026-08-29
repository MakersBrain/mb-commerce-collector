# syntax=docker/dockerfile:1.7
# The catalogue explorer, including the /ops pages.
#
# adapter-node, so the server routes run: /ops needs them for the SSE proxy that
# attaches the control token, and that proxy is the reason the token never
# reaches the browser.
FROM node:22-slim AS build

WORKDIR /src
# .npmrc carries the registry mapping for @makersbrain/ui and no credential.
# The credential arrives as a BuildKit secret mounted only for this RUN, so it
# never lands in a layer or in the pushed image.
COPY package.json package-lock.json .npmrc ./
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc \
    npm ci
COPY . .
RUN npm run build && npm prune --omit=dev


FROM node:22-slim

WORKDIR /srv
COPY --from=build /src/build ./build
COPY --from=build /src/node_modules ./node_modules
COPY --from=build /src/package.json ./
COPY --from=build /src/config ./config

RUN useradd --system --uid 10001 explorer
USER explorer

ENV NODE_ENV=production \
    PORT=3000 \
    BODY_SIZE_LIMIT=1M
EXPOSE 3000
CMD ["node", "build"]
