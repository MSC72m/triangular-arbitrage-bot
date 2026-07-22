FROM golang:1.24-alpine AS build

RUN apk add --no-cache gcc musl-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X triangular-arbitrage-bot/internal/version.Version=${VERSION} -X triangular-arbitrage-bot/internal/version.Commit=${COMMIT} -X triangular-arbitrage-bot/internal/version.BuildTime=${BUILD_TIME}" -o /build/bot .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

RUN adduser -D -u 1001 bot

WORKDIR /app

COPY --from=build /build/bot .
COPY config.example.json /app/config.json

RUN chown -R bot:bot /app

USER bot

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8080/healthz || exit 1

ENTRYPOINT ["./bot"]
CMD ["-config", "/app/config.json"]
