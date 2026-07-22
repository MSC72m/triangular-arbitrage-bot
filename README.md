# Triangular Arbitrage Bot

A production-grade Go application that identifies and executes triangular arbitrage opportunities on the CoinEx cryptocurrency exchange in real time.

## Architecture

```
                          ┌──────────────┐
                          │   CoinEx     │
                          │   WebSocket  │
                          └──────┬───────┘
                                 │ order book updates
                          ┌──────▼───────┐
                          │   Market     │
                          │   Manager    │
                          └──────┬───────┘
                                 │
                 ┌───────────────▼───────────────┐
                 │       Arbitrage Engine         │
                 │  (scan → detect → execute)     │
                 └───────────────┬───────────────┘
                                 │ FOK orders
                 ┌───────────────▼───────────────┐
                 │     CoinEx REST API Client     │
                 └───────────────────────────────┘
```

The bot maintains a real-time order book for every traded market via WebSocket, continuously scans for three-leg arbitrage cycles (e.g., USDT → BTC → USDT), and executes Fill-or-Kill orders when the estimated profit exceeds a configurable threshold.

## Project Structure

```
.
├── cmd/bot/main.go            # Application entry point
├── internal/
│   ├── config/                # Configuration loading and validation
│   ├── execution/             # Arbitrage detection and execution engine
│   ├── exchange/              # CoinEx API client (REST + auth)
│   ├── logging/               # Structured logging infrastructure
│   ├── market/                # Market data processing
│   └── network/               # HTTP client, WebSocket, rate limiting
├── pkg/
│   ├── common/                # Shared utilities
│   └── models/                # Core data types (OrderBook, Metrics, etc.)
├── .github/workflows/ci.yml  # GitHub Actions CI pipeline
├── .golangci-lint.yml         # Linter configuration
├── Makefile                   # Build, test, lint targets
└── go.mod
```

## Prerequisites

- Go 1.24 or later
- CoinEx API key and secret (set via environment variables)

## Configuration

The bot reads configuration from `config.json` (see `config.example.json` for a template) and environment variables:

| Variable | Description |
|---|---|
| `COINEX_API_KEY` | CoinEx API key |
| `COINEX_SECRET_ID` | CoinEx API secret |

## Build and Run

```bash
# Install dependencies
make deps

# Build
make build

# Run
make run

# Or build and run in one step
make run
```

### Cross-platform builds

```bash
make build-all-platforms   # builds for linux/amd64 and darwin/arm64
```

## Testing

```bash
make test                    # run all tests with race detector
make cover                   # run with coverage report (HTML)
go test -race ./...          # run with race detector
```

## CI/CD

This project uses GitHub Actions for continuous integration. On every push and pull request to `main`:

1. **Lint** — runs `golangci-lint` with the project configuration
2. **Test** — runs the full test suite with race detection and coverage
3. **Build** — compiles the binary and runs `go vet`

## Health Check

The bot exposes a health check endpoint at `GET :8080/healthz` that returns `200 OK` when the process is running.

## License

MIT
