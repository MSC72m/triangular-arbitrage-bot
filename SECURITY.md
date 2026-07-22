# Security Policy

## API Key Management

- **Never commit** `config.json` or `.env` to version control.
- Use the `.env.example` template and keep real secrets in environment variables or a vault.
- The bot supports paper trading (`SimulationMode: true`) — always validate in simulation first.

## Trading Limits

Configure these in `config.json` to cap financial exposure:

| Setting | Purpose |
|---|---|
| `maxDailySpend` | Maximum USD spent per day |
| `minOrderAmount` / `maxOrderAmount` | Per-order bounds |
| `maxVolumeFraction` | Fraction of available order-book depth used (0.0–1.0) |
| `enableSpendingLimits` | Master switch for all spending caps |

## Rate Limiting

The bot enforces CoinEx API rate limits (`rateLimitPerSecond`, `rateLimitPerMinute`).
Do not lower these below the exchange's published limits.

## Reporting a Vulnerability

Open an issue at https://github.com/anomalyco/triangular-arbitrage-bot/issues
