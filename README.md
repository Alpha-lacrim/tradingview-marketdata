# Experimental multi-asset market data

The complete application is in **[PycharmProjects/tradingview](PycharmProjects/tradingview/)**.
The existing four implementation commits and their directory layout have been preserved.

Start with the **[project README and review guide](PycharmProjects/tradingview/README.md)**
for architecture, provider boundaries, PostgreSQL/TimescaleDB migrations, REST/WebSocket
APIs, local setup and tests. Run development commands from the project directory:

```text
cd PycharmProjects/tradingview
```

The TradingView guest transport is **experimental and unsupported**. It retrieves native
completed one-minute candles; mock data is the default configuration. Live capacity was
tested through 64 series with source omissions; 100 live instruments remain unverified.
See the [measured live/synthetic results](PycharmProjects/tradingview/docs/candle-retrieval-scalability.md)
and [current handoff](PycharmProjects/tradingview/HANDOFF.md).

Local credentials, database history/volumes, dumps, logs and raw generated artifacts are
excluded. Sanitized benchmark summaries, fixtures, source, tests, configuration templates,
migrations and deployment files are included. Repository visibility is private.
