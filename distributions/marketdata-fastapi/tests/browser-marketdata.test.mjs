import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(new URL("../examples/browser-marketdata.js", import.meta.url), "utf8");
const { startMarketData } = await import("data:text/javascript;base64," + Buffer.from(source).toString("base64"));

test("bootstrap keeps refreshed prices and newer candle corrections", async () => {
  globalThis.location = { origin: "http://localhost" };
  const identity = { instrument_id: "one", canonical_symbol: "SIM:EXAMPLE",
    provider: "mock", provider_symbol: "EXAMPLE", supported_historical_intervals: ["1m"] };
  const candle = (received_timestamp, close) => ({ ...identity, interval: "1m",
    bucket_start: "2026-10-14T00:00:00Z", received_timestamp, close });
  let releaseReplay;
  globalThis.fetch = async input => {
    const url = new URL(input, location.origin);
    let body = {};
    if (url.pathname.endsWith("/instruments")) body = { items: [identity] };
    if (url.pathname.endsWith("/candles")) body = { items: [
      candle("2026-10-14T00:01:00.1Z", "new"),
      candle("2026-10-14T00:01:00Z", "old"),
      candle("2026-10-14T00:01:00.100001Z", "newer"),
      candle("2026-10-14T00:01:00.100000Z", "older"),
    ] };
    if (url.pathname.endsWith("/prices/latest")) body = { items: [{ ...identity, last: "new" }] };
    if (url.pathname.endsWith("/coverage")) body = { items: [] };
    if (url.pathname.endsWith("/marketdata/changes")) {
      if (url.searchParams.has("after")) await new Promise(resolve => { releaseReplay = resolve; });
      body = { items: [], cursor: "fence", has_more: false };
    }
    return { ok: true, json: async () => body };
  };
  class Socket {
    static OPEN = 1;
    constructor() { Socket.instance = this; this.readyState = 1; }
    send() {}
    close() { this.readyState = 3; }
  }
  globalThis.WebSocket = Socket;
  const prices = [], candles = [];
  const stream = await startMarketData({ symbols: [identity.canonical_symbol],
    onPrice: p => prices.push(p.last), onCandle: c => candles.push(c.close),
    onConnection() {}, onCoverage() {}, onError: assert.fail });
  try {
    const message = data => Socket.instance.onmessage({ data: JSON.stringify({ version: 1, ...data }) });
    const recovery = message({ type: "subscription_ack", request_id: "restore", snapshot_cursor: "fence" });
    while (!releaseReplay) await new Promise(resolve => setImmediate(resolve));
    await message({ type: "price_snapshot", items: [{ ...identity, last: "old" }] });
    releaseReplay();
    await recovery;
    assert.deepEqual(prices, ["new"]);
    assert.deepEqual(candles, ["new", "newer"]);
  } finally { stream.stop(); }
});

test("invalid history configuration stops reconnection", async () => {
  globalThis.location = { origin: "http://localhost" };
  globalThis.fetch = async input => {
    const path = new URL(input, location.origin).pathname;
    return { ok: !path.endsWith("/candles"), status: 422, json: async () =>
      path.endsWith("/instruments") ? { items: [{ canonical_symbol: "SIM:EXAMPLE",
        supported_historical_intervals: ["1m"] }] } : { cursor: "fence" } };
  };
  let reconnects = 0;
  class Socket {
    static OPEN = 1;
    constructor() { Socket.instance = this; this.readyState = 1; }
    send() {}
    close() { this.onclose?.({ code: 1000 }); }
  }
  globalThis.WebSocket = Socket;
  const originalTimeout = globalThis.setTimeout;
  const errors = [];
  const stream = await startMarketData({ symbols: ["SIM:EXAMPLE"],
    onError: e => errors.push(e), onConnection() {} });
  globalThis.setTimeout = () => { reconnects++; };
  try {
    await Socket.instance.onmessage({ data: JSON.stringify({ version: 1,
      type: "subscription_ack", request_id: "restore", snapshot_cursor: "fence" }) });
    assert.equal(errors[0].status, 422);
    assert.equal(reconnects, 0);
  } finally { globalThis.setTimeout = originalTimeout; stream.stop(); }
});
