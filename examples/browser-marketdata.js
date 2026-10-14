// Provisional BFF integration. No service bearer token belongs in this browser module.
// Endpoint paths and login belong to the company's eventual frontend/session architecture.
export async function startMarketData({
  symbols, sessionUrl = "/session", apiBase = "/marketdata/api/v1",
  websocketPath = "/marketdata/ws", historyStart,
  onPrice = console.log, onCandle = console.log, onStatus = console.log,
  onCoverage = console.log,
  onConnection = console.log, onError = console.error,
}) {
  let stopped = false, socket, checkpoint = null, retry = 0, reconnectTimer, pingTimer;
  let generation = 0, controller = new AbortController(), recovering = false;
  let buffered = [], loaded = false;
  const candles = new Map();
  const minute = () => new Date(Math.floor(Date.now() / 60000) * 60000).toISOString();
  const start = historyStart || new Date(Date.now() - 86400000)
    .toISOString().slice(0, 16) + ":00Z";

  async function get(path, params = {}, signal = controller.signal) {
    const url = new URL(apiBase + path, location.origin);
    for (const [key, value] of Object.entries(params)) {
      for (const entry of Array.isArray(value) ? value : [value]) {
        if (entry != null) url.searchParams.append(key, entry);
      }
    }
    const response = await fetch(url, { credentials: "same-origin", signal });
    if (!response.ok) {
      const error = new Error(`Market data HTTP ${response.status}`);
      error.status = response.status;
      throw error;
    }
    return response.json();
  }

  async function requireSession(signal = controller.signal) {
    const response = await fetch(sessionUrl, { credentials: "same-origin", signal });
    if (!response.ok) {
      const error = new Error("Company login required");
      error.status = response.status;
      throw error;
    }
    // Do not invent a session JSON schema; a successful authorized company endpoint suffices.
  }

  await requireSession();
  const instruments = [];
  let after;
  do {
    const page = await get("/instruments", { after, limit: 1000 });
    instruments.push(...page.items);
    after = page.next_after;
  } while (after);
  const requested = new Set(symbols);
  if (!requested.size || requested.size > 100) throw new Error("Choose 1–100 canonical symbols");
  const sources = instruments.filter(item => requested.has(item.canonical_symbol));
  if ([...requested].some(symbol => !sources.some(item => item.canonical_symbol === symbol))) {
    throw new Error("Unknown canonical symbol");
  }

  function putCandle(candle) {
    const key = [candle.instrument_id, candle.provider, candle.provider_symbol,
      candle.interval, candle.bucket_start].join("|");
    const previous = candles.get(key);
    // REST can see a later correction while earlier journal events are replayed.
    // UTC serialization omits zero fractions; pad them before comparing, preserving microseconds.
    const timestampKey = value => value.replace(/(?:\.(\d+))?Z$/, (_, fraction = "") =>
      "." + fraction.padEnd(6, "0") + "Z");
    if (previous && timestampKey(previous.received_timestamp) >
        timestampKey(candle.received_timestamp)) return;
    candles.set(key, candle);
    onCandle(candle); // Upsert by logical key. Keep decimal strings; volume can be null.
  }

  async function refreshHistory(signal) {
    const end = minute();
    for (const source of sources) {
      if (!source.supported_historical_intervals.includes("1m")) continue;
      let cursor;
      do {
        const page = await get("/candles", { symbol: source.canonical_symbol,
          provider: source.provider, provider_symbol: source.provider_symbol,
          start, end, interval: "1m", limit: 1000, cursor }, signal);
        page.items.forEach(putCandle);
        cursor = page.next_cursor;
      } while (cursor);
    }
    loaded = true;
  }

  async function refreshPrices(signal) {
    let after;
    do {
      const page = await get("/prices/latest", { symbols: [...requested], after, limit: 1000 }, signal);
      page.items.forEach(onPrice); // Render price_type/basis and freshness explicitly.
      after = page.next_after;
    } while (after);
  }

  async function refreshCoverage(signal) {
    let after;
    do {
      const page = await get("/coverage", { symbols: [...requested], start,
        end: minute(), after, limit: 1000 }, signal);
      page.items.forEach(onCoverage); // Keep EXPECTED gaps, NOT_EXPECTED and UNKNOWN distinct.
      after = page.next_after;
    } while (after);
  }

  function apply(message) {
    if (message.type === "candle_completed" || message.type === "candle_corrected") {
      if (requested.has(message.data.canonical_symbol)) putCandle(message.data);
    } else if (message.type === "price_update") {
      if (requested.has(message.data.canonical_symbol)) onPrice(message.data);
    } else if (message.type === "price_snapshot") {
      message.items.forEach(onPrice); // candle_close is a completed close, never a live quote.
    } else if (message.type === "source_status") {
      onStatus(message); // freshness=stale/unavailable; closed requires reported session evidence.
    } else if (message.type === "heartbeat") {
      onConnection(message.database === "available" ? "connected; synchronized"
        : "connected; database unavailable; data may be stale");
    } else if (message.type === "error" || message.type === "resync_required") {
      onError(message);
    }
    if (message.cursor && message.database !== "unavailable") checkpoint = message.cursor;
  }

  async function recover(fence, signal) {
    if (!loaded) await refreshHistory(signal);
    let cursor = checkpoint;
    if (cursor) {
      try {
        let page;
        do {
          page = await get("/marketdata/changes", { after: cursor, until: fence, limit: 250 }, signal);
          page.items.forEach(apply); // Includes old recovered minutes and corrections, not just a tail.
          cursor = page.cursor;
        } while (page.has_more);
      } catch (error) {
        if (error.status !== 409) throw error;
        await refreshHistory(signal); // Expired/foreign epoch: refresh the entire loaded chart window.
      }
    }
    checkpoint = fence;
    await refreshPrices(signal);
    await refreshCoverage(signal);
  }

  async function open() {
    if (stopped) return;
    const current = ++generation;
    controller.abort();
    controller = new AbortController();
    const signal = controller.signal;
    try {
      await requireSession(signal);
      // First checkpoint is captured BEFORE historical reads, so bootstrap races are recoverable.
      if (!checkpoint) checkpoint = (await get("/marketdata/changes", {}, signal)).cursor;
      const url = new URL(websocketPath, location.origin);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      const connection = new WebSocket(url); // Cookie goes only to the same-origin BFF.
      socket = connection;
      let lastMessageAt = Date.now();
      recovering = true;
      buffered = [];
      connection.onopen = () => {
        onConnection("connected; synchronizing");
        connection.send(JSON.stringify({ action: "subscribe", symbols: [...requested], request_id: "restore" }));
        pingTimer = setInterval(() => {
          if (Date.now() - lastMessageAt > 45000) {
            connection.close(1000, "heartbeat timeout; resynchronize");
          } else if (connection.readyState === WebSocket.OPEN) {
            connection.send(JSON.stringify({ action: "ping" }));
          }
        }, 10000);
      };
      connection.onmessage = async event => {
        if (current !== generation || signal.aborted) return;
        lastMessageAt = Date.now();
        const message = JSON.parse(event.data);
        if (message.version !== 1) { connection.close(); return; }
        if (message.type === "error" || message.type === "resync_required") {
          onError(message);
          if (recovering) {
            if (["unknown_symbol", "subscription_limit", "invalid_command"].includes(message.code)) stopped = true;
            connection.close();
          }
          return;
        }
        if (message.type === "subscription_ack" && message.request_id === "restore") {
          try {
            await recover(message.snapshot_cursor, signal);
            if (current !== generation || signal.aborted) return;
            recovering = false;
            for (const pending of buffered) apply(pending);
            buffered = [];
            retry = 0;
            onConnection("connected; synchronized");
          } catch (error) {
            if (signal.aborted) return;
            onError(error);
            if ([401, 403, 404, 422].includes(error.status)) stopped = true;
            connection.close();
          }
        } else if (recovering) {
          // recover() fetches current prices after replay. The initial barrier snapshot is older.
          if (message.type === "price_snapshot") return;
          if (buffered.length >= 4096) {
            onError(new Error("Recovery buffer full; reconnect and retry REST resynchronization"));
            connection.close();
            return;
          }
          buffered.push(message);
        } else {
          apply(message);
        }
      };
      connection.onclose = event => {
        if (current !== generation) return;
        clearInterval(pingTimer);
        controller.abort();
        onConnection(`disconnected (${event.code}); data may be stale`);
        if (event.code === 1008) { stopped = true; onError("Session/origin denied; sign in again"); }
        schedule();
      };
    } catch (error) {
      if (signal.aborted) return;
      onError(error);
      if ([401, 403, 404, 422].includes(error.status)) stopped = true;
      schedule();
    }
  }

  function schedule() {
    if (stopped) return;
    const delay = Math.min(30000, 1000 * 2 ** Math.min(retry++, 5)) * (0.75 + Math.random() * 0.5);
    reconnectTimer = setTimeout(open, delay);
  }
  await open();
  return {
    instruments,
    stop() {
      stopped = true;
      controller.abort();
      clearTimeout(reconnectTimer);
      clearInterval(pingTimer);
      if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ action: "disconnect" }));
      socket?.close(1000, "client shutdown");
    },
  };
}
