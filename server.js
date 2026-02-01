import express from "express";
import cors from "cors";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "1mb" }));

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "X-API-Key"],
  })
);

// =======================
// Auth (protect your MCP server)
// =======================
const API_KEY = process.env.API_KEY || "";
function requireAuth(req, res) {
  if (!API_KEY) return true; // auth disabled
  const hdr = req.header("x-api-key") || "";
  const bearer = req.header("authorization") || "";
  const ok = hdr === API_KEY || bearer === `Bearer ${API_KEY}`;
  if (!ok) {
    res.status(401).json({ error: "Unauthorized" });
    return false;
  }
  return true;
}

// sessionId -> { res, createdAt }
const sessions = new Map();

// =======================
// Alpha Vantage "function catalog"
// (grouped like the Alpha Vantage MCP tools page)
// =======================
const AV_FUNCTION_CATALOG = {
  core_stock_apis: [
    "TIME_SERIES_INTRADAY",
    "TIME_SERIES_DAILY",
    "TIME_SERIES_DAILY_ADJUSTED",
    "TIME_SERIES_WEEKLY",
    "TIME_SERIES_WEEKLY_ADJUSTED",
    "TIME_SERIES_MONTHLY",
    "TIME_SERIES_MONTHLY_ADJUSTED",
    "GLOBAL_QUOTE",
    "REALTIME_BULK_QUOTES",
    "SYMBOL_SEARCH",
    "MARKET_STATUS",
  ],
  options_data_apis: ["REALTIME_OPTIONS", "HISTORICAL_OPTIONS"],
  alpha_intelligence: [
    "NEWS_SENTIMENT",
    "EARNINGS_CALL_TRANSCRIPT",
    "TOP_GAINERS_LOSERS",
    "INSIDER_TRANSACTIONS",
    "ANALYTICS_FIXED_WINDOW",
    "ANALYTICS_SLIDING_WINDOW",
  ],
  fundamental_data: [
    "COMPANY_OVERVIEW",
    "INCOME_STATEMENT",
    "BALANCE_SHEET",
    "CASH_FLOW",
    "EARNINGS",
    "LISTING_STATUS",
    "EARNINGS_CALENDAR",
    "IPO_CALENDAR",
  ],
  forex: ["FX_INTRADAY", "FX_DAILY", "FX_WEEKLY", "FX_MONTHLY"],
  cryptocurrencies: [
    "CURRENCY_EXCHANGE_RATE",
    "DIGITAL_CURRENCY_INTRADAY",
    "DIGITAL_CURRENCY_DAILY",
    "DIGITAL_CURRENCY_WEEKLY",
    "DIGITAL_CURRENCY_MONTHLY",
  ],
  commodities: [
    "WTI",
    "BRENT",
    "NATURAL_GAS",
    "COPPER",
    "ALUMINUM",
    "WHEAT",
    "CORN",
    "COTTON",
    "SUGAR",
    "COFFEE",
    "GOLD_SILVER_SPOT",
    "GOLD_SILVER_HISTORY",
    "ALL_COMMODITIES",
  ],
  economic_indicators: [
    "REAL_GDP",
    "REAL_GDP_PER_CAPITA",
    "TREASURY_YIELD",
    "FEDERAL_FUNDS_RATE",
    "CPI",
    "INFLATION",
    "RETAIL_SALES",
    "DURABLES",
    "UNEMPLOYMENT",
    "NONFARM_PAYROLL",
  ],
  technical_indicators: [
    "SMA",
    "EMA",
    "WMA",
    "DEMA",
    "TEMA",
    "TRIMA",
    "KAMA",
    "MAMA",
    "VWAP",
    "T3",
    "MACD",
    "MACDEXT",
    "STOCH",
    "STOCHF",
    "RSI",
    "STOCHRSI",
    "WILLR",
    "ADX",
    "ADXR",
    "APO",
    "PPO",
    "MOM",
    "BOP",
    "CCI",
    "CMO",
    "ROC",
    "ROCR",
    "AROON",
    "AROONOSC",
    "MFI",
    "TRIX",
    "ULTOSC",
    "DX",
    "MINUS_DI",
    "PLUS_DI",
    "MINUS_DM",
    "PLUS_DM",
    "BBANDS",
    "MIDPOINT",
    "MIDPRICE",
    "SAR",
    "TRANGE",
    "ATR",
    "NATR",
    "AD",
    "ADOSC",
    "OBV",
    "HT_TRENDLINE",
    "HT_SINE",
    "HT_TRENDMODE",
    "HT_DCPERIOD",
    "HT_DCPHASE",
    "HT_PHASOR",
  ],
  ping: ["PING", "ADD_TWO_NUMBERS"],
};

const ALL_AV_FUNCTIONS = Object.values(AV_FUNCTION_CATALOG).flat();

function normalizeFnName(name) {
  return String(name || "").trim().toUpperCase();
}

function isKnownAvFunction(name) {
  return ALL_AV_FUNCTIONS.includes(normalizeFnName(name));
}

function getAvCategory(fnName) {
  const fn = normalizeFnName(fnName);
  for (const [category, fns] of Object.entries(AV_FUNCTION_CATALOG)) {
    if (fns.includes(fn)) return category;
  }
  return "unknown";
}

// =======================
// SSE helpers
// =======================
function sseSend(res, eventName, dataObj) {
  res.write(`event: ${eventName}\n`);
  res.write(`data: ${JSON.stringify(dataObj)}\n\n`);
}

function sendJsonRpc(sessionId, payload) {
  const sess = sessions.get(sessionId);
  if (!sess) return false;
  sseSend(sess.res, "message", payload);
  return true;
}

// =======================
// Routes
// =======================

// 1) SSE "listening line"
app.get("/sse", (req, res) => {
  if (!requireAuth(req, res)) return;

  res.status(200);
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no"); // helps behind some proxies
  res.flushHeaders?.();

  const sessionId = crypto.randomUUID();
  sessions.set(sessionId, { res, createdAt: Date.now() });

  // Tell client where to POST messages for this session
  const messageUrl = `/messages?sessionId=${encodeURIComponent(sessionId)}`;
  sseSend(res, "endpoint", { endpoint: messageUrl });

  // Keep alive pings
  const ping = setInterval(() => {
    sseSend(res, "ping", {});
  }, 15000);

  req.on("close", () => {
    clearInterval(ping);
    sessions.delete(sessionId);
  });
});

// 2) Message inbox
app.post("/messages", (req, res) => {
  if (!requireAuth(req, res)) return;

  const sessionId = String(req.query.sessionId || "");
  if (!sessionId || !sessions.has(sessionId)) {
    return res.status(404).json({ error: "Unknown or missing sessionId" });
  }

  const msg = req.body;
  if (!msg || msg.jsonrpc !== "2.0" || typeof msg.method !== "string") {
    return res.status(400).json({ error: "Invalid JSON-RPC" });
  }

  // ACK quickly; actual reply goes over SSE
  res.status(202).json({ ok: true });

  handleJsonRpc(sessionId, msg).catch((err) => {
    if (msg.id !== undefined) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id: msg.id,
        error: {
          code: -32603,
          message: "Internal error",
          data: String(err?.message || err),
        },
      });
    }
  });
});

app.get("/health", (req, res) => res.status(200).send("ok"));

// =======================
// JSON-RPC / MCP-ish handlers
// =======================
async function handleJsonRpc(sessionId, msg) {
  const { id, method, params } = msg;

  // 1) Initialize
  if (method === "initialize") {
    if (id !== undefined) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion: "2024-11-05",
          serverInfo: { name: "legacy-sse-alpha-vantage-proxy", version: "1.0.0" },
          capabilities: { tools: {} },
        },
      });
    }
    return;
  }

  // 2) List tools (these are the MCP tools your server offers)
  if (method === "tools/list") {
    if (id !== undefined) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        result: {
          tools: [
            {
              name: "av_list_functions",
              description: "List Alpha Vantage function names grouped by category.",
              inputSchema: { type: "object", properties: {}, additionalProperties: false },
            },
            {
              name: "av_describe_function",
              description:
                "Describe an Alpha Vantage function (category + how to call it).",
              inputSchema: {
                type: "object",
                properties: { function: { type: "string" } },
                required: ["function"],
                additionalProperties: false,
              },
            },
            {
              name: "av_call_function",
              description:
                "Call an Alpha Vantage function and return the JSON response. Provide function name + query params.",
              inputSchema: {
                type: "object",
                properties: {
                  function: { type: "string", description: "Alpha Vantage function, e.g. GLOBAL_QUOTE" },
                  params: { type: "object", description: "Query parameters (symbol, interval, outputsize, etc.)" },
                },
                required: ["function"],
                additionalProperties: false,
              },
            },
            // Keep echo as a simple connectivity test
            {
              name: "echo",
              description: "Echo back the provided text (connectivity test).",
              inputSchema: {
                type: "object",
                properties: { text: { type: "string" } },
                required: ["text"],
                additionalProperties: false,
              },
            },
          ],
        },
      });
    }
    return;
  }

  // 3) Call tool
  if (method === "tools/call") {
    const toolName = String(params?.name || "");
    const args = params?.arguments || {};

    // --- echo (connectivity test)
    if (toolName === "echo") {
      const text = String(args.text ?? "");
      if (id !== undefined) {
        sendJsonRpc(sessionId, {
          jsonrpc: "2.0",
          id,
          result: { content: [{ type: "text", text }] },
        });
      }
      return;
    }

    // --- av_list_functions
    if (toolName === "av_list_functions") {
      if (id !== undefined) {
        sendJsonRpc(sessionId, {
          jsonrpc: "2.0",
          id,
          result: {
            content: [{ type: "text", text: JSON.stringify(AV_FUNCTION_CATALOG, null, 2) }],
          },
        });
      }
      return;
    }

    // --- av_describe_function
    if (toolName === "av_describe_function") {
      const fn = normalizeFnName(args.function);
      if (!isKnownAvFunction(fn)) {
        if (id !== undefined) {
          sendJsonRpc(sessionId, {
            jsonrpc: "2.0",
            id,
            error: { code: -32602, message: `Unknown Alpha Vantage function: ${fn}` },
          });
        }
        return;
      }

      const category = getAvCategory(fn);
      if (id !== undefined) {
        sendJsonRpc(sessionId, {
          jsonrpc: "2.0",
          id,
          result: {
            content: [
              {
                type: "text",
                text: JSON.stringify(
                  {
                    function: fn,
                    category,
                    usage: {
                      tool: "av_call_function",
                      example: {
                        function: fn,
                        params: { symbol: "AAPL" },
                      },
                      note:
                        "Put Alpha Vantage query parameters inside params (symbol, interval, outputsize, market, etc.).",
                    },
                  },
                  null,
                  2
                ),
              },
            ],
          },
        });
      }
      return;
    }

    // --- av_call_function
    if (toolName === "av_call_function") {
      const avKey = process.env.ALPHAVANTAGE_API_KEY || "";
      if (!avKey) {
        if (id !== undefined) {
          sendJsonRpc(sessionId, {
            jsonrpc: "2.0",
            id,
            error: { code: -32000, message: "Missing ALPHAVANTAGE_API_KEY on server" },
          });
        }
        return;
      }

      const fn = normalizeFnName(args.function);
      if (!isKnownAvFunction(fn)) {
        if (id !== undefined) {
          sendJsonRpc(sessionId, {
            jsonrpc: "2.0",
            id,
            error: { code: -32602, message: `Unknown Alpha Vantage function: ${fn}` },
          });
        }
        return;
      }

      const p = args.params;
      if (typeof p !== "object" || p === null || Array.isArray(p)) {
        if (id !== undefined) {
          sendJsonRpc(sessionId, {
            jsonrpc: "2.0",
            id,
            error: { code: -32602, message: "params must be an object" },
          });
        }
        return;
      }

      const url = new URL("https://www.alphavantage.co/query");
      url.searchParams.set("function", fn);
      url.searchParams.set("apikey", avKey);

      for (const [k, v] of Object.entries(p)) {
        if (v === undefined || v === null) continue;
        url.searchParams.set(k, String(v));
      }

      const resp = await fetch(url);
      const data = await resp.json();

      if (id !== undefined) {
        sendJsonRpc(sessionId, {
          jsonrpc: "2.0",
          id,
          result: { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] },
        });
      }
      return;
    }

    // Unknown tool
    if (id !== undefined) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        error: { code: -32601, message: `Unknown tool: ${toolName}` },
      });
    }
    return;
  }

  // Unknown method
  if (id !== undefined) {
    sendJsonRpc(sessionId, {
      jsonrpc: "2.0",
      id,
      error: { code: -32601, message: `Method not found: ${method}` },
    });
  }
}

// =======================
// Start server
// =======================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✅ Server running on port ${PORT}`);
  console.log(`   SSE endpoint:      /sse`);
  console.log(`   Messages endpoint: /messages`);
});
