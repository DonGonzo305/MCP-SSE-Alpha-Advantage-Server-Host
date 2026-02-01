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

// Optional simple auth: set API_KEY to require it.
// If API_KEY is empty, auth is OFF.
const API_KEY = process.env.API_KEY || "";
function requireAuth(req, res) {
  if (!API_KEY) return true;
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

// Tool catalog based on Alpha Vantage MCP "Tools Reference" (Jan 2026 wrapper model)
const TOOL_CATALOG = {
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
    "SMA","EMA","WMA","DEMA","TEMA","TRIMA","KAMA","MAMA","VWAP","T3",
    "MACD","MACDEXT","STOCH","STOCHF","RSI","STOCHRSI","WILLR","ADX","ADXR",
    "APO","PPO","MOM","BOP","CCI","CMO","ROC","ROCR","AROON","AROONOSC","MFI",
    "TRIX","ULTOSC","DX","MINUS_DI","PLUS_DI","MINUS_DM","PLUS_DM","BBANDS",
    "MIDPOINT","MIDPRICE","SAR","TRANGE","ATR","NATR","AD","ADOSC","OBV",
    "HT_TRENDLINE","HT_SINE","HT_TRENDMODE","HT_DCPERIOD","HT_DCPHASE","HT_PHASOR",
  ],
  ping: ["PING", "ADD_TWO_NUMBERS"],
};

const ALL_FUNCTIONS = Object.values(TOOL_CATALOG).flat();

function isKnownFunction(name) {
  return ALL_FUNCTIONS.includes(String(name || "").toUpperCase());
}


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
  console.log("Sessions currently:", [...sessions.keys()]);
  console.log("Incoming sessionId:", req.query.sessionId);

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
        error: { code: -32603, message: "Internal error", data: String(err?.message || err) },
      });
    }
  });
});

// MCP-ish methods (minimal)
async function handleJsonRpc(sessionId, msg) {
  const { id, method, params } = msg;

  if (method === "initialize") {
    if (id !== undefined) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion: "2024-11-05",
          serverInfo: { name: "legacy-sse-mcp", version: "1.0.0" },
          capabilities: { tools: {} },
        },
      });
    }
    return;
  }

 if (method === "tools/list") {
  if (id !== undefined) {
    sendJsonRpc(sessionId, {
      jsonrpc: "2.0",
      id,
      result: {
        tools: [
          {
            name: "TOOL_LIST",
            description: "List available Alpha Vantage functions grouped by category.",
            inputSchema: { type: "object", properties: {}, additionalProperties: false },
          },
          {
            name: "TOOL_GET",
            description: "Get info about a specific Alpha Vantage function (name, category, and basic usage).",
            inputSchema: {
              type: "object",
              properties: { tool_name: { type: "string" } },
              required: ["tool_name"],
              additionalProperties: false,
            },
          },
          {
            name: "TOOL_CALL",
            description: "Call an Alpha Vantage function by name with arguments (passed through to the API).",
            inputSchema: {
              type: "object",
              properties: {
                tool_name: { type: "string" },
                arguments: { type: "object" },
              },
              required: ["tool_name"],
              additionalProperties: false,
            },
          },
        ],
      },
    });
  }
  return;
}


  if (method === "tools/call") {
  const toolName = String(params?.name || "");
  const args = params?.arguments || {};

  // Wrapper: TOOL_LIST
  if (toolName === "TOOL_LIST") {
    sendJsonRpc(sessionId, {
      jsonrpc: "2.0",
      id,
      result: { content: [{ type: "text", text: JSON.stringify(TOOL_CATALOG, null, 2) }] },
    });
    return;
  }

  // Wrapper: TOOL_GET
  if (toolName === "TOOL_GET") {
    const fn = String(args.tool_name || "").toUpperCase();
    if (!isKnownFunction(fn)) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        error: { code: -32602, message: `Unknown tool_name: ${fn}` },
      });
      return;
    }

    // Find category
    const category = Object.entries(TOOL_CATALOG).find(([, list]) => list.includes(fn))?.[0] || "unknown";

    sendJsonRpc(sessionId, {
      jsonrpc: "2.0",
      id,
      result: {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              {
                tool_name: fn,
                category,
                note:
                  "Use TOOL_CALL with tool_name set to this function name, and arguments containing the Alpha Vantage query params (e.g., symbol, interval, outputsize, etc.).",
              },
              null,
              2
            ),
          },
        ],
      },
    });
    return;
  }

  // Wrapper: TOOL_CALL
  if (toolName === "TOOL_CALL") {
    const avKey = process.env.ALPHAVANTAGE_API_KEY || "";
    if (!avKey) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        error: { code: -32000, message: "Missing ALPHAVANTAGE_API_KEY on server" },
      });
      return;
    }

    const fn = String(args.tool_name || "").toUpperCase();
    if (!isKnownFunction(fn)) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        error: { code: -32602, message: `Unknown tool_name: ${fn}` },
      });
      return;
    }

    const q = args.arguments || {};
    if (typeof q !== "object" || q === null || Array.isArray(q)) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        error: { code: -32602, message: "arguments must be an object" },
      });
      return;
    }

    // Build Alpha Vantage REST URL
    const url = new URL("https://www.alphavantage.co/query");
    url.searchParams.set("function", fn);
    url.searchParams.set("apikey", avKey);

    // Pass through all provided parameters
    for (const [k, v] of Object.entries(q)) {
      if (v === undefined || v === null) continue;
      url.searchParams.set(k, String(v));
    }

    const resp = await fetch(url);
    const data = await resp.json();

    sendJsonRpc(sessionId, {
      jsonrpc: "2.0",
      id,
      result: { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] },
    });
    return;
  }


  sendJsonRpc(sessionId, {
    jsonrpc: "2.0",
    id,
    error: { code: -32601, message: `Unknown tool: ${toolName}` },
  });
  return;
}

    if (id !== undefined) {
      sendJsonRpc(sessionId, {
        jsonrpc: "2.0",
        id,
        error: { code: -32601, message: `Unknown tool: ${toolName}` },
      });
    }
    return;
  }

  if (id !== undefined) {
    sendJsonRpc(sessionId, {
      jsonrpc: "2.0",
      id,
      error: { code: -32601, message: `Method not found: ${method}` },
    });
  }
}

app.get("/health", (req, res) => res.status(200).send("ok"));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✅ Server running at http://localhost:${PORT}`);
  console.log(`   SSE endpoint:      http://localhost:${PORT}/sse`);
  console.log(`   Messages endpoint: http://localhost:${PORT}/messages`);
});
