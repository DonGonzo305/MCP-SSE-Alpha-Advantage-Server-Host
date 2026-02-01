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
              name: "echo",
              description: "Echo back the provided text.",
              inputSchema: {
                type: "object",
                properties: { text: { type: "string" } },
                required: ["text"],
              },
            },
          ],
        },
      });
    }
    return;
  }

  if (method === "tools/call") {
    const toolName = params?.name;
    const args = params?.arguments || {};

    if (toolName === "echo") {
      const text = String(args.text ?? "");
      if (id !== undefined) {
        sendJsonRpc(sessionId, {
          jsonrpc: "2.0",
          id,
          result: {
            content: [{ type: "text", text }],
          },
        });
      }
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
