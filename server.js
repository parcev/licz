const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) { console.error("DATABASE_URL is not set"); process.exit(1); }

// Hosts without a dot (Render's internal URL, localhost) skip SSL; external hosts (Neon etc.) use it.
const host = new URL(DATABASE_URL).hostname;
const useSsl = host.includes(".") && host !== "127.0.0.1";
const pool = new Pool({ connectionString: DATABASE_URL, ssl: useSsl ? { rejectUnauthorized: false } : false });

const app = express();
app.use(cors({ origin: process.env.ALLOWED_ORIGIN || "*" }));
app.use(express.json({ limit: "1mb" }));

app.get("/health", (_req, res) => res.send("ok"));

// Optional shared access key: set API_KEY and enter it once in the web page.
app.use("/api", (req, res, next) => {
  const key = process.env.API_KEY;
  if (key && req.get("x-api-key") !== key) return res.status(401).json({ error: "unauthorized" });
  next();
});

const str = (s, max) => typeof s === "string" && s.length > 0 && s.length <= max;
const cleanName = s => (typeof s === "string" && s.trim() ? s.trim().slice(0, 80) : null);

// Every operation is idempotent, so a client can safely resend a batch it never got an answer for.
async function applyOp(c, o) {
  if (!o || typeof o !== "object") return;
  switch (o.type) {
    case "addTest":
      if (str(o.id, 40) && Number.isFinite(o.createdAt))
        await c.query("INSERT INTO ac_tests (id, name, created_at) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
          [o.id, cleanName(o.name) || "Untitled", Math.floor(o.createdAt)]);
      break;
    case "renameTest":
      if (str(o.id, 40) && cleanName(o.name))
        await c.query("UPDATE ac_tests SET name = $2 WHERE id = $1", [o.id, cleanName(o.name)]);
      break;
    case "deleteTest":
      if (str(o.id, 40)) await c.query("DELETE FROM ac_tests WHERE id = $1", [o.id]); // attempts cascade
      break;
    case "addAttempt":
      if (str(o.id, 40) && str(o.testId, 40) && typeof o.ok === "boolean" && Number.isFinite(o.t))
        await c.query(
          `INSERT INTO ac_attempts (id, test_id, ok, t)
           SELECT $1::text, $2::text, $3::boolean, $4::bigint WHERE EXISTS (SELECT 1 FROM ac_tests WHERE id = $2::text)
           ON CONFLICT (id) DO NOTHING`, [o.id, o.testId, o.ok, Math.floor(o.t)]);
      break;
    case "deleteAttempt":
      if (str(o.id, 40)) await c.query("DELETE FROM ac_attempts WHERE id = $1", [o.id]);
      break;
  }
}

// One endpoint: apply the client's queued operations, then return the full state
// (or just the revision number if nothing changed since the client's last sync).
app.post("/api/sync", async (req, res) => {
  const ops = Array.isArray(req.body.ops) ? req.body.ops.slice(0, 500) : [];
  const clientRev = Number(req.body.rev);
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    for (const o of ops) await applyOp(c, o);
    const q = ops.length ? "UPDATE ac_meta SET rev = rev + 1 RETURNING rev::int AS rev" : "SELECT rev::int AS rev FROM ac_meta";
    const rev = (await c.query(q)).rows[0].rev;
    await c.query("COMMIT");
    if (!ops.length && rev === clientRev) return res.json({ rev, state: null });
    const tests = (await pool.query('SELECT id, name, created_at::float8 AS "createdAt" FROM ac_tests ORDER BY created_at DESC')).rows;
    const attempts = (await pool.query('SELECT id, test_id AS "testId", ok, t::float8 AS t FROM ac_attempts')).rows;
    res.json({ rev, state: { rev, tests, attempts } });
  } catch (e) {
    await c.query("ROLLBACK").catch(() => {});
    console.error(e);
    res.status(500).json({ error: "server error" });
  } finally {
    c.release();
  }
});

async function init() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ac_tests (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at BIGINT NOT NULL);
    CREATE TABLE IF NOT EXISTS ac_attempts (
      id TEXT PRIMARY KEY, test_id TEXT NOT NULL REFERENCES ac_tests(id) ON DELETE CASCADE,
      ok BOOLEAN NOT NULL, t BIGINT NOT NULL);
    CREATE INDEX IF NOT EXISTS ac_attempts_test_idx ON ac_attempts (test_id);
    CREATE TABLE IF NOT EXISTS ac_meta (id INT PRIMARY KEY DEFAULT 1, rev BIGINT NOT NULL DEFAULT 0);
    INSERT INTO ac_meta (id) VALUES (1) ON CONFLICT (id) DO NOTHING;`);
}

init()
  .then(() => app.listen(process.env.PORT || 3000, () => console.log("Listening")))
  .catch(e => { console.error(e); process.exit(1); });
  