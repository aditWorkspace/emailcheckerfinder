const { neon } = require('@neondatabase/serverless');

let sql;

function getDb() {
  if (!sql) {
    const url = process.env.DATABASE_URL;
    if (!url) throw new Error('DATABASE_URL env var is required');
    sql = neon(url);
  }
  return sql;
}

async function migrate() {
  const sql = getDb();

  // Drop old schema if it exists
  await sql`DROP TABLE IF EXISTS email_results CASCADE`;
  await sql`DROP TABLE IF EXISTS names CASCADE`;
  await sql`DROP TABLE IF EXISTS submitted_names CASCADE`;
  await sql`DROP TABLE IF EXISTS finder_runs CASCADE`;

  await sql`
    CREATE TABLE IF NOT EXISTS finder_runs (
      id SERIAL PRIMARY KEY,
      sender TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      submitted_count INTEGER DEFAULT 0,
      checked_count INTEGER DEFAULT 0,
      valid_count INTEGER DEFAULT 0,
      invalid_count INTEGER DEFAULT 0,
      skipped_count INTEGER DEFAULT 0
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS submitted_names (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES finder_runs(id) ON DELETE CASCADE,
      display_name TEXT NOT NULL,
      normalized_name TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS email_results (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES finder_runs(id) ON DELETE CASCADE,
      submitted_name_id INTEGER NOT NULL REFERENCES submitted_names(id) ON DELETE CASCADE,
      email TEXT NOT NULL,
      pattern TEXT NOT NULL,
      status TEXT NOT NULL,
      raw_response TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `;

  // Index for fast global dedupe lookups
  await sql`CREATE INDEX IF NOT EXISTS idx_submitted_names_normalized ON submitted_names(normalized_name)`;

  console.log('DB migration complete');
}

function normalizeName(name) {
  return name.trim().replace(/\s+/g, ' ').toLowerCase();
}

// Global dedupe: check across ALL runs
async function nameExists(normalized) {
  const sql = getDb();
  const rows = await sql`
    SELECT id FROM submitted_names WHERE normalized_name = ${normalized} LIMIT 1
  `;
  return rows.length > 0;
}

async function createRun(sender) {
  const sql = getDb();
  const rows = await sql`
    INSERT INTO finder_runs (sender) VALUES (${sender}) RETURNING id
  `;
  return rows[0].id;
}

async function insertSubmittedName(runId, displayName, normalized) {
  const sql = getDb();
  const rows = await sql`
    INSERT INTO submitted_names (run_id, display_name, normalized_name)
    VALUES (${runId}, ${displayName}, ${normalized})
    RETURNING id
  `;
  return rows[0].id;
}

async function insertResult(runId, nameId, r) {
  const sql = getDb();
  await sql`
    INSERT INTO email_results (run_id, submitted_name_id, email, pattern, status, raw_response)
    VALUES (${runId}, ${nameId}, ${r.email}, ${r.pattern}, ${r.status}, ${r.raw})
  `;
}

async function finalizeRun(runId, counts) {
  const sql = getDb();
  await sql`
    UPDATE finder_runs
    SET submitted_count = ${counts.submitted},
        checked_count = ${counts.checked},
        valid_count = ${counts.valid},
        invalid_count = ${counts.invalid},
        skipped_count = ${counts.skipped}
    WHERE id = ${runId}
  `;
}

async function updateRunSender(runId, sender) {
  const sql = getDb();
  await sql`UPDATE finder_runs SET sender = ${sender} WHERE id = ${runId}`;
}

async function getHistory() {
  const sql = getDb();

  const runs = await sql`
    SELECT id, sender, created_at, submitted_count, checked_count,
           valid_count, invalid_count, skipped_count
    FROM finder_runs ORDER BY created_at DESC
  `;
  if (runs.length === 0) return [];

  const runIds = runs.map(r => r.id);

  const names = await sql`
    SELECT id, run_id, display_name
    FROM submitted_names WHERE run_id = ANY(${runIds}) ORDER BY id
  `;

  const results = await sql`
    SELECT run_id, submitted_name_id, email, pattern, status
    FROM email_results WHERE run_id = ANY(${runIds}) ORDER BY id
  `;

  // Group names by run
  const namesByRun = {};
  for (const n of names) {
    if (!namesByRun[n.run_id]) namesByRun[n.run_id] = [];
    namesByRun[n.run_id].push(n);
  }

  // Group results by name
  const resultsByName = {};
  for (const r of results) {
    if (!resultsByName[r.submitted_name_id]) resultsByName[r.submitted_name_id] = [];
    resultsByName[r.submitted_name_id].push(r);
  }

  return runs.map(run => ({
    id: run.id,
    sender: run.sender,
    createdAt: run.created_at,
    submittedCount: run.submitted_count,
    checkedCount: run.checked_count,
    validCount: run.valid_count,
    invalidCount: run.invalid_count,
    skippedCount: run.skipped_count,
    names: (namesByRun[run.id] || []).map(n => ({
      id: n.id,
      displayName: n.display_name,
      results: resultsByName[n.id] || [],
    })),
  }));
}

module.exports = {
  migrate, normalizeName, nameExists,
  createRun, insertSubmittedName, insertResult, finalizeRun,
  updateRunSender, getHistory,
};
