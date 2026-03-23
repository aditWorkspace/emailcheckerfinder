const express = require('express');
const axios = require('axios');
const path = require('path');
const db = require('./db');

const app = express();
app.use(express.json());
app.use(express.static('public'));

const API_KEY = process.env.BULK_EMAIL_KEY || 't8ZPpGnFeVrm7owcM9b4YELgQRN2d5aA';
const API_URL = 'https://api.bulkemailchecker.com/real-time/';

function generateEmails(fullName) {
  const parts = fullName
    .trim()
    .toLowerCase()
    .replace(/[^a-z\s]/g, '')
    .split(/\s+/)
    .filter(Boolean);

  if (parts.length < 2) return { emails: [], firstName: '', lastName: '', parts };

  const firstName = parts[0];
  const lastName = parts[parts.length - 1];
  const fi = firstName[0];

  let emails;

  if (parts.length >= 3) {
    const middle = parts.slice(1, -1).join('');
    const mi = parts[1][0];
    emails = [
      { email: `${firstName}${lastName}@gmail.com`, pattern: 'firstlast' },
      { email: `${firstName}.${lastName}@gmail.com`, pattern: 'first.last' },
      { email: `${firstName}${middle}${lastName}@gmail.com`, pattern: 'firstmiddlelast' },
      { email: `${firstName}.${middle}.${lastName}@gmail.com`, pattern: 'first.middle.last' },
      { email: `${fi}${mi}${lastName}@gmail.com`, pattern: 'fmlast' },
    ];
  } else {
    emails = [
      { email: `${firstName}${lastName}@gmail.com`, pattern: 'firstlast' },
      { email: `${firstName}.${lastName}@gmail.com`, pattern: 'first.last' },
      { email: `${fi}${lastName}@gmail.com`, pattern: 'flast' },
      { email: `${firstName}_${lastName}@gmail.com`, pattern: 'first_last' },
      { email: `${lastName}${firstName}@gmail.com`, pattern: 'lastfirst' },
    ];
  }

  return { emails, firstName, lastName, parts };
}

async function checkEmail(email) {
  try {
    const { data } = await axios.get(API_URL, {
      params: { key: API_KEY, email },
      timeout: 15000,
    });

    let status = 'unknown';
    let raw = typeof data === 'object' ? JSON.stringify(data) : String(data);

    if (typeof data === 'object' && data !== null) {
      const s = (data.status || '').toLowerCase();
      if (s === 'passed') status = 'valid';
      else if (s === 'failed') status = 'invalid';
      else if (s === 'unknown' || s === 'inconclusive') status = 'risky';
    }

    return { status, raw: String(raw).substring(0, 200) };
  } catch (err) {
    const detail = err.response
      ? `${err.response.status} - ${JSON.stringify(err.response.data).substring(0, 150)}`
      : err.message;
    console.error(`[checkEmail] ${email}: ${detail}`);
    return { status: 'error', raw: detail };
  }
}

// Create a new run
app.post('/api/runs', async (req, res) => {
  const { sender } = req.body;
  if (!sender) return res.status(400).json({ error: 'Sender is required' });
  const runId = await db.createRun(sender);
  res.json({ runId });
});

// Check one name within a run
app.post('/api/runs/:runId/check-name', async (req, res) => {
  const runId = Number(req.params.runId);
  const { name } = req.body;
  if (!name || !name.trim()) return res.json({ results: [], error: 'Empty name' });

  const normalized = db.normalizeName(name);

  // Global dedupe across all runs
  const exists = await db.nameExists(normalized);
  if (exists) {
    return res.json({ results: [], skipped: true, reason: 'Already processed in a previous run' });
  }

  const { emails, firstName, lastName, parts } = generateEmails(name);
  if (emails.length === 0) {
    return res.json({ results: [], error: 'Need at least first and last name' });
  }

  // Save name to this run
  const displayName = name.trim().replace(/\s+/g, ' ');
  const nameId = await db.insertSubmittedName(runId, displayName, normalized);

  const results = [];
  for (const { email, pattern } of emails) {
    const { status, raw } = await checkEmail(email);
    const r = { email, pattern, status, raw, firstName, lastName };
    results.push(r);
    await db.insertResult(runId, nameId, r);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }

  res.json({ results, firstName, lastName, namePartsCount: parts.length });
});

// Finalize run with counts
app.patch('/api/runs/:runId/finalize', async (req, res) => {
  const runId = Number(req.params.runId);
  await db.finalizeRun(runId, req.body);
  res.json({ ok: true });
});

// Update sender for a run
app.patch('/api/runs/:runId/sender', async (req, res) => {
  const runId = Number(req.params.runId);
  const { sender } = req.body;
  await db.updateRunSender(runId, sender);
  res.json({ ok: true });
});

// Get full history
app.get('/api/history', async (_req, res) => {
  const history = await db.getHistory();
  res.json(history);
});

async function start() {
  try {
    await db.migrate();
  } catch (err) {
    console.error('DB migration failed (will still start server):', err.message);
  }
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`Running at http://localhost:${PORT}`));
}

start();

module.exports = app;
