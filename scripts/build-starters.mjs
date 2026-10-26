#!/usr/bin/env node
// Builds the starter decks from 17Lands' public PremierDraft game data (https://www.17lands.com/public_datasets, CC BY 4.0).
//
//   node scripts/build-starters.mjs [--set FRA] [--file game_data.csv.gz] [--today 2026-10-26]
//
// Picks the newest released set that 17Lands has data for, keeps the drafts that won 7 matches and lost at most 2
// (players in a good win-rate bucket), one deck per colour pair, and writes decks/<SET>/<id>.txt plus index.json.
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, readdir, rm, writeFile } from 'node:fs/promises';
import { createGunzip } from 'node:zlib';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createInterface } from 'node:readline';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const S3 = 'https://17lands-public.s3.amazonaws.com/analysis_data/game_data';
const MIN_WINS = 7;
const MAX_LOSSES = 2;
const MIN_WIN_RATE_BUCKET = 0.55; // the player's overall win rate, as 17Lands buckets it
const MIN_PLAYER_GAMES_BUCKET = 50; // ...over at least this many games, so the rate means something
const PER_PAIR = 3; // decks kept per colour pair, so each day offers some choice
const DECK_SIZE_MIN = 40;

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, all) => (v.startsWith('--') ? [...a, [v.slice(2), all[i + 1]]] : a), []));
const today = args.today ?? new Date().toISOString().slice(0, 10);

/** Splits one CSV line; quotes only matter for card names with a comma ("deck_Arni, Humble Scribe"). */
function splitCsv(line) {
  if (!line.includes('"')) return line.split(',');
  const out = [];
  let cur = '';
  let quoted = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (quoted) {
      if (c === '"' && line[i + 1] === '"') (cur += '"'), i++;
      else if (c === '"') quoted = false;
      else cur += c;
    } else if (c === '"') quoted = true;
    else if (c === ',') (out.push(cur), (cur = ''));
    else cur += c;
  }
  out.push(cur);
  return out;
}

/** mulberry32 seeded by the date: the same day gives the same picks, whoever runs it. */
function seededRandom(text) {
  let h = 1779033703 ^ text.length;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 3432918353), (h = (h << 13) | (h >>> 19));
  let a = h >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const SCRYFALL_HEADERS = { 'user-agent': 'tabletop-starters/1.0', accept: 'application/json' }; // Scryfall refuses calls without them

async function newestSet() {
  const res = await fetch('https://api.scryfall.com/sets', { headers: SCRYFALL_HEADERS });
  if (!res.ok) throw new Error(`Scryfall sets: ${res.status}`);
  const sets = (await res.json()).data
    .filter((s) => ['expansion', 'core'].includes(s.set_type) && s.released_at <= today)
    .sort((a, b) => b.released_at.localeCompare(a.released_at));
  for (const s of sets.slice(0, 12)) {
    const res = await fetch(`${S3}/game_data_public.${s.code.toUpperCase()}.PremierDraft.csv.gz`, { method: 'HEAD' });
    if (res.ok) return { code: s.code.toUpperCase(), name: s.name };
  }
  throw new Error('No recent set has 17Lands data');
}

async function download(code) {
  const path = join(process.env.TMPDIR ?? '/tmp', `game_data_${code}.csv.gz`);
  const res = await fetch(`${S3}/game_data_public.${code}.PremierDraft.csv.gz`);
  if (!res.ok) throw new Error(`17Lands ${code}: ${res.status}`);
  await pipeline(Readable.fromWeb(res.body), createWriteStream(path));
  return path;
}

/** Streams the games and keeps, per draft, its deck and its match record. */
async function readDrafts(file) {
  const lines = createInterface({ input: createReadStream(file).pipe(createGunzip()), crlfDelay: Infinity });
  let col = null;
  const drafts = new Map();
  for await (const line of lines) {
    if (!col) {
      const head = splitCsv(line);
      const index = (name) => head.indexOf(name);
      col = {
        draft: index('draft_id'), match: index('match_number'), game: index('game_number'), won: index('won'),
        colors: index('main_colors'), bucket: index('user_game_win_rate_bucket'), games: index('user_n_games_bucket'),
        deck: head.flatMap((h, i) => (h.startsWith('deck_') ? [[i, h.slice(5)]] : [])),
      };
      continue;
    }
    const row = splitCsv(line);
    const id = row[col.draft];
    let d = drafts.get(id);
    if (!d) {
      const cards = [];
      for (const [i, name] of col.deck) {
        const n = Number(row[i]);
        if (n > 0) cards.push([name, n]);
      }
      d = { id, cards, colors: row[col.colors], bucket: Number(row[col.bucket]), games: Number(row[col.games]), matches: new Map() };
      drafts.set(id, d);
    }
    const m = Number(row[col.match]);
    const m1 = d.matches.get(m) ?? { won: 0, lost: 0 };
    row[col.won] === 'True' ? m1.won++ : m1.lost++;
    d.matches.set(m, m1);
  }
  return [...drafts.values()].map((d) => {
    const results = [...d.matches.values()];
    return { ...d, wins: results.filter((r) => r.won > r.lost).length, losses: results.filter((r) => r.lost >= r.won).length };
  });
}

/** Names Scryfall doesn't know (a 17Lands spelling, an Arena-only card): such a deck can't be loaded, so it's dropped. */
async function unknownNames(names) {
  const bad = new Set();
  for (let i = 0; i < names.length; i += 75) {
    const res = await fetch('https://api.scryfall.com/cards/collection', {
      method: 'POST',
      headers: { ...SCRYFALL_HEADERS, 'content-type': 'application/json' },
      body: JSON.stringify({ identifiers: names.slice(i, i + 75).map((name) => ({ name })) }),
    });
    if (!res.ok) throw new Error(`Scryfall: ${res.status}`);
    for (const nf of (await res.json()).not_found) bad.add(nf.name);
  }
  return bad;
}

const LANDS = ['Plains', 'Island', 'Swamp', 'Mountain', 'Forest'];
const deckText = (name, cards) =>
  `About\nName ${name}\n\nDeck\n${[...cards].sort((a, b) => (LANDS.includes(a[0]) - LANDS.includes(b[0])) || a[0].localeCompare(b[0])).map(([c, n]) => `${n} ${c}`).join('\n')}\n`;

async function main() {
  const set = args.set ? { code: args.set.toUpperCase(), name: args.set.toUpperCase() } : await newestSet();
  console.error(`Set: ${set.code} (${set.name})`);
  const file = args.file ?? (await download(set.code));
  const drafts = await readDrafts(file);
  console.error(`${drafts.length} drafts`);

  const good = drafts.filter((d) =>
    d.wins >= MIN_WINS && d.losses <= MAX_LOSSES && d.bucket >= MIN_WIN_RATE_BUCKET && d.games >= MIN_PLAYER_GAMES_BUCKET &&
    d.cards.reduce((n, [, k]) => n + k, 0) >= DECK_SIZE_MIN && d.colors.length === 2); // two colours: the balanced, easy-to-play decks
  console.error(`${good.length} qualify`);

  const rand = seededRandom(`${today}${set.code}`);
  const byPair = new Map();
  for (const d of good) byPair.set(d.colors, [...(byPair.get(d.colors) ?? []), d]);

  const picked = [];
  for (const [pair, list] of [...byPair].sort()) {
    const shuffled = list.map((d) => [rand(), d]).sort((a, b) => a[0] - b[0]).map(([, d]) => d);
    const keep = [];
    for (const d of shuffled) {
      if (keep.length >= PER_PAIR) break;
      const bad = await unknownNames(d.cards.map(([c]) => c));
      if (bad.size) console.error(`skip ${d.id}: unknown ${[...bad].join(', ')}`);
      else keep.push(d);
    }
    picked.push(...keep);
  }

  const dir = join(ROOT, 'decks', set.code);
  await rm(dir, { recursive: true, force: true });
  await mkdir(dir, { recursive: true });
  const index = [];
  for (const d of picked) {
    const id = d.id.slice(0, 8);
    const name = `${set.code} ${d.colors} ${d.wins}-${d.losses}`;
    await writeFile(join(dir, `${id}.txt`), deckText(name, d.cards));
    index.push({ id, name, colors: d.colors, record: `${d.wins}-${d.losses}`, file: `decks/${set.code}/${id}.txt` });
  }
  await writeFile(join(ROOT, 'index.json'), JSON.stringify({ updated: today, set: set.code, setName: set.name, decks: index }, null, 1) + '\n');
  for (const old of await readdir(join(ROOT, 'decks'))) if (old !== set.code) await rm(join(ROOT, 'decks', old), { recursive: true });
  console.error(`Wrote ${index.length} decks`);
}

main().catch((e) => (console.error(e), process.exit(1)));
