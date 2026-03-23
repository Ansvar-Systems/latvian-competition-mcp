#!/usr/bin/env npx tsx
/**
 * Ingestion crawler for Konkurences padome (KP) — Competition Council of Latvia.
 *
 * Crawls lemumi.kp.gov.lv/decisions for enforcement decisions and merger rulings,
 * parses the listing pages and linked PDF content, then inserts into the SQLite DB.
 *
 * Usage:
 *   npx tsx scripts/ingest-kp.ts                  # full crawl
 *   npx tsx scripts/ingest-kp.ts --resume          # skip already-ingested case numbers
 *   npx tsx scripts/ingest-kp.ts --dry-run          # parse only, do not write to DB
 *   npx tsx scripts/ingest-kp.ts --force             # drop existing data and re-ingest
 *   npx tsx scripts/ingest-kp.ts --max-pages 5       # limit to N listing pages (testing)
 *
 * Env:
 *   CC_LV_DB_PATH — SQLite database path (default: data/cc-lv.db)
 *
 * Dependencies: better-sqlite3, cheerio (must be installed separately).
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, unlinkSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const BASE_URL = "https://lemumi.kp.gov.lv";
const DECISIONS_URL = `${BASE_URL}/decisions`;
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3000;
const REQUEST_TIMEOUT_MS = 30_000;
const PDF_TIMEOUT_MS = 60_000;
const USER_AGENT =
  "AnsvarKPCrawler/1.0 (+https://ansvar.eu; compliance research)";

const DB_PATH = process.env["CC_LV_DB_PATH"] ?? "data/cc-lv.db";
const STATE_PATH = join(dirname(DB_PATH), "ingest-kp-state.json");

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");

function getFlagValue(flag: string): string | undefined {
  const idx = args.indexOf(flag);
  return idx !== -1 && idx + 1 < args.length ? args[idx + 1] : undefined;
}

const MAX_PAGES = getFlagValue("--max-pages")
  ? parseInt(getFlagValue("--max-pages")!, 10)
  : Infinity;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Raw listing entry scraped from the decisions table. */
interface ListingEntry {
  /** Decision number shown in the table, e.g. "43" or "E02-1". */
  nr: string;
  /** Status text, e.g. "Spēkā", "Tiesvedībā", "Atcelts". */
  status: string;
  /** Decision title in Latvian. */
  title: string;
  /** Adoption date string as shown, e.g. "16.03.2026". */
  adoptedRaw: string;
  /** Publication date string, e.g. "20.03.2026. (43.)". */
  publishedRaw: string;
  /** Absolute URL to the PDF document, or null if missing. */
  pdfUrl: string | null;
}

/** Parsed decision ready for DB insertion. */
interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string;
  sector: string | null;
  parties: string | null;
  summary: string;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

/** Parsed merger ready for DB insertion. */
interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

/** Crawler progress state for --resume. */
interface CrawlState {
  lastPage: number;
  ingestedCaseNumbers: string[];
  startedAt: string;
  updatedAt: string;
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
}

function warn(msg: string): void {
  const ts = new Date().toISOString();
  console.warn(`[${ts}] WARN: ${msg}`);
}

function error(msg: string): void {
  const ts = new Date().toISOString();
  console.error(`[${ts}] ERROR: ${msg}`);
}

// ---------------------------------------------------------------------------
// State persistence (for --resume)
// ---------------------------------------------------------------------------

function loadState(): CrawlState {
  if (existsSync(STATE_PATH)) {
    try {
      return JSON.parse(readFileSync(STATE_PATH, "utf-8")) as CrawlState;
    } catch {
      warn(`Could not parse state file at ${STATE_PATH}, starting fresh`);
    }
  }
  return {
    lastPage: 0,
    ingestedCaseNumbers: [],
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

function saveState(state: CrawlState): void {
  state.updatedAt = new Date().toISOString();
  const dir = dirname(STATE_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(STATE_PATH, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// HTTP helpers with retry
// ---------------------------------------------------------------------------

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(
  url: string,
  opts: { timeout?: number; retries?: number } = {},
): Promise<Response> {
  const timeout = opts.timeout ?? REQUEST_TIMEOUT_MS;
  const retries = opts.retries ?? MAX_RETRIES;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeout);

      const resp = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "lv,en;q=0.5",
        },
        signal: controller.signal,
      });

      clearTimeout(timer);

      if (resp.status === 429) {
        const retryAfter = parseInt(resp.headers.get("Retry-After") ?? "10", 10);
        warn(`Rate limited (429) on ${url}, waiting ${retryAfter}s`);
        await sleep(retryAfter * 1000);
        continue;
      }

      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} for ${url}`);
      }

      return resp;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (attempt < retries) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        warn(`Attempt ${attempt}/${retries} failed for ${url}: ${msg}. Retrying in ${backoff}ms`);
        await sleep(backoff);
      } else {
        throw new Error(`All ${retries} attempts failed for ${url}: ${msg}`);
      }
    }
  }

  throw new Error(`Unreachable: fetchWithRetry exhausted for ${url}`);
}

async function fetchHtml(url: string): Promise<string> {
  const resp = await fetchWithRetry(url);
  return resp.text();
}

async function fetchPdfText(url: string): Promise<string | null> {
  try {
    const resp = await fetchWithRetry(url, { timeout: PDF_TIMEOUT_MS });
    const buf = Buffer.from(await resp.arrayBuffer());

    // Basic PDF text extraction without external libraries.
    // PDFs from kp.gov.lv are typically digitally generated (not scanned),
    // so stream text extraction works for most documents.
    return extractTextFromPdf(buf);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    warn(`Failed to fetch PDF at ${url}: ${msg}`);
    return null;
  }
}

/**
 * Minimal PDF text extraction.
 *
 * Extracts text from PDF stream objects by finding BT/ET blocks and
 * decoding Tj/TJ operators. This works for digitally generated PDFs
 * (the majority of KP decisions). Scanned PDFs will return empty text.
 */
function extractTextFromPdf(buf: Buffer): string {
  const raw = buf.toString("latin1");
  const textChunks: string[] = [];

  // Decompress deflated streams and extract text operators.
  // For simple extraction, scan for text between parentheses in Tj/TJ ops.
  const tjPattern = /\(([^)]*)\)\s*Tj/g;
  let match: RegExpExecArray | null;
  while ((match = tjPattern.exec(raw)) !== null) {
    textChunks.push(decodePdfString(match[1]));
  }

  // TJ arrays: [(text) kerning (text) ...]
  const tjArrayPattern = /\[([^\]]*)\]\s*TJ/g;
  while ((match = tjArrayPattern.exec(raw)) !== null) {
    const inner = match[1];
    const partPattern = /\(([^)]*)\)/g;
    let part: RegExpExecArray | null;
    while ((part = partPattern.exec(inner)) !== null) {
      textChunks.push(decodePdfString(part[1]));
    }
  }

  const text = textChunks.join(" ").replace(/\s+/g, " ").trim();
  return text;
}

/** Decode common PDF escape sequences. */
function decodePdfString(s: string): string {
  return s
    .replace(/\\n/g, "\n")
    .replace(/\\r/g, "\r")
    .replace(/\\t/g, "\t")
    .replace(/\\\(/g, "(")
    .replace(/\\\)/g, ")")
    .replace(/\\\\/g, "\\");
}

// ---------------------------------------------------------------------------
// Listing page parser
// ---------------------------------------------------------------------------

function parseListingPage(html: string): ListingEntry[] {
  const $ = cheerio.load(html);
  const entries: ListingEntry[] = [];

  // The decisions table has rows with 5 cells: Nr, Status, Title (linked), Adopted, Published.
  $("table tr").each((_i, row) => {
    const cells = $(row).find("td");
    if (cells.length < 5) return; // skip header or malformed rows

    const nr = cells.eq(0).text().trim().replace(/\.$/, "");
    const status = cells.eq(1).text().trim();
    const titleCell = cells.eq(2);
    const title = titleCell.text().trim();
    const link = titleCell.find("a").attr("href") ?? null;
    const adoptedRaw = cells.eq(3).text().trim().replace(/\.$/, "");
    const publishedRaw = cells.eq(4).text().trim();

    if (!nr || !title) return;

    let pdfUrl: string | null = null;
    if (link) {
      pdfUrl = link.startsWith("http") ? link : `${BASE_URL}${link.startsWith("/") ? "" : "/"}${link}`;
    }

    entries.push({ nr, status, title, adoptedRaw, publishedRaw, pdfUrl });
  });

  return entries;
}

/** Return total page count from pagination links. */
function parseTotalPages(html: string): number {
  const $ = cheerio.load(html);
  let maxPage = 1;

  // Pagination links are simple ?page=N anchors.
  $("a[href*='page=']").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const m = href.match(/page=(\d+)/);
    if (m) {
      const p = parseInt(m[1], 10);
      if (p > maxPage) maxPage = p;
    }
  });

  return maxPage;
}

// ---------------------------------------------------------------------------
// Decision classification
// ---------------------------------------------------------------------------

/** Map Latvian status text to a DB status value. */
function mapStatus(raw: string): string {
  const lower = raw.toLowerCase();
  if (lower.includes("galējā spēkā")) return "final";
  if (lower.includes("spēkā")) return "in_force";
  if (lower.includes("tiesvedībā")) return "in_litigation";
  if (lower.includes("atcelts")) return "repealed";
  if (lower.includes("administratīvā")) return "administrative_settlement";
  return "in_force";
}

/** Infer whether a listing entry is a merger or enforcement decision. */
function isMerger(entry: ListingEntry): boolean {
  const t = entry.title.toLowerCase();
  return (
    t.includes("apvienošan") || // apvienošanos / apvienošanās
    t.includes("koncentrācij") ||
    t.includes("iegūšan") || // ietekmes iegūšanu
    t.includes("izšķirošas ietekmes") ||
    t.includes("kopīgas kontroles") ||
    t.includes("iegūst kontroli") ||
    t.includes("merger") // edge case: English text
  );
}

/**
 * Classify the enforcement type from the title.
 * KP decisions fall into:
 *   - abuse_of_dominance  (dominējošā stāvokļa)
 *   - cartel              (aizliegta vienošanās, saskaņota rīcība, 11. pants)
 *   - unfair_trading       (negodīga konkurence, 18. pants)
 *   - sector_inquiry       (sektorālā izmeklēšana, tirgus uzraudzība)
 *   - merger_prohibited    (apvienošanās aizliegta)
 *   - state_aid            (valsts atbalsts)
 *   - other
 */
function classifyDecisionType(title: string): string {
  const t = title.toLowerCase();
  if (t.includes("dominējošā stāvokļa") || t.includes("dominējoš")) return "abuse_of_dominance";
  if (
    t.includes("aizliegta vienošan") ||
    t.includes("saskaņota rīcība") ||
    t.includes("11. pant") ||
    t.includes("karteļ")
  )
    return "cartel";
  if (t.includes("negodīga konkurence") || t.includes("18. pant") || t.includes("nct"))
    return "unfair_trading";
  if (t.includes("sektorāl") || t.includes("tirgus uzraudzīb")) return "sector_inquiry";
  if (t.includes("aizliegta") && t.includes("apvienošan")) return "merger_prohibited";
  if (t.includes("valsts atbalst")) return "state_aid";
  if (t.includes("13. pant") || t.includes("pārkāpum")) return "cartel";
  return "other";
}

/**
 * Try to extract the sector from the decision title using known Latvian keywords.
 */
function inferSector(title: string): string | null {
  const t = title.toLowerCase();
  const sectorMap: [string[], string][] = [
    [["enerģ", "elektro", "gāz", "degviel", "nafta"], "energy"],
    [["mazumtirdzniecīb", "veikals", "tirdzniecīb", "pārtikas"], "retail"],
    [["telekom", "mobil", "sakaru", "platjosl", "internets", "televīzij"], "telecommunications"],
    [["bank", "apdrošināš", "finanš", "kredīt", "maksājum"], "financial_services"],
    [["transport", "loģistik", "pārvadā", "dzelzceļ", "osta", "aviācij"], "transport"],
    [["farmaceit", "aptieka", "zāl", "medikament", "veselīb"], "pharmaceuticals"],
    [["būvniecīb", "celtniecīb", "nekustam"], "construction"],
    [["pārtik", "piena", "lauksaimniecīb", "gaļ", "graud"], "agriculture_food"],
    [["medij", "reklām", "izdevniecīb", "preses"], "media"],
    [["atkritum", "ūden", "kanalizācij", "vide"], "environment_utilities"],
    [["tehnoloģ", "it ", "programmatūr", "dator", "digit"], "technology"],
    [["auto", "automaš", "automob"], "automotive"],
    [["tūrism", "viesnīc", "ceļojum"], "tourism"],
  ];

  for (const [keywords, sector] of sectorMap) {
    if (keywords.some((kw) => t.includes(kw))) return sector;
  }
  return null;
}

/** Parse DD.MM.YYYY into ISO YYYY-MM-DD. */
function parseDate(raw: string): string | null {
  // Strip trailing period and parenthesised content.
  const cleaned = raw.replace(/\s*\(.*\)/, "").replace(/\.$/, "").trim();
  const m = cleaned.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[2]}-${m[1]}`;
}

/**
 * Build a case number from the listing entry.
 * KP uses numbering like "43", "E02-1", etc. per year.
 * We prefix with the year from the adopted date to create unique IDs: "KP/43/2026".
 */
function buildCaseNumber(entry: ListingEntry): string {
  const dateIso = parseDate(entry.adoptedRaw);
  const year = dateIso ? dateIso.substring(0, 4) : "unknown";
  return `KP/${entry.nr}/${year}`;
}

/**
 * Extract party names from a merger title.
 * KP merger titles typically follow patterns like:
 *   'Par "COMPANY A" SIA ... iegūšanu pār "COMPANY B" ...'
 *   'Par "COMPANY A" SIA un "COMPANY B" SIA apvienošanos'
 */
function extractMergerParties(title: string): {
  acquiring: string | null;
  target: string | null;
  parties: string[];
} {
  const quoted: string[] = [];
  const quotePattern = /[""„]([^""„"]+)[""„"]/g;
  let m: RegExpExecArray | null;
  while ((m = quotePattern.exec(title)) !== null) {
    quoted.push(m[1].trim());
  }

  if (quoted.length === 0) {
    return { acquiring: null, target: null, parties: [] };
  }

  if (quoted.length === 1) {
    return { acquiring: quoted[0], target: null, parties: quoted };
  }

  // First quoted name is typically the acquirer, rest are targets.
  return {
    acquiring: quoted[0],
    target: quoted.slice(1).join(", "),
    parties: quoted,
  };
}

/**
 * Extract the outcome from decision title and status.
 */
function inferOutcome(entry: ListingEntry): string {
  const t = entry.title.toLowerCase();

  // Mergers
  if (isMerger(entry)) {
    if (t.includes("aizliedz") || t.includes("aizliegta")) return "blocked";
    if (t.includes("nosacījum") || t.includes("saistošiem noteikumiem")) return "cleared_with_conditions";
    return "cleared_phase1";
  }

  // Enforcement
  if (t.includes("naudas sod") || t.includes("sodu")) return "fine";
  if (t.includes("izbeig") || t.includes("izbeidz")) return "closed";
  if (t.includes("pārkāpum") && !t.includes("nav konstatē")) return "infringement_found";
  if (t.includes("nav konstatē")) return "cleared";
  if (entry.status.toLowerCase().includes("atcelts")) return "repealed";

  return "decision_issued";
}

/**
 * Generate a summary from the title and available text.
 * Used when PDF text extraction is unavailable or yields no content.
 */
function generateFallbackSummary(title: string): string {
  return title;
}

/**
 * Extract a summary from the first meaningful paragraph of full text.
 */
function extractSummary(fullText: string, maxLen: number = 500): string {
  if (!fullText || fullText.length < 20) return "";

  // Take the first paragraph that looks like a summary (>50 chars, <maxLen).
  const paragraphs = fullText.split(/\n\n+/).filter((p) => p.trim().length > 50);
  if (paragraphs.length === 0) return fullText.substring(0, maxLen);

  const first = paragraphs[0].trim();
  return first.length > maxLen ? first.substring(0, maxLen) + "…" : first;
}

/**
 * Extract Latvian Competition Law article references from text.
 * KP decisions reference the Konkurences likums (Competition Law):
 *   - 11. pants (horizontal agreements)
 *   - 13. pants (abuse of dominance)
 *   - 15. pants (merger control)
 *   - 18. pants (unfair competition)
 *   - etc.
 */
function extractLawArticles(text: string): string | null {
  const articles = new Set<string>();
  // Match patterns like "11. panta", "13.panta", "Konkurences likuma 11."
  const patterns = [
    /(\d+)\.\s*pant/gi,
    /Konkurences\s+likuma\s+(\d+)\./gi,
    /KL\s+(\d+)\.\s*pant/gi,
  ];

  for (const pattern of patterns) {
    let m: RegExpExecArray | null;
    while ((m = pattern.exec(text)) !== null) {
      articles.add(m[1]);
    }
  }

  if (articles.size === 0) return null;
  return JSON.stringify(Array.from(articles).sort((a, b) => parseInt(a) - parseInt(b)));
}

/**
 * Try to extract a fine amount from text.
 * KP fines are in EUR, patterns like "EUR 2 100 000" or "2100000 euro".
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    /(?:EUR|eur|€)\s*([\d\s,.]+)/g,
    /([\d\s,.]+)\s*(?:EUR|eur|euro|eiro)/g,
    /naudas\s+sod[uā]\s+(?:EUR|eur|€)?\s*([\d\s,.]+)/gi,
  ];

  let maxFine = 0;
  for (const pattern of patterns) {
    let m: RegExpExecArray | null;
    while ((m = pattern.exec(text)) !== null) {
      const numStr = m[1].replace(/\s/g, "").replace(/,/g, ".");
      const num = parseFloat(numStr);
      if (!isNaN(num) && num > maxFine && num < 1_000_000_000) {
        maxFine = num;
      }
    }
  }

  return maxFine > 0 ? maxFine : null;
}

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`--force: deleted existing database at ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function getExistingCaseNumbers(db: Database.Database): Set<string> {
  const decisionNrs = (
    db.prepare("SELECT case_number FROM decisions").all() as { case_number: string }[]
  ).map((r) => r.case_number);
  const mergerNrs = (
    db.prepare("SELECT case_number FROM mergers").all() as { case_number: string }[]
  ).map((r) => r.case_number);
  return new Set([...decisionNrs, ...mergerNrs]);
}

// ---------------------------------------------------------------------------
// Sector table management
// ---------------------------------------------------------------------------

const SECTOR_DEFINITIONS: Record<string, { name: string; name_en: string; description: string }> = {
  energy: {
    name: "Enerģētika",
    name_en: "Energy",
    description: "Elektroenerģijas ražošana, pārvade un tirdzniecība, dabasgāze un degviela.",
  },
  retail: {
    name: "Mazumtirdzniecība",
    name_en: "Retail",
    description: "Pārtikas mazumtirdzniecība, tirdzniecības centri un e-komercija.",
  },
  telecommunications: {
    name: "Telekomunikācijas",
    name_en: "Telecommunications",
    description: "Mobilo tīklu pakalpojumi, platjoslas internets un televīzijas izplatīšana.",
  },
  financial_services: {
    name: "Finanšu pakalpojumi",
    name_en: "Financial services",
    description: "Komerciālās bankas, apdrošināšana un maksājumu pakalpojumi.",
  },
  transport: {
    name: "Transports",
    name_en: "Transport",
    description: "Kravas pārvadājumi, publiskais transports, loģistika un aviācija.",
  },
  pharmaceuticals: {
    name: "Farmaceitika",
    name_en: "Pharmaceuticals",
    description: "Zāļu ražošana, aptieku tīkli un veselības aprūpes pakalpojumi.",
  },
  construction: {
    name: "Būvniecība",
    name_en: "Construction",
    description: "Būvniecība, nekustamā īpašuma attīstība un celtniecības materiāli.",
  },
  agriculture_food: {
    name: "Lauksaimniecība un pārtika",
    name_en: "Agriculture & food",
    description: "Pārtikas pārstrāde, piena produkti, lauksaimniecības ražošana.",
  },
  media: {
    name: "Mediji",
    name_en: "Media",
    description: "Mediju izdevniecība, reklāma un satura izplatīšana.",
  },
  environment_utilities: {
    name: "Vide un komunālie pakalpojumi",
    name_en: "Environment & utilities",
    description: "Atkritumu apsaimniekošana, ūdensapgāde un komunālie pakalpojumi.",
  },
  technology: {
    name: "Tehnoloģijas",
    name_en: "Technology",
    description: "IT pakalpojumi, programmatūra un digitālie risinājumi.",
  },
  automotive: {
    name: "Automobiļu nozare",
    name_en: "Automotive",
    description: "Automobiļu tirdzniecība, serviss un rezerves daļas.",
  },
  tourism: {
    name: "Tūrisms",
    name_en: "Tourism",
    description: "Tūrisma pakalpojumi, viesnīcas un ceļojumu organizēšana.",
  },
};

function upsertSectors(db: Database.Database): void {
  const stmt = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, 0, 0)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name,
      name_en = excluded.name_en,
      description = excluded.description
  `);

  for (const [id, def] of Object.entries(SECTOR_DEFINITIONS)) {
    stmt.run(id, def.name, def.name_en, def.description);
  }
}

function updateSectorCounts(db: Database.Database): void {
  db.exec(`
    UPDATE sectors SET
      decision_count = (SELECT COUNT(*) FROM decisions WHERE decisions.sector = sectors.id),
      merger_count   = (SELECT COUNT(*) FROM mergers WHERE mergers.sector = sectors.id)
  `);
}

// ---------------------------------------------------------------------------
// Main crawler
// ---------------------------------------------------------------------------

async function crawlListingPages(
  startPage: number,
): Promise<ListingEntry[]> {
  // Fetch page 1 to determine total page count.
  log(`Fetching listing page 1 to determine total pages...`);
  const firstPageHtml = await fetchHtml(`${DECISIONS_URL}?page=1`);
  const totalPages = Math.min(parseTotalPages(firstPageHtml), MAX_PAGES);
  log(`Total listing pages: ${totalPages} (crawling from page ${startPage})`);

  const allEntries: ListingEntry[] = [];

  // Parse page 1 if within range.
  if (startPage <= 1) {
    const entries = parseListingPage(firstPageHtml);
    log(`  Page 1: ${entries.length} entries`);
    allEntries.push(...entries);
  }

  // Crawl remaining pages.
  const firstPage = Math.max(startPage, 2);
  for (let page = firstPage; page <= totalPages; page++) {
    await sleep(RATE_LIMIT_MS);

    try {
      const html = await fetchHtml(`${DECISIONS_URL}?page=${page}`);
      const entries = parseListingPage(html);
      log(`  Page ${page}/${totalPages}: ${entries.length} entries`);
      allEntries.push(...entries);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      error(`Failed to fetch page ${page}: ${msg}. Continuing...`);
    }
  }

  return allEntries;
}

async function processEntry(
  entry: ListingEntry,
  db: Database.Database | null,
  existingCaseNumbers: Set<string>,
  state: CrawlState,
  stats: { decisions: number; mergers: number; skipped: number; pdfFailed: number },
): Promise<void> {
  const caseNumber = buildCaseNumber(entry);

  // Skip if already ingested (--resume).
  if (FLAG_RESUME && existingCaseNumbers.has(caseNumber)) {
    stats.skipped++;
    return;
  }

  // Fetch PDF text.
  let fullText = "";
  if (entry.pdfUrl) {
    await sleep(RATE_LIMIT_MS);
    const pdfText = await fetchPdfText(entry.pdfUrl);
    if (pdfText && pdfText.length > 50) {
      fullText = pdfText;
    } else {
      stats.pdfFailed++;
      warn(`PDF extraction yielded insufficient text for ${caseNumber} (${entry.pdfUrl})`);
    }
  } else {
    warn(`No PDF link for ${caseNumber}`);
  }

  // Use title as fallback text if PDF extraction failed.
  if (!fullText) {
    fullText = entry.title;
  }

  const dateIso = parseDate(entry.adoptedRaw);
  const sector = inferSector(entry.title + " " + fullText);
  const outcome = inferOutcome(entry);
  const dbStatus = mapStatus(entry.status);

  if (isMerger(entry)) {
    const { acquiring, target } = extractMergerParties(entry.title);
    const summary = fullText.length > entry.title.length
      ? extractSummary(fullText)
      : generateFallbackSummary(entry.title);

    const merger: ParsedMerger = {
      case_number: caseNumber,
      title: entry.title,
      date: dateIso,
      sector,
      acquiring_party: acquiring,
      target,
      summary,
      full_text: fullText,
      outcome,
      turnover: null,
    };

    if (!FLAG_DRY_RUN && db) {
      try {
        db.prepare(`
          INSERT OR IGNORE INTO mergers
            (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          merger.case_number, merger.title, merger.date, merger.sector,
          merger.acquiring_party, merger.target, merger.summary, merger.full_text,
          merger.outcome, merger.turnover,
        );
        stats.mergers++;
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        warn(`DB insert failed for merger ${caseNumber}: ${msg}`);
      }
    } else {
      log(`  [dry-run] Would insert merger: ${caseNumber} — ${entry.title.substring(0, 80)}`);
      stats.mergers++;
    }
  } else {
    const summary = fullText.length > entry.title.length
      ? extractSummary(fullText)
      : generateFallbackSummary(entry.title);
    const lawArticles = extractLawArticles(entry.title + " " + fullText);
    const fineAmount = extractFineAmount(fullText);
    const decisionType = classifyDecisionType(entry.title + " " + fullText);

    const { parties } = extractMergerParties(entry.title);
    const partiesJson = parties.length > 0 ? JSON.stringify(parties) : null;

    const decision: ParsedDecision = {
      case_number: caseNumber,
      title: entry.title,
      date: dateIso,
      type: decisionType,
      sector,
      parties: partiesJson,
      summary,
      full_text: fullText,
      outcome,
      fine_amount: fineAmount,
      gwb_articles: lawArticles,
      status: dbStatus,
    };

    if (!FLAG_DRY_RUN && db) {
      try {
        db.prepare(`
          INSERT OR IGNORE INTO decisions
            (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          decision.case_number, decision.title, decision.date, decision.type,
          decision.sector, decision.parties, decision.summary, decision.full_text,
          decision.outcome, decision.fine_amount, decision.gwb_articles, decision.status,
        );
        stats.decisions++;
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        warn(`DB insert failed for decision ${caseNumber}: ${msg}`);
      }
    } else {
      log(`  [dry-run] Would insert decision: ${caseNumber} — ${entry.title.substring(0, 80)}`);
      stats.decisions++;
    }
  }

  // Track in state for resume.
  state.ingestedCaseNumbers.push(caseNumber);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("=== KP (Konkurences padome) Ingestion Crawler ===");
  log(`Flags: resume=${FLAG_RESUME}, dry-run=${FLAG_DRY_RUN}, force=${FLAG_FORCE}, max-pages=${MAX_PAGES === Infinity ? "all" : MAX_PAGES}`);
  log(`Database: ${DB_PATH}`);
  log(`Source: ${DECISIONS_URL}`);

  const db = FLAG_DRY_RUN ? null : initDb();
  const existingCaseNumbers = db ? getExistingCaseNumbers(db) : new Set<string>();
  const state = FLAG_RESUME ? loadState() : {
    lastPage: 0,
    ingestedCaseNumbers: [],
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  if (FLAG_RESUME && state.ingestedCaseNumbers.length > 0) {
    log(`Resuming from previous run: ${state.ingestedCaseNumbers.length} already ingested`);
    for (const cn of state.ingestedCaseNumbers) {
      existingCaseNumbers.add(cn);
    }
  }

  // Phase 1: Crawl all listing pages.
  log("\n--- Phase 1: Crawling listing pages ---");
  const startPage = FLAG_RESUME ? Math.max(state.lastPage, 1) : 1;
  const entries = await crawlListingPages(startPage);
  log(`Total entries collected: ${entries.length}`);

  if (entries.length === 0) {
    warn("No entries found. The page structure may have changed.");
    if (db) db.close();
    return;
  }

  // Phase 2: Insert sectors.
  if (db) {
    log("\n--- Phase 2: Upserting sector definitions ---");
    upsertSectors(db);
  }

  // Phase 3: Process each entry (fetch PDF, parse, insert).
  log("\n--- Phase 3: Processing entries ---");
  const stats = { decisions: 0, mergers: 0, skipped: 0, pdfFailed: 0 };

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]!;
    const progress = `[${i + 1}/${entries.length}]`;
    const caseNumber = buildCaseNumber(entry);

    if (FLAG_RESUME && existingCaseNumbers.has(caseNumber)) {
      stats.skipped++;
      continue;
    }

    log(`${progress} Processing ${caseNumber}: ${entry.title.substring(0, 70)}...`);

    await processEntry(entry, db, existingCaseNumbers, state, stats);

    // Save state periodically (every 25 entries).
    if (!FLAG_DRY_RUN && (i + 1) % 25 === 0) {
      state.lastPage = startPage + Math.floor(i / 10);
      saveState(state);
    }
  }

  // Phase 4: Update sector counts and final state.
  if (db) {
    log("\n--- Phase 4: Updating sector counts ---");
    updateSectorCounts(db);
  }

  if (!FLAG_DRY_RUN) {
    state.lastPage = MAX_PAGES === Infinity ? 999 : MAX_PAGES;
    saveState(state);
  }

  // Summary.
  const totalDb = db
    ? {
        decisions: (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt,
        mergers: (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt,
        sectors: (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt,
      }
    : null;

  if (db) db.close();

  log("\n=== Ingestion complete ===");
  log(`  New decisions inserted: ${stats.decisions}`);
  log(`  New mergers inserted:   ${stats.mergers}`);
  log(`  Skipped (existing):     ${stats.skipped}`);
  log(`  PDF extraction failed:  ${stats.pdfFailed}`);
  if (totalDb) {
    log(`\nDatabase totals:`);
    log(`  Decisions: ${totalDb.decisions}`);
    log(`  Mergers:   ${totalDb.mergers}`);
    log(`  Sectors:   ${totalDb.sectors}`);
  }
  log(`\nDatabase: ${DB_PATH}`);
  if (!FLAG_DRY_RUN) log(`State: ${STATE_PATH}`);
}

main().catch((err) => {
  error(`Fatal: ${err instanceof Error ? err.message : String(err)}`);
  if (err instanceof Error && err.stack) {
    console.error(err.stack);
  }
  process.exit(1);
});
