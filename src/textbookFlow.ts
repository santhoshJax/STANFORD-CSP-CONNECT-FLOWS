/**
 * Stanford CSP Migration – Textbook Import flow.
 *
 * Two-object upsert from a single TSV stream:
 *   1. Textbook__c          — deduplicated by ISBN (Title+Author slug fallback for
 *                             the ~0.8% of rows without ISBN)
 *   2. Textbook_Association__c — one per source row; upsert key = source ID
 *
 * Scope: Course_ID prefix must fall in the 2-year window
 *        (ANCHOR_QUARTER = "wi25", YEARS_BACK = 2).
 *        Row where ID = "1" is a junk row — excluded per workbook note.
 *        "Course Reader" / "No required textbook" entries excluded entirely
 *        (Amy + stakeholder confirmation, Jul 8 review).
 *
 * Author field: Strip "(Required)" or "(Recommended)" prefix before writing
 *   Author__c. Parse stripped prefix into Required_Optional__c on
 *   Textbook_Association__c: "Required" | "Optional" | (omitted).
 *   "Recommended" → "Optional" per Teresa's terminology confirmation.
 *
 * Textbook__c.External_ID_4D__c dedup key:
 *   Primary:  cleaned ISBN (strip hyphens, whitespace, trailing _4DNL_ — Jul 23 Julia)
 *   Fallback: "NOISBN_" + slug(Title) + "_" + slug(Author)
 *
 * UNMAPPED (purple — system audit fields):
 *   Created_Date, Created_Time, Created_By,
 *   Last_Modified_Date, Last_Modified_Time, Last_Modified_By
 * URL: confirmed not needed (Jul 10 stakeholder review).
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse } from "papaparse";
import { str, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// ── Constants ─────────────────────────────────────────────────────────────────

const SF_TEXTBOOK = "Textbook__c";
const SF_ASSOCIATION = "Textbook_Association__c";
const EXT_ID_FIELD = "External_ID_4D__c";
const BULK_BATCH_SIZE = 50_000;

const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 2;

function buildValidCourseIdPrefixes(anchor: string, yearsBack: number): Set<string> {
  const m = anchor.toLowerCase().match(/^([a-z]+)(\d+)$/);
  if (!m) throw new Error(`Invalid ANCHOR_QUARTER: "${anchor}"`);
  const anchorYear = 2000 + parseInt(m[2], 10);
  const set = new Set<string>();
  for (let y = anchorYear - yearsBack + 1; y <= anchorYear; y++) {
    for (let digit = 1; digit <= 4; digit++) {
      set.add(`${y}${digit}`);
    }
  }
  return set;
}

const VALID_COURSE_ID_PREFIXES = buildValidCourseIdPrefixes(ANCHOR_QUARTER, YEARS_BACK);

// Entries excluded from both Textbook__c and Textbook_Association__c per Amy (Jul 8).
const EXCLUDED_TITLE_PATTERNS = [
  /course\s+reader/i,
  /no\s+required\s+textbook/i,
  /no\s+textbook/i,
];

// ── Raw row type ──────────────────────────────────────────────────────────────

interface RawTextbookRow {
  ID?: string;
  Course_ID?: string;
  Title?: string;
  Author?: string;
  // Created_Date, Created_Time, Created_By        — Do Not Map
  // Last_Modified_Date, Last_Modified_Time, Last_Modified_By — Do Not Map
  // URL — confirmed not needed (Jul 10 review)
  ISBN?: string;
  [key: string]: string | undefined;
}

// ── Value helpers ─────────────────────────────────────────────────────────────

/**
 * Normalise ISBN for use as an external key (Jul 23, Julia):
 *   1. Strip trailing _4DNL_ token
 *   2. Strip ALL whitespace and hyphens (converts to bare digit string)
 */
function cleanIsbn(raw: string | undefined): string {
  return str(raw)
    .replace(/_4DNL_\s*$/, "")
    .replace(/[\s\-]/g, "");
}

/** Build the Textbook__c external ID: ISBN primary, Title+Author slug fallback. */
function buildTextbookExtId(isbn: string, title: string, author: string): string {
  if (isbn) return isbn;
  const slug = (s: string) =>
    s
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .slice(0, 50);
  return `NOISBN_${slug(title)}_${slug(author)}`;
}

/** Returns true for Course Reader / No-textbook entries. */
function isExcludedEntry(title: string): boolean {
  return EXCLUDED_TITLE_PATTERNS.some((re) => re.test(title));
}

/**
 * Parse the Author field:
 *   - Strip "(Required)" or "(Recommended)" prefix
 *   - Map prefix to Required_Optional__c picklist value
 *   - "Recommended" → "Optional" per Teresa's terminology confirmation
 */
function parseAuthor(raw: string | undefined): {
  author: string;
  requiredOptional: "Required" | "Optional" | undefined;
} {
  const s = str(raw).replace(/_4DNL_/g, " ").trim();
  const reqMatch = s.match(/^\(Required\)\s+/i);
  if (reqMatch) return { author: s.slice(reqMatch[0].length), requiredOptional: "Required" };
  const recMatch = s.match(/^\(Recommended\)\s+/i);
  if (recMatch) return { author: s.slice(recMatch[0].length), requiredOptional: "Optional" };
  return { author: s, requiredOptional: undefined };
}

// ── SF record types ───────────────────────────────────────────────────────────

type SfRecord = Record<string, unknown>;

// ── TSV streaming ─────────────────────────────────────────────────────────────

async function streamAndMap(
  fileId: string,
  accessToken: string,
  logger: { info: (m: string) => void; warn: (m: string) => void },
): Promise<{
  textbooks: SfRecord[];
  associations: SfRecord[];
  totalRows: number;
  filteredOut: number;
  excluded: number;
}> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const textbookMap = new Map<string, SfRecord>();
    const associations: SfRecord[] = [];
    let headers: string[] = [];
    let totalRows = 0;
    let filteredOut = 0;
    let excluded = 0;

    papaParse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      skipEmptyLines: true,

      step: (result: Papa.ParseResult<string[]>) => {
        const raw = result.data as unknown as string[];

        if (headers.length === 0) {
          headers = raw.map((h, i) =>
            h.replace(/^﻿/, "").replace(/\r/g, "").trim() || `__blank_${i}`,
          );
          return;
        }

        const row: RawTextbookRow = {};
        headers.forEach((h, i) => {
          if (!h.startsWith("__blank_")) {
            row[h] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        const id = str(row.ID).trim();
        if (!id || id === "1") return; // blank IDs and junk row 1 silently dropped
        totalRows++;

        // ── Scope filter: Course_ID prefix must be in 2yr window ─────────────
        const courseId = str(row.Course_ID).trim();
        const prefix = courseId.split("_")[0];
        if (!courseId || !VALID_COURSE_ID_PREFIXES.has(prefix)) {
          filteredOut++;
          return;
        }

        const title = str(row.Title).trim();

        // ── Exclude Course Reader / No-textbook entries entirely ──────────────
        if (isExcludedEntry(title)) {
          excluded++;
          return;
        }

        const isbn = cleanIsbn(row.ISBN);
        const { author, requiredOptional } = parseAuthor(row.Author);
        const tbExtId = buildTextbookExtId(isbn, title, author);

        // ── Textbook__c: add to dedup map on first encounter ─────────────────
        if (!textbookMap.has(tbExtId)) {
          const tb: SfRecord = {
            [EXT_ID_FIELD]: tbExtId,
            Name: title.slice(0, 80),
          };
          if (title) tb.Title__c = title;
          if (author) tb.Author__c = author;
          if (isbn) tb.ISBN__c = isbn;
          textbookMap.set(tbExtId, tb);
        }

        // ── Textbook_Association__c ──────────────────────────────────────────
        const assoc: SfRecord = {
          [EXT_ID_FIELD]: id,
          "Course_Offering__r.External_ID_4D__c": courseId,
          "Textbook__r.External_ID_4D__c": tbExtId,
        };
        if (requiredOptional !== undefined) {
          assoc.Required_Optional__c = requiredOptional;
        }
        associations.push(assoc);
      },

      complete: () => {
        logger.info(
          `[Textbook Import] Dedup: ${textbookMap.size} unique Textbook__c records from ${associations.length} associations`,
        );
        resolve({
          textbooks: [...textbookMap.values()],
          associations,
          totalRows,
          filteredOut,
          excluded,
        });
      },
      error: (err: Error) => reject(err),
    });
  });
}

// ── Bulk upsert helper ────────────────────────────────────────────────────────

async function upsertInBatches(
  records: SfRecord[],
  objectName: string,
  extIdField: string,
  sfBase: string,
  sfToken: string,
  logger: { info: (m: string) => void; warn: (m: string) => void; error: (m: string) => void },
  logPrefix: string,
): Promise<{ processed: number; failed: number; successfulCsv: string; failedCsv: string }> {
  let processed = 0;
  let failed = 0;
  let successfulCsv = "";
  let failedCsv = "";

  const batches: SfRecord[][] = [];
  for (let i = 0; i < records.length; i += BULK_BATCH_SIZE) {
    batches.push(records.slice(i, i + BULK_BATCH_SIZE));
  }

  for (let b = 0; b < batches.length; b++) {
    logger.info(`${logPrefix} Batch ${b + 1}/${batches.length} — ${batches[b].length} records`);
    const result = await runBulkJob(
      sfBase,
      sfToken,
      objectName,
      extIdField,
      batches[b] as Record<string, unknown>[],
      logger,
      `${logPrefix} Batch ${b + 1}`,
    );
    processed += result.numberRecordsProcessed;
    failed += result.numberRecordsFailed;
    if (result.successfulCsv)
      successfulCsv += (successfulCsv ? "\n" : "") + result.successfulCsv;
    if (result.failedCsv)
      failedCsv += (failedCsv ? "\n" : "") + result.failedCsv;
  }

  return { processed, failed, successfulCsv, failedCsv };
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const textbookImport = flow({
  name: "Textbook Import",
  stableKey: "tex00001-2345-4678-9abc-def012345678",
  description:
    "Streams the Textbook TSV from Google Drive, deduplicates books by ISBN, " +
    "and bulk-upserts Textbook__c then Textbook_Association__c records via Bulk API 2.0. " +
    "Scope: Course_ID prefix within the 2-year window (ANCHOR_QUARTER wi25).",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;
    logger.info("[Textbook Import] Starting…");

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars["Google Drive Connection"] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Textbook File ID"] as unknown as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as string | undefined;

    if (!fileId) throw new Error("Textbook File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── Stream + map ──────────────────────────────────────────────────────────
    logger.info("[Textbook Import] Streaming TSV from Google Drive…");
    const { textbooks, associations, totalRows, filteredOut, excluded } =
      await streamAndMap(fileId, gdToken, logger);

    logger.info(
      `[Textbook Import] Stream complete — ` +
        `total=${totalRows}, filtered(outside 2yr window)=${filteredOut}, ` +
        `excluded(Course Reader/No Textbook)=${excluded}, ` +
        `unique textbooks=${textbooks.length}, associations=${associations.length}`,
    );

    if (textbooks.length === 0 && associations.length === 0) {
      logger.info("[Textbook Import] No records to upsert — done.");
      return {
        data: { totalRows, filteredOut, excluded, textbooks: 0, associations: 0 },
      };
    }

    // ── Phase 1: Upsert Textbook__c ───────────────────────────────────────────
    let tbProcessed = 0;
    let tbFailed = 0;
    let tbSuccessfulCsv = "";
    let tbFailedCsv = "";

    if (textbooks.length > 0) {
      logger.info(`[Textbook Import] Upserting ${textbooks.length} Textbook__c record(s)…`);
      const tb = await upsertInBatches(
        textbooks,
        SF_TEXTBOOK,
        EXT_ID_FIELD,
        sfBase,
        sfToken,
        logger,
        "[Textbook Import][Textbook__c]",
      );
      tbProcessed = tb.processed;
      tbFailed = tb.failed;
      tbSuccessfulCsv = tb.successfulCsv;
      tbFailedCsv = tb.failedCsv;
    }

    // ── Phase 2: Upsert Textbook_Association__c ───────────────────────────────
    let assocProcessed = 0;
    let assocFailed = 0;
    let assocSuccessfulCsv = "";
    let assocFailedCsv = "";

    if (associations.length > 0) {
      logger.info(
        `[Textbook Import] Upserting ${associations.length} Textbook_Association__c record(s)…`,
      );
      const assoc = await upsertInBatches(
        associations,
        SF_ASSOCIATION,
        EXT_ID_FIELD,
        sfBase,
        sfToken,
        logger,
        "[Textbook Import][Textbook_Association__c]",
      );
      assocProcessed = assoc.processed;
      assocFailed = assoc.failed;
      assocSuccessfulCsv = assoc.successfulCsv;
      assocFailedCsv = assoc.failedCsv;
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Textbook Import",
        objects: [
          { objectName: SF_TEXTBOOK, successfulCsv: tbSuccessfulCsv, failedCsv: tbFailedCsv },
          { objectName: SF_ASSOCIATION, successfulCsv: assocSuccessfulCsv, failedCsv: assocFailedCsv },
        ],
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Textbook Import] Results sheet: ${sheet.url}`);
    } catch (err) {
      logger.warn(`[Textbook Import] Could not write results sheet: ${String(err)}`);
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Textbook Import] Complete —` +
        `\n  Source rows:               ${totalRows}` +
        `\n  Filtered (outside 2yr):    ${filteredOut}` +
        `\n  Excluded (Course Reader):  ${excluded}` +
        `\n  Textbook__c submitted:     ${textbooks.length}` +
        `\n  Textbook__c processed:     ${tbProcessed}` +
        `\n  Textbook__c failed:        ${tbFailed}` +
        `\n  Association submitted:     ${associations.length}` +
        `\n  Association processed:     ${assocProcessed}` +
        `\n  Association failed:        ${assocFailed}`,
    );

    return {
      data: {
        totalRows,
        filteredOut,
        excluded,
        textbooks: textbooks.length,
        textbooksProcessed: tbProcessed,
        textbooksFailed: tbFailed,
        associations: associations.length,
        associationsProcessed: assocProcessed,
        associationsFailed: assocFailed,
      },
    };
  },
});

export default [textbookImport];
