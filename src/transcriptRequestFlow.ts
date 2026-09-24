/**
 * Stanford CSP Migration – Transcript Request Import flow.
 *
 * Streams the Transcript_Request TSV from Google Drive, applies the 2-year
 * scope filter (Request_Date >= 2024-07-07), and:
 *   1. Queries Salesforce for Person Accounts matching each Student_ID via
 *      Student_ID_4D__c. Records with no matching Person Account are skipped.
 *   2. Bulk-upserts CSP_Transcript_Request__c (parent) via Bulk API 2.0.
 *      - First/Last name come from the matched Person Account.
 *      - Account__c lookup field set via Account__r.Student_ID_4D__c.
 *   3. Bulk-upserts Transcript_Request_Item__c child records via a second
 *      Bulk API 2.0 job (one record per non-empty Entry_1–5 value).
 *
 * Entry → Item rule (mapping workbook, TS Aug 7):
 *   - One Transcript_Request_Item__c per non-empty Entry (1–5).
 *   - Transcript_Format = "Electronic" AND entry contains "@" → Recipient_Email__c
 *   - All other cases (Paper, unknown, or Electronic with address) → Comments__c
 *   - Fulfillment_Status__c: "Sent" if Sent_Date present; otherwise "Pending"
 *   - Copies__c: Quantity_Total cast to integer (0 → null)
 *   - _4DNL_ tokens replaced with newlines before storing.
 *   - Upsert key for items: External_ID_4D__c = "<parentId>_<entryNum>" (e.g. "7_1")
 *
 * Scope: 2,154 records in 2-year window (2,154/26,020 total).
 *
 * Key conversions (mapping workbook, AS July 7):
 *   - Unfullfilled_Notes: replace _4DNL_ tokens with \n
 *   - Quantity_1–5, Quantity_Total, Unfullfilled_Quantity: cast to int, 0 → null
 *   - Dates (Sent_Date, Transaction_ID_Date_Entered): MM/DD/YYYY; "00/00/00" → null
 *   - Amount: 0% populated in 2yr window but migrated per Amy's instruction
 *   - Transcript_Format: "Electronic" | "Paper" direct map → picklist
 *   - Status__c: "Fulfilled" if Sent_Date present; otherwise "Submitted"
 *
 * UNMAPPED (purple — system audit fields):
 *   Created_Date, Created_Time, Created_By,
 *   Last_Modified_Date, Last_Modified_Time, Last_Modified_By,
 *   Student_ID_Previous (0% in 2yr window)
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse } from "papaparse";
import {
  str,
  toDate,
  cleanEmail,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
} from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// ── Constants ─────────────────────────────────────────────────────────────────
const SF_OBJECT        = "CSP_Transcript_Request__c";
const SF_ITEM_OBJECT   = "Transcript_Request_Item__c";
const EXT_ID_FIELD     = "External_ID_4D__c";
const BULK_BATCH_SIZE  = 50_000;
const SF_API_VERSION   = "v61.0";
const SOQL_BATCH_SIZE  = 200; // Student IDs per SOQL IN clause

// Drives the 2-year scope filter per workbook (AS, July 7).
const TWO_YEAR_CUTOFF = "2024-07-07";

// Entry fields in order — each non-empty entry becomes one child item record.
const ENTRY_FIELDS: Array<{ key: keyof RawTranscriptRow; num: number }> = [
  { key: "Entry_1", num: 1 },
  { key: "Entry_2", num: 2 },
  { key: "Entry_3", num: 3 },
  { key: "Entry_4", num: 4 },
  { key: "Entry_5", num: 5 },
];

// ── Raw row type ──────────────────────────────────────────────────────────────

interface RawTranscriptRow {
  ID?: string;
  Student_ID?: string;
  Last_Name?: string;            // Not used — name comes from Person Account
  First_Name?: string;           // Not used — name comes from Person Account
  Entry_1?: string;
  Entry_2?: string;
  Entry_3?: string;
  Entry_4?: string;
  Entry_5?: string;
  Request_Date?: string;
  Created_Date?: string;         // Do Not Map
  Created_Time?: string;         // Do Not Map
  Created_By?: string;           // Do Not Map
  Last_Modified_Date?: string;   // Do Not Map
  Last_Modified_Time?: string;   // Do Not Map
  Last_Modified_By?: string;     // Do Not Map
  Email_Address?: string;
  Student_ID_Previous?: string;  // Do Not Map (0% in 2yr window)
  Sent_Date?: string;
  Quantity_1?: string;
  Quantity_2?: string;
  Quantity_3?: string;
  Quantity_4?: string;
  Quantity_5?: string;
  Quantity_Total?: string;
  Student_ID_Longint?: string;
  Transaction_ID?: string;
  Amount?: string;
  Transaction_ID_Date_Entered?: string;
  Transcript_Format?: string;
  Unfullfilled_Quantity?: string;
  Unfullfilled_Notes?: string;
  [key: string]: string | undefined;
}

// ── In-scope row (post-2yr-filter, pre-SF-mapping) ───────────────────────────

interface ScopedRow {
  raw: RawTranscriptRow;
  parentId: string;              // stripDotZero(ID), guaranteed non-empty
  studentId: string;             // str(Student_ID).trim()
  requestDate: string;           // YYYY-MM-DD, passes TWO_YEAR_CUTOFF
  sentDate: string | undefined;  // parseDate(Sent_Date)
  qtyTotal: string | undefined;  // toQtyString(Quantity_Total)
}

// ── Value helpers ─────────────────────────────────────────────────────────────

/** Replace _4DNL_ newline tokens and trim. Returns undefined when empty. */
function expand4DNL(raw: string | undefined): string | undefined {
  const s = str(raw).replace(/_4DNL_/g, "\n").trim();
  return s || undefined;
}

/**
 * Cast quantity to non-zero integer string for Text(255) SF fields.
 * Zero and unparseable → undefined (omit from record).
 */
function toQtyString(raw: string | undefined): string | undefined {
  const s = str(raw).replace(/\.0$/, "").trim();
  if (!s) return undefined;
  const n = parseInt(s, 10);
  if (isNaN(n) || n === 0) return undefined;
  return String(n);
}

/** Parse date with explicit "00/00/00" guard; returns YYYY-MM-DD or undefined. */
function parseDate(raw: string | undefined): string | undefined {
  const s = str(raw).trim();
  // "00/00/00" or "00/00/0000" are sentinel nulls in 4D
  if (!s || /^0+[/\-]0+[/\-]0+$/.test(s)) return undefined;
  const d = toDate(s);
  return d || undefined;
}

/** Convert a YYYY-MM-DD date to a Salesforce Date/Time string (midnight UTC). */
function toDateTime(date: string): string {
  return `${date}T00:00:00.000Z`;
}

/** Strip trailing ".0" artifact from 4D Longint-to-Text export. */
function stripDotZero(v: string | undefined): string {
  return str(v).replace(/\.0$/, "");
}

/** Parse currency value; returns undefined when empty. Zero is preserved. */
function parseCurrency(raw: string | undefined): number | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  const n = parseFloat(s);
  return isNaN(n) ? undefined : n;
}

// ── SF record types ───────────────────────────────────────────────────────────
type TranscriptRecord     = Record<string, unknown>;
type TranscriptItemRecord = Record<string, unknown>;

// ── Step 1: Stream TSV → in-scope rows ───────────────────────────────────────

async function streamRawRows(
  fileId: string,
  accessToken: string,
): Promise<{ rows: ScopedRow[]; totalRows: number; filteredOut: number }> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: ScopedRow[] = [];
    let headers: string[]   = [];
    let totalRows           = 0;
    let filteredOut         = 0;

    papaParse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      skipEmptyLines: true,

      step: (result: Papa.ParseResult<string[]>) => {
        const raw = result.data as unknown as string[];

        if (headers.length === 0) {
          headers = raw.map(
            (h, i) =>
              h.replace(/^﻿/, "").replace(/\r/g, "").trim() || `__blank_${i}`,
          );
          return;
        }

        const row: RawTranscriptRow = {};
        headers.forEach((h, i) => {
          if (!h.startsWith("__blank_")) {
            row[h] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        const parentId = stripDotZero(row.ID);
        if (!parentId) return;
        totalRows++;

        const requestDate = parseDate(row.Request_Date);
        if (!requestDate || requestDate < TWO_YEAR_CUTOFF) {
          filteredOut++;
          return;
        }

        rows.push({
          raw: row,
          parentId,
          studentId:   str(row.Student_ID).trim(),
          requestDate,
          sentDate:    parseDate(row.Sent_Date),
          qtyTotal:    toQtyString(row.Quantity_Total),
        });
      },

      complete: () => resolve({ rows, totalRows, filteredOut }),
      error:    (err: Error) => reject(err),
    });
  });
}

// ── Step 2: Query SF for matching Person Accounts ─────────────────────────────
//
// Matches on Student_ID_4D__c. Returns a map keyed by that field value so
// we can look up names and confirm account existence in O(1) per row.
// Rows with no match are skipped — not imported.

interface AccountInfo {
  firstName: string;
  lastName: string;
}

interface SoqlAccountPage {
  records: Array<{
    Student_ID_4D__c: string | null;
    FirstName: string | null;
    LastName: string | null;
  }>;
  nextRecordsUrl?: string;
  done: boolean;
}

async function fetchAccountMap(
  studentIds: string[],
  sfBase: string,
  sfToken: string,
): Promise<Map<string, AccountInfo>> {
  const map = new Map<string, AccountInfo>();
  if (studentIds.length === 0) return map;

  for (let i = 0; i < studentIds.length; i += SOQL_BATCH_SIZE) {
    const batch    = studentIds.slice(i, i + SOQL_BATCH_SIZE);
    const inClause = batch.map((id) => `'${id}'`).join(",");
    const q =
      `SELECT Student_ID_4D__c,FirstName,LastName FROM Account ` +
      `WHERE IsPersonAccount = true AND Student_ID_4D__c IN (${inClause})`;

    let nextUrl: string | undefined =
      `${sfBase}/services/data/${SF_API_VERSION}/query/`;
    let params: Record<string, string> | undefined = { q };

    while (nextUrl) {
      const resp: { data: SoqlAccountPage } = await axios.get<SoqlAccountPage>(nextUrl, {
        params,
        headers: { Authorization: `Bearer ${sfToken}` },
      });

      for (const rec of resp.data.records) {
        if (rec.Student_ID_4D__c) {
          map.set(rec.Student_ID_4D__c, {
            firstName: rec.FirstName ?? "",
            lastName:  rec.LastName  ?? "",
          });
        }
      }

      // Follow pagination if SF returned a nextRecordsUrl
      nextUrl = resp.data.nextRecordsUrl
        ? `${sfBase}${resp.data.nextRecordsUrl}`
        : undefined;
      params = undefined; // nextRecordsUrl already encodes the query
    }
  }

  return map;
}

// ── Step 3a: Map one row → parent CSP_Transcript_Request__c record ────────────

function mapToTranscriptRecord(
  raw: RawTranscriptRow,
  parentId: string,
  studentId: string,
  requestDate: string,
  sentDate: string | undefined,
  account: AccountInfo | undefined,
): TranscriptRecord {
  // Request_Date__c is Date/Time in SF — send midnight UTC
  const record: TranscriptRecord = {
    [EXT_ID_FIELD]: parentId,
    Request_Date__c: toDateTime(requestDate),
  };

  // ── Person Account link + name ────────────────────────────────────────────
  // Account__c lookup set via external ID — SF fails the record if not found,
  // which surfaces it in the results sheet error CSV.
  record["Account__r.Student_ID_4D__c"] = studentId;
  // Names come from the matched Account; omitted if account not in SF yet.
  if (account?.firstName) record.First_Name__c = account.firstName;
  if (account?.lastName)  record.Last_Name__c  = account.lastName;

  // ── Direct maps ───────────────────────────────────────────────────────────
  if (studentId) record.Student_ID__c = studentId;

  const email = cleanEmail(raw.Email_Address);
  if (email) record.Email_Address__c = email;

  const studentIdLongint = stripDotZero(raw.Student_ID_Longint);
  if (studentIdLongint) record.Student_ID_Longint__c = studentIdLongint;

  const transactionId = str(raw.Transaction_ID).trim();
  if (transactionId) record.Transaction_ID__c = transactionId;

  const transcriptFormat = str(raw.Transcript_Format).trim();
  if (transcriptFormat) record.Transcript_Format__c = transcriptFormat;

  // ── Date fields with 00/00/00 guard ──────────────────────────────────────
  if (sentDate) record.Sent_Date__c = sentDate;

  const txnDateEntered = parseDate(raw.Transaction_ID_Date_Entered);
  if (txnDateEntered) record.Transaction_ID_Date_Entered__c = txnDateEntered;

  // Status__c is required — derived from Sent_Date:
  // Sent_Date present & valid → "Fulfilled", otherwise → "Submitted"
  record.Status__c = sentDate ? "Fulfilled" : "Submitted";

  // ── Quantity fields: Text(255) in SF — store as non-zero integer strings ──
  const qty1 = toQtyString(raw.Quantity_1);
  if (qty1 !== undefined) record.Quantity_1__c = qty1;

  const qty2 = toQtyString(raw.Quantity_2);
  if (qty2 !== undefined) record.Quantity_2__c = qty2;

  const qty3 = toQtyString(raw.Quantity_3);
  if (qty3 !== undefined) record.Quantity_3__c = qty3;

  const qty4 = toQtyString(raw.Quantity_4);
  if (qty4 !== undefined) record.Quantity_4__c = qty4;

  const qty5 = toQtyString(raw.Quantity_5);
  if (qty5 !== undefined) record.Quantity_5__c = qty5;

  const qtyTotal = toQtyString(raw.Quantity_Total);
  if (qtyTotal !== undefined) record.Quantity_Total__c = qtyTotal;

  // ── Unfulfilled fields ────────────────────────────────────────────────────
  const unfulfilledQty = toQtyString(raw.Unfullfilled_Quantity);
  if (unfulfilledQty !== undefined) record.Unfullfilled_Quantity__c = unfulfilledQty;

  const unfulfilledNotes = expand4DNL(raw.Unfullfilled_Notes);
  if (unfulfilledNotes) record.Unfullfilled_Notes__c = unfulfilledNotes;

  // ── Amount: 0% populated in 2yr window but migrated per Amy ──────────────
  const amount = parseCurrency(raw.Amount);
  if (amount !== undefined) record.Amount__c = amount;

  return record;
}

// ── Step 3b: Map one row → child Transcript_Request_Item__c records ───────────
//
// One item per non-empty Entry_1–5 value.
// Electronic + entry contains "@" → Recipient_Email__c
// All other cases                 → Comments__c (TextArea)

function mapToItemRecords(
  raw: RawTranscriptRow,
  parentId: string,
  sentDate: string | undefined,
  qtyTotalStr: string | undefined,
): TranscriptItemRecord[] {
  const format            = str(raw.Transcript_Format).trim();
  const fulfillmentStatus = sentDate ? "Sent" : "Pending";
  // Copies__c is a Number field on the item — parse qty string back to integer
  const copies =
    qtyTotalStr !== undefined ? parseInt(qtyTotalStr, 10) : undefined;

  const items: TranscriptItemRecord[] = [];

  for (const { key, num } of ENTRY_FIELDS) {
    const entryValue = expand4DNL(raw[key] as string | undefined);
    if (!entryValue) continue;

    const item: TranscriptItemRecord = {
      [EXT_ID_FIELD]: `${parentId}_${num}`,
      // Bulk API 2.0 external-ID lookup to parent CSP_Transcript_Request__c
      "Transcript_Request__r.External_ID_4D__c": parentId,
      Fulfillment_Status__c: fulfillmentStatus,
    };

    if (copies !== undefined) item.Copies__c = copies;

    // Route by content: some Electronic records contain a mailing address
    // instead of an email — the "@" check is the reliable discriminator.
    if (format === "Electronic" && entryValue.includes("@")) {
      item.Recipient_Email__c = entryValue;
    } else {
      item.Comments__c = entryValue;
    }

    items.push(item);
  }

  return items;
}

// ── Step 3: Build SF records (filter by account match, use account data) ───────

function buildRecords(
  rows: ScopedRow[],
  accountMap: Map<string, AccountInfo>,
): {
  sfRecords: TranscriptRecord[];
  itemRecords: TranscriptItemRecord[];
} {
  const sfRecords: TranscriptRecord[]       = [];
  const itemRecords: TranscriptItemRecord[] = [];

  for (const { raw, parentId, studentId, requestDate, sentDate, qtyTotal } of rows) {
    // Account may or may not exist — always submit the record.
    // If Student_ID_4D__c has no match in SF, the Bulk API will fail that
    // record and it will appear in the results sheet error CSV.
    const account = accountMap.get(studentId);

    sfRecords.push(
      mapToTranscriptRecord(raw, parentId, studentId, requestDate, sentDate, account),
    );

    const items = mapToItemRecords(raw, parentId, sentDate, qtyTotal);
    itemRecords.push(...items);
  }

  return { sfRecords, itemRecords };
}

// ── Batch helper ──────────────────────────────────────────────────────────────

type FlowLogger = {
  info:  (m: string) => void;
  warn:  (m: string) => void;
  error: (m: string) => void;
};

async function runBatches(
  records: Record<string, unknown>[],
  sfObject: string,
  extIdField: string,
  label: string,
  sfBase: string,
  sfToken: string,
  logger: FlowLogger,
): Promise<{
  processed: number;
  failed: number;
  successfulCsv: string;
  failedCsv: string;
}> {
  let processed     = 0;
  let failed        = 0;
  let successfulCsv = "";
  let failedCsv     = "";

  const batches: Record<string, unknown>[][] = [];
  for (let i = 0; i < records.length; i += BULK_BATCH_SIZE) {
    batches.push(records.slice(i, i + BULK_BATCH_SIZE));
  }

  logger.info(
    `[Transcript Request Import] ${label}: ` +
      `${batches.length} batch(es), ${records.length} records`,
  );

  for (let b = 0; b < batches.length; b++) {
    const result = await runBulkJob(
      sfBase,
      sfToken,
      sfObject,
      extIdField,
      batches[b],
      logger,
      `[${label} Batch ${b + 1}/${batches.length}]`,
    );
    processed += result.numberRecordsProcessed;
    failed    += result.numberRecordsFailed;
    if (result.successfulCsv)
      successfulCsv += (successfulCsv ? "\n" : "") + result.successfulCsv;
    if (result.failedCsv)
      failedCsv += (failedCsv ? "\n" : "") + result.failedCsv;
  }

  return { processed, failed, successfulCsv, failedCsv };
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const transcriptRequestImport = flow({
  name: "Transcript Request Import",
  stableKey: "tr112233-4455-4c67-8891-aabbccddeeff",
  description:
    "Streams the Transcript_Request TSV from Google Drive, filters to the " +
    "2-year window (Request_Date >= 2024-07-07), skips records with no " +
    "matching Person Account (matched via Student_ID_4D__c), then " +
    "bulk-upserts CSP_Transcript_Request__c (name + Account link from " +
    "Account) and Transcript_Request_Item__c child records via Bulk API 2.0.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;
    logger.info("[Transcript Request Import] Starting…");

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars["Google Drive Connection"] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Transcript Request File ID"] as unknown as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Transcript Request File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase  = getSfInstanceUrl(sfConn);

    // ── Step 1: Stream TSV → in-scope rows ───────────────────────────────────
    logger.info("[Transcript Request Import] Streaming TSV from Google Drive…");
    const { rows, totalRows, filteredOut } = await streamRawRows(fileId, gdToken);
    logger.info(
      `[Transcript Request Import] Stream complete — ` +
        `total=${totalRows}, filtered(outside 2yr window)=${filteredOut}, ` +
        `in-scope=${rows.length}`,
    );

    if (rows.length === 0) {
      logger.info("[Transcript Request Import] No in-scope rows — done.");
      return {
        data: { totalRows, filteredOut, noAccount: 0, parents: 0, items: 0 },
      };
    }

    // ── Step 2: Query SF for matching Person Accounts ─────────────────────────
    const uniqueStudentIds = [
      ...new Set(rows.map((r) => r.studentId).filter(Boolean)),
    ];
    logger.info(
      `[Transcript Request Import] Querying SF for ` +
        `${uniqueStudentIds.length} unique Student ID(s) via Student_ID_4D__c…`,
    );
    const accountMap = await fetchAccountMap(uniqueStudentIds, sfBase, sfToken);
    logger.info(
      `[Transcript Request Import] ${accountMap.size} matching Person Account(s) found.`,
    );

    // ── Step 3: Build SF records (all submitted; unmatched accounts fail in SF) ─
    const { sfRecords, itemRecords } = buildRecords(rows, accountMap);
    logger.info(
      `[Transcript Request Import] Records ready — ` +
        `parents=${sfRecords.length}, items=${itemRecords.length}`,
    );

    if (sfRecords.length === 0) {
      logger.info("[Transcript Request Import] No in-scope records to upsert — done.");
      return {
        data: { totalRows, filteredOut, parents: 0, items: 0 },
      };
    }

    // ── Step 4a: Upsert parent records (CSP_Transcript_Request__c) ────────────
    // Parents must be committed before items so the lookup relationship resolves.
    logger.info(
      "[Transcript Request Import] Upserting parent records (CSP_Transcript_Request__c)…",
    );
    const parentResult = await runBatches(
      sfRecords as unknown as Record<string, unknown>[],
      SF_OBJECT,
      EXT_ID_FIELD,
      "Parent",
      sfBase,
      sfToken,
      logger,
    );
    logger.info(
      `[Transcript Request Import] Parents — ` +
        `processed=${parentResult.processed}, failed=${parentResult.failed}`,
    );

    // ── Step 4b: Upsert child records (Transcript_Request_Item__c) ────────────
    // Must run after parents so the lookup relationship resolves.
    let itemResult = {
      processed: 0,
      failed: 0,
      successfulCsv: "",
      failedCsv: "",
    };
    if (itemRecords.length > 0) {
      logger.info(
        "[Transcript Request Import] Upserting child records (Transcript_Request_Item__c)…",
      );
      itemResult = await runBatches(
        itemRecords as unknown as Record<string, unknown>[],
        SF_ITEM_OBJECT,
        EXT_ID_FIELD,
        "Item",
        sfBase,
        sfToken,
        logger,
      );
      logger.info(
        `[Transcript Request Import] Items — ` +
          `processed=${itemResult.processed}, failed=${itemResult.failed}`,
      );
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Transcript Request Import",
        objects: [
          {
            objectName: SF_OBJECT,
            successfulCsv: parentResult.successfulCsv,
            failedCsv:     parentResult.failedCsv,
          },
          {
            objectName: SF_ITEM_OBJECT,
            successfulCsv: itemResult.successfulCsv,
            failedCsv:     itemResult.failedCsv,
          },
        ],
        accessToken: gdToken,
        folderId:    failedFolderId,
      });
      logger.info(`[Transcript Request Import] Results sheet: ${sheet.url}`);
    } catch (err) {
      logger.warn(
        `[Transcript Request Import] Could not write results sheet: ${String(err)}`,
      );
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Transcript Request Import] Complete —` +
        `\n  Source rows:          ${totalRows}` +
        `\n  Filtered (>2yr):      ${filteredOut}` +
        `\n  Parents submitted:    ${sfRecords.length}` +
        `\n  Parents processed:    ${parentResult.processed}` +
        `\n  Parents failed:       ${parentResult.failed}` +
        `\n  Items submitted:      ${itemRecords.length}` +
        `\n  Items processed:      ${itemResult.processed}` +
        `\n  Items failed:         ${itemResult.failed}`,
    );

    return {
      data: {
        totalRows,
        filteredOut,
        parents:         sfRecords.length,
        parentProcessed: parentResult.processed,
        parentFailed:    parentResult.failed,
        items:           itemRecords.length,
        itemProcessed:   itemResult.processed,
        itemFailed:      itemResult.failed,
      },
    };
  },
});

export default [transcriptRequestImport];
