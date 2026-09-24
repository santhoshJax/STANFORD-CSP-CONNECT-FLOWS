/**
 * Stanford CSP Migration – Course Timecard flow.
 *
 * Reads the Timecard TSV from Google Drive, maps each row to a
 * Course_Offering_Staff_Timecard__c record, and bulk-upserts via Bulk API 2.0.
 * Recurses via context.invokeFlow until the full file is processed.
 *
 * Field mapping (per architect workbook):
 *   ID                  → External_ID_4D__c
 *   Pay_Period_End      → Pay_Period_End__c (Date, M/D/YYYY → YYYY-MM-DD)
 *   Job_Number          → Job_Number__c (Text, cast from int; 0 = primary job)
 *   Hours_Regular       → Hours_Regular__c
 *   Hours_Overtime      → Hours_Overtime__c
 *   Hours_Double_OT     → Hours_Double_OT__c
 *   Hours_Sick          → Hours_Sick__c
 *   Course_Instructor_ID (>0) → Course_Offering_Staff_Detail__r.External_ID_4D__c = "CINST-SD-{value}"
 *   Course_Associate_ID (>0)  → Course_Offering_Staff_Detail__r.External_ID_4D__c = "CASSOC-SD-{value}"
 *   (mutually exclusive per row — only one will be non-zero)
 *
 * Do Not Map: University_ID, Employee_Name, QCode (formula fields via Staff Detail → Contact)
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import { str, toDate, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 1000;

const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 2;

// 4D quarter number format: {YYYY}{digit} — fa=1, wi=2, sp=3, su=4
function buildValidQuarterCodes(anchor: string, yearsBack: number): Set<string> {
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

const VALID_QUARTERS = buildValidQuarterCodes(ANCHOR_QUARTER, YEARS_BACK);

// ── Raw TSV shape ──────────────────────────────────────────────────────────────

interface RawTimecardRecord {
  ID?: string;
  University_ID?: string;      // Do Not Map
  Employee_Name?: string;      // Do Not Map
  QCode?: string;              // Do Not Map
  Pay_Period_End?: string;
  Job_Number?: string;
  Hours_Regular?: string;
  Hours_Overtime?: string;
  Hours_Double_OT?: string;
  Hours_Sick?: string;
  Course_Instructor_ID?: string;
  Course_Associate_ID?: string;
  [key: string]: string | undefined;
}

// ── Salesforce target shape ────────────────────────────────────────────────────

interface TimecardRecord {
  External_ID_4D__c: string;
  "Course_Offering_Staff_Detail__r.External_ID_4D__c"?: string;
  Pay_Period_End__c?: string;
  Job_Number__c?: string;
  Hours_Regular__c?: number | string;
  Hours_Overtime__c?: number | string;
  Hours_Double_OT__c?: number | string;
  Hours_Sick__c?: number | string;
}

// ── Stream result ──────────────────────────────────────────────────────────────

interface StreamResult {
  records: Partial<TimecardRecord>[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function parseNum(v: string | undefined): number | string {
  const n = parseFloat(str(v));
  return isNaN(n) ? "" : n;
}

// ── Field mapping ──────────────────────────────────────────────────────────────

function mapToTimecard(
  raw: RawTimecardRecord,
): Partial<TimecardRecord> | null {
  const id = str(raw.ID).trim();
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  const qcode = str(raw.QCode).trim();
  if (!qcode || !VALID_QUARTERS.has(qcode)) return null;

  const record: Partial<TimecardRecord> = {
    External_ID_4D__c: id,
  };

  // Parent Staff Detail lookup — mutually exclusive FKs.
  // Course_Instructor_ID > 0 → instructor timecard (CINST-SD- prefix).
  // Course_Associate_ID > 0  → associate timecard (CASSOC-SD- prefix).
  const instrId = parseInt(str(raw.Course_Instructor_ID).trim(), 10);
  const assocId = parseInt(str(raw.Course_Associate_ID).trim(), 10);

  if (instrId > 0) {
    record["Course_Offering_Staff_Detail__r.External_ID_4D__c"] =
      `CINST-SD-${instrId}`;
  } else if (assocId > 0) {
    record["Course_Offering_Staff_Detail__r.External_ID_4D__c"] =
      `CASSOC-SD-${assocId}`;
  }

  const payPeriodEnd = toDate(raw.Pay_Period_End);
  if (payPeriodEnd) record.Pay_Period_End__c = payPeriodEnd;

  // Job_Number is a small integer (0-9) but stored as Text; 0 = primary job.
  const jobNum = str(raw.Job_Number).trim().replace(/\.0$/, "");
  if (jobNum !== "") record.Job_Number__c = jobNum;

  record.Hours_Regular__c = parseNum(raw.Hours_Regular);
  record.Hours_Overtime__c = parseNum(raw.Hours_Overtime);
  record.Hours_Double_OT__c = parseNum(raw.Hours_Double_OT);
  record.Hours_Sick__c = parseNum(raw.Hours_Sick);

  return record;
}

// ── TSV streaming ──────────────────────────────────────────────────────────────

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
): Promise<StreamResult> {
  const reqHeaders: Record<string, string> = {
    Authorization: `Bearer ${accessToken}`,
  };
  if (byteOffset > 0) reqHeaders.Range = `bytes=${byteOffset}-`;

  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: reqHeaders,
      responseType: "stream",
    },
  );

  const lineEndBytes: number[] = [];
  let totalBytesReceived = 0;

  const byteTracker = new Transform({
    transform(
      chunk: Buffer,
      _encoding: BufferEncoding,
      callback: (err?: Error | null, data?: Buffer) => void,
    ) {
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] === 0x0a) {
          lineEndBytes.push(byteOffset + totalBytesReceived + i + 1);
        }
      }
      totalBytesReceived += chunk.length;
      callback(null, chunk);
    },
  });

  (response.data as NodeJS.ReadableStream).pipe(byteTracker);

  return new Promise((resolve, reject) => {
    let parsedHeaders: string[] =
      knownHeaders.length > 0 ? [...knownHeaders] : [];
    const records: Partial<TimecardRecord>[] = [];
    let aborted = false;
    let firstId = "";
    let lastId = "";
    let skippedCount = 0;
    let lastCompletedCursor = byteOffset;
    let stepCount = 0;

    parse(byteTracker as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: false,
      skipEmptyLines: false,
      quoteChar: "\x00",

      step: (result: ParseResult<string[]>, parser: Parser) => {
        if (aborted) return;

        const row = result.data as unknown as string[];
        const rowEndByte =
          lineEndBytes[stepCount] ?? byteOffset + totalBytesReceived;
        stepCount++;

        if (row.length === 0 || row.every((c) => c.replace(/\r/g, "") === "")) {
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (parsedHeaders.length === 0) {
          parsedHeaders = row.map((h) => h.replace(/\r/g, "").trim());
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (records.length >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        if (row.length !== parsedHeaders.length) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const raw: Record<string, string> = {};
        parsedHeaders.forEach((header, i) => {
          raw[header] = row[i] ?? "";
        });

        const mapped = mapToTimecard(raw as RawTimecardRecord);
        if (!mapped) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const extId = mapped.External_ID_4D__c ?? "";
        if (!firstId) firstId = extId;
        lastId = extId;
        records.push(mapped);
        lastCompletedCursor = rowEndByte;
      },

      complete: () =>
        resolve({
          records,
          hasMore: aborted,
          nextByteOffset: lastCompletedCursor,
          parsedHeaders,
          firstId,
          lastId,
          skippedCount,
        }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const courseTimecardFlow = flow({
  name: "Course Timecard",
  stableKey: "c5d6e7f8-ffff-4c6d-9e7f-445566778899",
  description:
    "Reads the Timecard TSV from Google Drive, maps each row to a " +
    "Course_Offering_Staff_Timecard__c record (with parent Staff Detail " +
    "lookup via CINST-SD- or CASSOC-SD- prefix), and upserts via Bulk API 2.0. " +
    "Recurses until the full file is processed.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    const triggerBody = (
      params.onTrigger.results as unknown as
        | { body?: { data?: unknown } }
        | undefined
    )?.body?.data as Record<string, unknown> | undefined;

    const byteOffset =
      typeof triggerBody?.byteOffset === "number" ? triggerBody.byteOffset : 0;
    const knownHeaders = Array.isArray(triggerBody?.headers)
      ? (triggerBody.headers as string[])
      : [];
    const windowNumber =
      typeof triggerBody?.windowNumber === "number"
        ? triggerBody.windowNumber
        : 1;
    const sheetId =
      typeof triggerBody?.sheetId === "string"
        ? triggerBody.sheetId
        : undefined;

    logger.info(`[Course Timecard] Starting at byte offset ${byteOffset}`);

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = (configVars as Record<string, unknown>)[
      "Timecard File ID"
    ] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Timecard File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    logger.info(
      `[Course Timecard] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
    );

    const {
      records,
      hasMore,
      nextByteOffset,
      parsedHeaders,
      firstId,
      lastId,
      skippedCount,
    } = await streamAndParseTsv(
      fileId,
      gdToken,
      byteOffset,
      MAX_ROWS,
      knownHeaders,
    );

    logger.info(
      `[Course Timecard] Parsed ${records.length} records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    let nextSheetId = sheetId;

    if (records.length > 0) {
      const jobResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "Course_Offering_Staff_Timecard__c",
        "External_ID_4D__c",
        records as Record<string, unknown>[],
        logger,
        "[Course Timecard]",
      );

      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Course Timecard",
          objectName: "Course_Offering_Staff_Timecard__c",
          contacts: records as Record<string, unknown>[],
          externalIdField: "External_ID_4D__c",
          failedExternalIds: jobResult.failedExternalIds,
          successfulCsv: jobResult.successfulCsv,
          failedCsv: jobResult.failedCsv,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: sheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Course Timecard] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(
          `[Course Timecard] Could not update results sheet: ${String(err)}`,
        );
      }
    } else {
      logger.info(
        `[Course Timecard] Window contained no records; skipping upload.`,
      );
    }

    if (hasMore) {
      logger.info(
        `[Course Timecard] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Course Timecard", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(`[Course Timecard] All rows processed — import complete.`);
    }

    return {
      data: {
        byteOffset,
        rowsProcessed: records.length,
        hasMore,
      },
    };
  },
});

export default [courseTimecardFlow];
