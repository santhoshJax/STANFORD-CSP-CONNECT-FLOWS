/**
 * Stanford CSP Migration – Coursework (Syllabus) flow.
 *
 * Reads the Coursework TSV from Google Drive, filters rows where Syllabus = True
 * and Course_ID is within the 2-year quarter window, then bulk-upserts:
 *   1. Syllabus__c            — Name (normalized), URL__c; upsert key: External_ID_4D__c = Coursework ID
 *   2. Syllabus_Association__c — links Syllabus to CourseOffering; upsert key: External_ID_4D__c = Coursework ID
 *
 * Recurses via context.invokeFlow until the full file is processed.
 *
 * Field mapping (per architect workbook):
 *   ID         → Syllabus_Association__c.External_ID_4D__c  (also used as key for Syllabus__c)
 *   Course_ID  → Syllabus_Association__c.Course_ID__r.External_ID_4D__c  (Lookup → CourseOffering)
 *   Name       → Syllabus__c.Name  (normalize common misspellings → "Syllabus")
 *   URL        → Syllabus__c.URL__c
 *
 * Do Not Map: Filename, Syllabus (filter flag), all audit fields,
 *             Review, Document_ID_Draft, Document_ID_Final
 *
 * Note: field/relationship API names for Syllabus_Association__c
 * (Course_ID__r, Syllabus__r) must match what exists in your org.
 * Adjust if Bulk API returns "Unable to find relationship" errors.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import { str, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 1000;

const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 2;

// 4D quarter number format: {YYYY}{digit} — fa=1, wi=2, sp=3, su=4
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

// ── Raw TSV shape ──────────────────────────────────────────────────────────────

interface RawCourseworkRecord {
  ID?: string;
  Course_ID?: string;
  Name?: string;
  Filename?: string;             // Do Not Map
  Syllabus?: string;             // Filter: True rows only
  URL?: string;
  Created_Date?: string;         // Do Not Map
  Created_Time?: string;         // Do Not Map
  Created_By?: string;           // Do Not Map
  Last_Modified_Date?: string;   // Do Not Map
  Last_Modified_Time?: string;   // Do Not Map
  Last_Modified_By?: string;     // Do Not Map
  Review?: string;               // Do Not Map
  Document_ID_Draft?: string;    // Do Not Map
  Document_ID_Final?: string;    // Do Not Map
  [key: string]: string | undefined;
}

// ── Salesforce target shapes ───────────────────────────────────────────────────

interface SyllabusRecord {
  External_ID_4D__c: string;
  Name: string;
  URL__c?: string;
}

interface SyllabusAssociationRecord {
  External_ID_4D__c: string;
  "Course_ID__r.External_ID_4D__c": string;
  "Syllabus__r.External_ID_4D__c": string;
}

// ── Stream result ──────────────────────────────────────────────────────────────

interface MappedRow {
  syllabus: SyllabusRecord;
  association: SyllabusAssociationRecord;
}

interface StreamResult {
  rows: MappedRow[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
}

// ── Name normalization ─────────────────────────────────────────────────────────

function normalizeSyllabusName(raw: string): string {
  const trimmed = raw.trim();
  if (/^syllaubs$/i.test(trimmed)) return "Syllabus";
  if (/^syllbaus$/i.test(trimmed)) return "Syllabus";
  if (/^syllbus$/i.test(trimmed)) return "Syllabus";
  return trimmed || "Syllabus";
}

// ── Field mapping ──────────────────────────────────────────────────────────────

function mapToCoursework(raw: RawCourseworkRecord): MappedRow | null {
  const id = str(raw.ID).trim();
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  // Filter: only migrate rows where Syllabus = True
  const syllabusFlag = str(raw.Syllabus).trim().toLowerCase();
  if (syllabusFlag !== "true" && syllabusFlag !== "1") return null;

  // Filter: Course_ID within the 2-year quarter window
  const courseId = str(raw.Course_ID).trim();
  if (!courseId) return null;
  const prefix = courseId.split("_")[0];
  if (!VALID_COURSE_ID_PREFIXES.has(prefix)) return null;

  const syllabus: SyllabusRecord = {
    External_ID_4D__c: id,
    Name: normalizeSyllabusName(str(raw.Name)),
  };
  const url = str(raw.URL).trim();
  if (url) syllabus.URL__c = url;

  const association: SyllabusAssociationRecord = {
    External_ID_4D__c: id,
    "Course_ID__r.External_ID_4D__c": courseId,
    "Syllabus__r.External_ID_4D__c": id,
  };

  return { syllabus, association };
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
    const rows: MappedRow[] = [];
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

        if (rows.length >= maxRows) {
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

        const mapped = mapToCoursework(raw as RawCourseworkRecord);
        if (!mapped) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const extId = mapped.syllabus.External_ID_4D__c;
        if (!firstId) firstId = extId;
        lastId = extId;
        rows.push(mapped);
        lastCompletedCursor = rowEndByte;
      },

      complete: () =>
        resolve({
          rows,
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

export const courseworkFlow = flow({
  name: "Coursework Import",
  stableKey: "d6e7f8a9-bbcc-4e6f-0a1b-556677889900",
  description:
    "Reads the Coursework TSV from Google Drive, filters to Syllabus=True rows " +
    "within the 2-year quarter window, and bulk-upserts Syllabus__c then " +
    "Syllabus_Association__c records. Recurses until the full file is processed.",

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

    logger.info(`[Coursework Import] Window ${windowNumber} — byte offset ${byteOffset}`);

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = (configVars as Record<string, unknown>)[
      "Coursework File ID"
    ] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Coursework File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    logger.info(
      `[Coursework Import] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
    );

    const {
      rows,
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
      `[Coursework Import] Parsed ${rows.length} records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    let nextSheetId = sheetId;

    if (rows.length > 0) {
      const syllabusRecords = rows.map((r) => r.syllabus) as unknown as Record<
        string,
        unknown
      >[];
      const associationRecords = rows.map(
        (r) => r.association,
      ) as unknown as Record<string, unknown>[];

      // Job 1: upsert Syllabus__c parent records first so the lookups resolve in Job 2.
      const syllabusResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "Syllabus__c",
        "External_ID_4D__c",
        syllabusRecords,
        logger,
        "[Coursework Import] Syllabus__c",
      );

      // Job 2: upsert Syllabus_Association__c junction records.
      const assocResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "Syllabus_Association__c",
        "External_ID_4D__c",
        associationRecords,
        logger,
        "[Coursework Import] Syllabus_Association__c",
      );

      const combinedFailedIds = new Set([
        ...syllabusResult.failedExternalIds,
        ...assocResult.failedExternalIds,
      ]);

      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Coursework Import",
          objectName: "Syllabus__c / Syllabus_Association__c",
          contacts: syllabusRecords,
          externalIdField: "External_ID_4D__c",
          failedExternalIds: combinedFailedIds,
          successfulCsv: syllabusResult.successfulCsv,
          failedCsv: [syllabusResult.failedCsv, assocResult.failedCsv]
            .filter(Boolean)
            .join("\n"),
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: sheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Coursework Import] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(
          `[Coursework Import] Could not update results sheet: ${String(err)}`,
        );
      }
    } else {
      logger.info(
        `[Coursework Import] Window contained no records after filtering; skipping upload.`,
      );
    }

    if (hasMore) {
      logger.info(
        `[Coursework Import] More rows remain — invoking window ${windowNumber + 1} at byte ${nextByteOffset}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Coursework Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(`[Coursework Import] All rows processed — import complete.`);
    }

    return {
      data: {
        byteOffset,
        rowsProcessed: rows.length,
        hasMore,
      },
    };
  },
});

export default [courseworkFlow];
