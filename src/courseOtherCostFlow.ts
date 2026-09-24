/**
 * Stanford CSP Migration – Course Other Cost flow.
 *
 * Reads the Course_Other_Cost TSV from Google Drive, maps each row to a
 * CourseOfferingExpense__c record, and bulk-upserts via Bulk API 2.0.
 * Recurses via context.invokeFlow until the full file is processed.
 *
 * Field mapping (per architect workbook):
 *   ID          → External_ID_4D__c
 *   Course_RecID→ Course_Offering__r.External_ID_4D__c  ⚠️ open: confirm Course_RecID = CourseOffering.External_ID_4D__c
 *   Cost        → Actual_Cost__c
 *   Description → Description
 *
 * Do Not Map: Created_Date, Created_Time, Created_By,
 *             Last_Modified_Date, Last_Modified_Time, Last_Modified_By
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import { str, getAccessToken, getSfInstanceUrl, runBulkJob, loadCourseRecIdMap } from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 1000;

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

// ── Raw TSV shape ──────────────────────────────────────────────────────────────

interface RawCourseOtherCostRecord {
  ID?: string;
  Course_RecID?: string;
  Cost?: string;
  Description__c?: string;
  [key: string]: string | undefined;
}

// ── Salesforce target shape ────────────────────────────────────────────────────

interface CourseOfferingExpenseRecord {
  External_ID_4D__c: string;
  "CourseOffering__r.External_ID_4D__c"?: string;
  Actual_Cost__c?: number | string;
  Description__c?: string;
}

// ── Stream result ──────────────────────────────────────────────────────────────

interface StreamResult {
  records: Partial<CourseOfferingExpenseRecord>[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
}

// ── Field mapping ──────────────────────────────────────────────────────────────

function mapToExpense(
  raw: RawCourseOtherCostRecord,
  courseRecIdMap?: Map<string, string>,
  missingRecIds?: Set<string>,
): Partial<CourseOfferingExpenseRecord> | null {
  const id = str(raw.ID).trim();
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  const record: Partial<CourseOfferingExpenseRecord> = {
    External_ID_4D__c: id,
  };

  const courseRecId = str(raw.Course_RecID).trim();
  if (courseRecId) {
    const courseId = courseRecIdMap?.get(courseRecId);
    if (courseId) {
      const prefix = courseId.split("_")[0];
      if (!VALID_COURSE_ID_PREFIXES.has(prefix)) return null;
      record["CourseOffering__r.External_ID_4D__c"] = courseId;
    } else {
      missingRecIds?.add(courseRecId);
    }
  }

  const cost = parseFloat(str(raw.Cost));
  if (!isNaN(cost)) record.Actual_Cost__c = cost;

  const description = str(raw.Description).trim();
  if (description) record.Description__c = description;

  return record;
}

// ── TSV streaming ──────────────────────────────────────────────────────────────

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  courseRecIdMap?: Map<string, string>,
  missingRecIds?: Set<string>,
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
    const records: Partial<CourseOfferingExpenseRecord>[] = [];
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

        const mapped = mapToExpense(raw as RawCourseOtherCostRecord, courseRecIdMap, missingRecIds);
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

export const courseOtherCostFlow = flow({
  name: "Course Other Cost",
  stableKey: "b4c5d6e7-eeee-4b5c-8d6e-334455667788",
  description:
    "Reads the Course_Other_Cost TSV from Google Drive, maps each row to a " +
    "CourseOfferingExpense__c record, and upserts via Bulk API 2.0. " +
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

    logger.info(`[Course Other Cost] Starting at byte offset ${byteOffset}`);

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = (configVars as Record<string, unknown>)[
      "Course Other Cost File ID"
    ] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Course Other Cost File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    const courseFileId = (configVars as Record<string, unknown>)[
      "Course File ID"
    ] as string;
    if (!courseFileId)
      throw new Error(
        "Course File ID config var is required for Course Other Cost flow.",
      );
    const courseRecIdMap = await loadCourseRecIdMap(courseFileId, gdToken);
    logger.info(
      `[Course Other Cost] Loaded ${courseRecIdMap.size} course RecID→ID mappings`,
    );
    const missingRecIds = new Set<string>();

    logger.info(
      `[Course Other Cost] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
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
      courseRecIdMap,
      missingRecIds,
    );

    logger.info(
      `[Course Other Cost] Parsed ${records.length} records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    if (missingRecIds.size > 0) {
      logger.warn(
        `[Course Other Cost] ${missingRecIds.size} Course_RecID(s) not found in crosswalk: ${[...missingRecIds].join(", ")}`,
      );
    }

    let nextSheetId = sheetId;

    if (records.length > 0) {
      const jobResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "CourseOfferingExpense__c",
        "External_ID_4D__c",
        records as Record<string, unknown>[],
        logger,
        "[Course Other Cost]",
      );

      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Course Other Cost",
          objectName: "CourseOfferingExpense__c",
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
        logger.info(`[Course Other Cost] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(
          `[Course Other Cost] Could not update results sheet: ${String(err)}`,
        );
      }
    } else {
      logger.info(
        `[Course Other Cost] Window contained no records; skipping upload.`,
      );
    }

    if (hasMore) {
      logger.info(
        `[Course Other Cost] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Course Other Cost", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(
        `[Course Other Cost] All rows processed — import complete.`,
      );
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

export default [courseOtherCostFlow];
