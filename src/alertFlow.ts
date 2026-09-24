/**
 * Stanford CSP Migration – Alert (RecordAlert) flow.
 *
 * Reads the Alert TSV from Google Drive, maps each row to a standard
 * Salesforce RecordAlert record, and upserts via Bulk API 2.0. Recurses via
 * context.invokeFlow until the full file is processed — same streaming
 * shape as every other flow in this project (see courseOtherCostFlow.ts).
 *
 * Field mapping (per "CSP Data Migration Mapping Workbook - Alert.pdf",
 * field API names confirmed against the RecordAlert object describe):
 *
 *   ID                          → SourceSystemIdentifier     (Upsert / external ID)
 *     Standard field, type "External Lookup (Unique)" — confirmed with SA.
 *
 *   Student_ID / Associate_ID / Instructor_ID
 *                                → ParentId AND WhatId       (Lookup → PersonAccount)
 *     Conversion: whichever of Student_ID, Associate_ID, Instructor_ID is
 *     populated and not "0" identifies the PersonAccount to look up (by
 *     Student_ID_4D__c / Associate_ID_4D__c / Instructor_ID_4D__c respectively).
 *     SA confirmed by reviewing the full source data: exactly one of the three
 *     is ever populated per row, never two — so the Student→Associate→
 *     Instructor priority order in code is a defensive fallback only, not
 *     something expected to actually trigger. The resolved Account Id is
 *     written to BOTH ParentId and WhatId, matching the two identical Lookup
 *     rows in the workbook (one labelled "Parent Record", one "What Record").
 *
 *   Title                        → Subject                    (Direct Map)
 *
 *   Category                     → RecordAlertCategory         (Lookup → RecordAlertCategory)
 *     Conversion: Behavioral→Behavioral, Payment→Payment, STAP→STAP,
 *     Grades→Grades, anything else→Other. Looked up against
 *     RecordAlertCategory.Name in the target org. Field name confirmed via
 *     Setup → Object Manager → Record Alert → Fields (no "Id" suffix, despite
 *     ParentId/WhatId having one).
 *
 *   Course_RecID                 → Course_Offering__c          (Lookup → CourseOffering)
 *     Field name confirmed via Setup → Object Manager → Record Alert → Fields.
 *     Conversion: Course_RecID identifies the CourseOffering this alert is
 *     tied to. Resolved the same way as every other Course_RecID lookup in
 *     this project — via the Course TSV RecID→ID crosswalk (loadCourseRecIdMap)
 *     — then set on the CSV upload using Salesforce's relationship/external-ID
 *     syntax (Course_Offering__r.External_ID_4D__c), same pattern as
 *     courseAssociateInstructorFlow.ts and textbookFlow.ts. Requires the same
 *     "Course File ID" config var those flows use. Skipped (left blank, not
 *     row-dropped) when Course_RecID is "0"/blank or isn't found in the
 *     crosswalk — unlike courseOtherCostFlow.ts, this flow does NOT drop the
 *     whole Alert record just because its course falls outside the 2-year
 *     migration window, since the alert is still meaningful people-data
 *     without a course link.
 *
 *   Description                  → Description                (Direct Map)
 *     _4DNL_ tokens from the 4D export are converted to real newlines, same
 *     handling as every other flow in this project (e.g. Notes__c in
 *     courseAssociateInstructorFlow.ts).
 *
 *   Created_Date + Created_Time  → EffectiveStartDate          (Conversion)
 *     The two source columns are combined into a single Date/Time value.
 *
 *   (no source field)            → Severity = "Info"           (Conversion)
 *     Workbook: "Default Map to 'Info'" — hardcoded, not read from the source.
 *
 *   Created_Date                 → ValidUntilDate               (Conversion)
 *     Set to Created_Date + 50 years. ValidUntilDate is a Date/Time field, so
 *     the time-of-day is copied from EffectiveStartDate (i.e. the alert's own
 *     Created_Time) rather than defaulting to midnight — confirmed correct.
 *
 * Do Not Map (per workbook): Created_By, Last_Modified_Date, Last_Modified_Time,
 *   Last_Modified_By, Student_ID_Previous.
 *
 * ── OPEN QUESTIONS FOR ARCHITECT (none of these are resolved by this flow —
 *    each is called out again inline at the relevant code) ────────────────────
 *
 *   Q1. RESOLVED (workbook v2) — Course_RecID now maps to a "Course Offering"
 *       lookup, Field API Course_Offering__c. Implemented below as
 *       COURSE_LOOKUP_RELATIONSHIP_FIELD. Field name confirmed — see Q5.
 *
 *   Q2. RESOLVED — SA reviewed the full source data and confirmed exactly one
 *       of Student_ID / Associate_ID / Instructor_ID is ever populated per
 *       row, never two at once. The Student→Associate→Instructor priority
 *       order in mapAlertRow() is kept as a defensive fallback only.
 *
 *   Q3. RecordAlertCategory reference data — this flow looks up existing
 *       RecordAlertCategory records by Name (Behavioral/Payment/STAP/Grades/
 *       Other) but does not create them. Are all five guaranteed to already
 *       exist in the target org before this flow runs?
 *
 *   Q4. RESOLVED — confirmed the time-of-day should follow the source data
 *       (i.e. reuse Created_Time / EffectiveStartDate's time), not default to
 *       midnight. Matches what this flow already does.
 *
 *   Q5. RESOLVED — SA confirmed via Setup → Object Manager → Record Alert →
 *       Fields that the Course Offering lookup's real API name is
 *       "Course_Offering__c", matching this flow's assumption exactly. CSV
 *       upload uses "Course_Offering__r.External_ID_4D__c" (relationship
 *       name + external-ID field) — see COURSE_LOOKUP_RELATIONSHIP_FIELD.
 *
 *   Q6. Date-scoping — this flow currently imports EVERY Alert row regardless
 *       of Created_Date, with no cutoff. Several other flows in this project
 *       (Course, Coursework, Textbook, Timecard, and especially Transcript
 *       Request — a dated event record shaped just like Alert) apply a hard
 *       2-year cutoff before migrating. For Transcript Request that cutoff
 *       took the import from 26,020 rows down to 2,154. Should Alert have the
 *       same kind of Created_Date cutoff, or is "migrate everything" correct
 *       for this object? Not implemented either way until confirmed.
 */

import { flow } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import {
  str,
  toDate,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
  resolveAccountIdsByField,
  loadCourseRecIdMap,
  SF_API_VERSION,
} from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 1000;

// Confirmed against the RecordAlert object describe — see header comment.
const EXTERNAL_ID_FIELD = "SourceSystemIdentifier";
const CATEGORY_LOOKUP_FIELD = "RecordAlertCategory";

// Q5 RESOLVED — confirmed against the RecordAlert object describe (see header).
const COURSE_LOOKUP_RELATIONSHIP_FIELD = "Course_Offering__r.External_ID_4D__c";

const VALID_UNTIL_YEARS = 50;

type ParentLookupField =
  "Student_ID_4D__c" | "Associate_ID_4D__c" | "Instructor_ID_4D__c";

// ── Raw TSV shape ──────────────────────────────────────────────────────────────

interface RawAlertRecord {
  ID?: string;
  Student_ID?: string;
  Title?: string;
  Category?: string;
  Associate_ID?: string;
  Instructor_ID?: string;
  Course_RecID?: string; // → Course_Offering__r.External_ID_4D__c (Q5)
  Description?: string;
  Created_Date?: string;
  Created_Time?: string;
  [key: string]: string | undefined;
}

// ── Salesforce RecordAlert target shape ────────────────────────────────────────

interface RecordAlertUpload {
  ParentId?: string;
  WhatId?: string;
  Subject?: string;
  Description?: string;
  EffectiveStartDate?: string;
  Severity?: string;
  ValidUntilDate?: string;
  // Also holds EXTERNAL_ID_FIELD and CATEGORY_LOOKUP_FIELD (dynamic keys).
  [key: string]: unknown;
}

/** A parsed row whose Parent/What/Category lookups haven't been resolved yet. */
interface PendingAlertRow {
  record: RecordAlertUpload;
  externalId: string;
  parentLookupField: ParentLookupField | null;
  parentLookupValue: string;
  categoryName: string;
}

interface StreamResult {
  rows: PendingAlertRow[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/** Combine a 4D Date column and a 4D Time column into one Date/Time ISO string. */
function combineDateTime(
  dateStr: string | undefined,
  timeStr: string | undefined,
): string {
  const d = toDate(dateStr);
  if (!d) return "";

  const t = str(timeStr);
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?/.exec(t);
  const hh = (m ? m[1] : "0").padStart(2, "0");
  const mm = m ? m[2] : "00";
  const ss = m ? (m[3] ?? "00") : "00";

  return `${d}T${hh}:${mm}:${ss}Z`;
}

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

/**
 * Add N years to an ISO Date/Time string produced by combineDateTime().
 * Handles the Feb 29 leap-day edge case: if the source date is Feb 29 and
 * the target year isn't a leap year, rolls back to Feb 28 (standard
 * "add years" calendar behavior) instead of emitting an invalid date like
 * "2075-02-29", which Salesforce would reject for that row.
 */
function addYears(isoDateTime: string, years: number): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(.+)$/.exec(isoDateTime);
  if (!m) return "";
  const [, y, mo, da, rest] = m;
  const newYear = parseInt(y, 10) + years;
  const day = mo === "02" && da === "29" && !isLeapYear(newYear) ? "28" : da;
  return `${newYear}-${mo}-${day}T${rest}`;
}

/**
 * 4D Category picklist → RecordAlertCategory.Name.
 * Behavioral = Behavioral, Payment = Payment, STAP = STAP, Grades = Grades,
 * all other values (including blank) = Other.
 */
function mapCategoryName(raw: string | undefined): string {
  const v = str(raw).toLowerCase();
  if (v === "behavioral") return "Behavioral";
  if (v === "payment") return "Payment";
  if (v === "stap") return "STAP";
  if (v === "grades") return "Grades";
  return "Other";
}

// Q3 (see header) — assumes Behavioral/Payment/STAP/Grades/Other already
// exist as RecordAlertCategory records; this flow only looks them up.
/** Loads the full RecordAlertCategory table as Map<Name, Id>. Small reference table. */
async function loadRecordAlertCategoryMap(
  instanceUrl: string,
  accessToken: string,
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const headers = { Authorization: `Bearer ${accessToken}` };
  let done = false;
  let nextUrl: string | null = null;

  const fetchPage = async (url: string | null) => {
    const { data } = await axios.get<{
      records: { Id: string; Name: string }[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      url
        ? { headers }
        : {
            params: { q: "SELECT Id, Name FROM RecordAlertCategory" },
            headers,
          },
    );
    for (const rec of data.records) {
      map.set(rec.Name, rec.Id);
    }
    done = data.done;
    nextUrl = data.nextRecordsUrl
      ? `${instanceUrl}${data.nextRecordsUrl}`
      : null;
  };

  await fetchPage(null);
  while (!done && nextUrl) await fetchPage(nextUrl);

  return map;
}

// ── Field mapping (per row, before Parent/What/Category lookups) ──────────────

function mapAlertRow(
  raw: RawAlertRecord,
  courseRecIdMap: Map<string, string>,
  missingCourseRecIds: Set<string>,
): PendingAlertRow | null {
  const id = str(raw.ID);
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  const record: RecordAlertUpload = {};
  record[EXTERNAL_ID_FIELD] = id;

  // Course_RecID → Course_Offering lookup. Left blank (not row-dropped) when
  // "0"/blank or not found in the crosswalk — see header comment.
  const courseRecId = str(raw.Course_RecID);
  if (courseRecId && courseRecId !== "0") {
    const courseId = courseRecIdMap.get(courseRecId);
    if (courseId) {
      record[COURSE_LOOKUP_RELATIONSHIP_FIELD] = courseId;
    } else {
      missingCourseRecIds.add(courseRecId);
    }
  }

  const title = str(raw.Title);
  if (title) record.Subject = title;

  const description = str(raw.Description)
    .replace(/_4DNL_/g, "\n")
    .trim()
    .slice(0, 32000);
  if (description) record.Description = description;

  // Severity has no source column — always "Info" per workbook.
  record.Severity = "Info";

  const effectiveStart = combineDateTime(raw.Created_Date, raw.Created_Time);
  if (effectiveStart) {
    record.EffectiveStartDate = effectiveStart;
    // Q4 RESOLVED — time-of-day follows the source data (EffectiveStartDate),
    // not midnight.
    const validUntil = addYears(effectiveStart, VALID_UNTIL_YEARS);
    if (validUntil) record.ValidUntilDate = validUntil;
  }

  // Parent/What lookup — Student_ID, then Associate_ID, then Instructor_ID,
  // whichever is populated and not "0". Q2 RESOLVED: SA confirmed only one
  // is ever populated per row, so this priority order is a defensive
  // fallback that isn't expected to actually trigger.
  let parentLookupField: ParentLookupField | null = null;
  let parentLookupValue = "";

  const studentId = str(raw.Student_ID);
  const associateId = str(raw.Associate_ID);
  const instructorId = str(raw.Instructor_ID);

  if (studentId && studentId !== "0") {
    parentLookupField = "Student_ID_4D__c";
    parentLookupValue = studentId;
  } else if (associateId && associateId !== "0") {
    parentLookupField = "Associate_ID_4D__c";
    parentLookupValue = associateId;
  } else if (instructorId && instructorId !== "0") {
    parentLookupField = "Instructor_ID_4D__c";
    parentLookupValue = instructorId;
  }

  return {
    record,
    externalId: id,
    parentLookupField,
    parentLookupValue,
    categoryName: mapCategoryName(raw.Category),
  };
}

// ── TSV streaming ──────────────────────────────────────────────────────────────

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  courseRecIdMap: Map<string, string>,
  missingCourseRecIds: Set<string>,
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
    const rows: PendingAlertRow[] = [];
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

        const mapped = mapAlertRow(
          raw as RawAlertRecord,
          courseRecIdMap,
          missingCourseRecIds,
        );
        if (!mapped) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (!firstId) firstId = mapped.externalId;
        lastId = mapped.externalId;
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

export const alertImport = flow({
  name: "Alert Import",
  stableKey: "a1e2r3t4-9999-4a1e-8b2c-556677889900",
  description:
    "Reads the Alert TSV from Google Drive, maps each row to a RecordAlert " +
    "record (resolving ParentId/WhatId to a PersonAccount, the Category " +
    "picklist to a RecordAlertCategory lookup, and Course_RecID to a " +
    "CourseOffering lookup), and upserts via Bulk API 2.0. Recurses until " +
    "the full file is processed.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    const triggerBody = (
      params.onTrigger.results as unknown as
        { body?: { data?: unknown } } | undefined
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

    logger.info(`[Alert] Starting at byte offset ${byteOffset}`);

    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = (configVars as Record<string, unknown>)[
      "Alert File ID"
    ] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      string | undefined;

    if (!fileId) throw new Error("Alert File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // Course_RecID → Course_Offering lookup crosswalk (same file/helper every
    // other Course_RecID-resolving flow in this project uses).
    const courseFileId = (configVars as Record<string, unknown>)[
      "Course File ID"
    ] as string;
    if (!courseFileId) {
      throw new Error(
        "Course File ID config var is required for Alert Import (used to resolve Course_RecID).",
      );
    }
    const courseRecIdMap = await loadCourseRecIdMap(courseFileId, gdToken);
    logger.info(
      `[Alert] Loaded ${courseRecIdMap.size} course RecID→ID mappings`,
    );
    const missingCourseRecIds = new Set<string>();

    logger.info(
      `[Alert] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
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
      courseRecIdMap,
      missingCourseRecIds,
    );

    if (missingCourseRecIds.size > 0) {
      logger.warn(
        `[Alert] ${missingCourseRecIds.size} Course_RecID(s) not found in crosswalk: ` +
          `${[...missingCourseRecIds].join(", ")} — Course_Offering left blank on those rows.`,
      );
    }

    logger.info(
      `[Alert] Parsed ${rows.length} records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    let nextSheetId = sheetId;
    let finalRecords: RecordAlertUpload[] = [];

    if (rows.length > 0) {
      // Resolve Category (RecordAlertCategory) and Parent/What (PersonAccount)
      // lookups in batch before uploading.
      const categoryMap = await loadRecordAlertCategoryMap(
        sfInstanceUrl,
        sfToken,
      );

      const studentIds = [
        ...new Set(
          rows
            .filter((r) => r.parentLookupField === "Student_ID_4D__c")
            .map((r) => r.parentLookupValue),
        ),
      ];
      const associateIds = [
        ...new Set(
          rows
            .filter((r) => r.parentLookupField === "Associate_ID_4D__c")
            .map((r) => r.parentLookupValue),
        ),
      ];
      const instructorIds = [
        ...new Set(
          rows
            .filter((r) => r.parentLookupField === "Instructor_ID_4D__c")
            .map((r) => r.parentLookupValue),
        ),
      ];

      const [studentMap, associateMap, instructorMap] = await Promise.all([
        resolveAccountIdsByField(
          sfInstanceUrl,
          sfToken,
          "Student_ID_4D__c",
          studentIds,
        ),
        resolveAccountIdsByField(
          sfInstanceUrl,
          sfToken,
          "Associate_ID_4D__c",
          associateIds,
        ),
        resolveAccountIdsByField(
          sfInstanceUrl,
          sfToken,
          "Instructor_ID_4D__c",
          instructorIds,
        ),
      ]);

      let missingParentCount = 0;
      let missingCategoryCount = 0;

      finalRecords = rows.map((row) => {
        const record = row.record;

        if (row.parentLookupField) {
          const map =
            row.parentLookupField === "Student_ID_4D__c"
              ? studentMap
              : row.parentLookupField === "Associate_ID_4D__c"
                ? associateMap
                : instructorMap;
          const accountId = map.get(row.parentLookupValue);
          if (accountId) {
            record.ParentId = accountId;
            record.WhatId = accountId;
          } else {
            missingParentCount++;
          }
        }

        const categoryId = categoryMap.get(row.categoryName);
        if (categoryId) {
          record[CATEGORY_LOOKUP_FIELD] = categoryId;
        } else {
          missingCategoryCount++;
        }

        return record;
      });

      if (missingParentCount > 0) {
        logger.warn(
          `[Alert] ${missingParentCount} row(s) had a Student/Associate/Instructor ` +
            `ID with no matching PersonAccount — ParentId/WhatId left blank.`,
        );
      }
      if (missingCategoryCount > 0) {
        logger.warn(
          `[Alert] ${missingCategoryCount} row(s) had a Category with no matching ` +
            `RecordAlertCategory record (checked target org for "${CATEGORY_LOOKUP_FIELD}") — ` +
            `left blank.`,
        );
      }

      const jobResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "RecordAlert",
        EXTERNAL_ID_FIELD,
        finalRecords as Record<string, unknown>[],
        logger,
        "[Alert]",
      );

      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Alert Import",
          objectName: "RecordAlert",
          contacts: finalRecords as Record<string, unknown>[],
          externalIdField: EXTERNAL_ID_FIELD,
          failedExternalIds: jobResult.failedExternalIds,
          successfulCsv: jobResult.successfulCsv,
          failedCsv: jobResult.failedCsv,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: sheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Alert] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(`[Alert] Could not update results sheet: ${String(err)}`);
      }
    } else {
      logger.info(`[Alert] Window contained no records; skipping upload.`);
    }

    if (hasMore) {
      logger.info(
        `[Alert] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Alert Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(`[Alert] All rows processed — import complete.`);
    }

    return {
      data: {
        byteOffset,
        rowsProcessed: finalRecords.length,
        hasMore,
      },
    };
  },
});

export default [alertImport];
