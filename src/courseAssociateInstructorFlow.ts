/**
 * Stanford CSP Migration – Course Associate / Instructor Staff Detail flows.
 *
 * Two flows in this file:
 *   1. Course Instructor Staff Detail — reads Course_Instructor TSV, creates
 *      Course_Offering_Staff_Detail__c records for instructors.
 *      External ID prefix: "CINST-SD-"
 *   2. Course Associate Staff Detail — reads Course_Associate TSV, creates
 *      Course_Offering_Staff_Detail__c records for associates.
 *      External ID prefix: "CASSOC-SD-"
 *
 * Each flow streams the source TSV in MAX_ROWS-row windows via HTTP Range headers,
 * maps rows to Staff Detail records, and bulk-upserts via Bulk API 2.0.
 * Recurses via context.invokeFlow until the full file is processed.
 *
 * Lookups:
 *   Course_Offering__r.External_ID_4D__c — links to CourseOffering
 *   Staff_Member__r.External_ID_4D__c    — links to Contact (person{ID})
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import {
  str,
  toDate,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
  loadCourseRecIdMap,
  queryExistingExternalIds,
} from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 500;

const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 2;

// 4D quarter number format: {YYYY}{digit} — fa=1, wi=2, sp=3, su=4
// e.g. wi25→20252, su25→20254, fa24→20241
function buildValidCourseIdPrefixes(
  anchor: string,
  yearsBack: number,
): Set<string> {
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

const VALID_COURSE_ID_PREFIXES = buildValidCourseIdPrefixes(
  ANCHOR_QUARTER,
  YEARS_BACK,
);

// ── Raw TSV shapes ─────────────────────────────────────────────────────────────

interface RawCourseInstructorRecord {
  ID?: string;
  Instructor_ID?: string;
  Course_ID?: string;
  Estimated_Comp?: string;
  Salary_Category?: string;
  Gross_Pay?: string;
  Hire_Date?: string;
  Instructor_Term_Date?: string;
  Hi_Enroll_Bonus?: string;
  Cont_Hourly_Rate?: string;
  Cont_Estimated_Hours?: string;
  Contract_Template?: string;
  Notes?: string;
  Offer_Letter_Received?: string;
  Job_Record_Number?: string;
  Classroom_Hours?: string;
  True_Up_Bonus?: string;
  [key: string]: string | undefined;
}

interface RawCourseAssociateRecord {
  ID?: string;
  Associate_ID?: string;
  // Course_RecID is the FK to CourseOffering for associates.
  // Open question from architect: confirm Course_RecID maps to CourseOffering.External_ID_4D__c.
  Course_RecID?: string;
  Speaking_Date?: string;
  Estimated_Comp?: string;
  Salary_Category?: string;
  Gross_Pay?: string;
  Hire_Date?: string;
  Instructor_Term_Date?: string;
  Hi_Enroll_Bonus?: string; // all 0 for CA — consolidated to Hi_Enroll_Bonus__c per Holly 7/10
  True_Up_Bonus?: string;
  Cont_Hourly_Rate?: string;
  Cont_Estimated_Hours?: string;
  Contract_Template?: string;
  Notes?: string;
  Offer_Letter_Received?: string;
  Job_Record_Number?: string;
  [key: string]: string | undefined;
}

// ── Salesforce Staff Detail shape ─────────────────────────────────────────────

interface StaffDetailRecord {
  External_ID_4D__c: string;
  "Course_Offering__r.External_ID_4D__c"?: string;
  "Staff_Member__r.Instructor_ID_4D__c"?: string;
  "Staff_Member__r.Associate_ID_4D__c"?: string;
  Estimated_Comp__c?: number | string;
  Salary_Category__c?: string;
  Gross_Pay__c?: number | string;
  Hire_Date__c?: string;
  Term_Date__c?: string;
  Notes__c?: string;
  Offer_Letter_Received__c?: string;
  Job_Record_Number__c?: string;
  // Instructor only
  Hi_Enroll_Bonus__c?: number | string;
  Classroom_Hours__c?: number | string;
  // Associate only
  // Additional_Comp__c retired — consolidated into Hi_Enroll_Bonus__c per Holly 7/10
  Speaking_Date__c?: string;
  // Both CI and CA
  Additional_Comp_Other__c?: number | string;
  "Person_Employment__r.External_ID_4D__c"?: string;
  // SA Decision fields (both CI and CA)
  Cont_Hourly_Rate__c?: number | string;
  Cont_Estimated_Hours__c?: number | string;
  Contract_Note__c?: string;
}

interface StreamResult<T> {
  records: Partial<StaffDetailRecord>[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
  _raw?: T; // unused, keeps generic param referenced
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function parseNum(v: string | undefined): number | string {
  const n = parseFloat(str(v));
  return isNaN(n) ? "" : n;
}

function setDate(
  record: Partial<StaffDetailRecord>,
  key: keyof StaffDetailRecord,
  v: string | undefined,
) {
  const d = toDate(v);
  if (d) (record as Record<string, unknown>)[key] = d;
}

// ── Field mappings ─────────────────────────────────────────────────────────────

function mapInstructorToStaffDetail(
  raw: RawCourseInstructorRecord,
): Partial<StaffDetailRecord> | null {
  const id = str(raw.ID);
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  const record: Partial<StaffDetailRecord> = {
    External_ID_4D__c: `CINST-SD-${id}`,
  };

  const courseId = str(raw.Course_ID);
  if (courseId) {
    const prefix = courseId.split("_")[0];
    if (!VALID_COURSE_ID_PREFIXES.has(prefix)) return null;
    record["Course_Offering__r.External_ID_4D__c"] = courseId;
  }

  const instructorId = str(raw.Instructor_ID);
  if (instructorId)
    record["Staff_Member__r.Instructor_ID_4D__c"] = instructorId;

  record.Estimated_Comp__c = parseNum(raw.Estimated_Comp);
  record.Gross_Pay__c = parseNum(raw.Gross_Pay);
  record.Hi_Enroll_Bonus__c = parseNum(raw.Hi_Enroll_Bonus);
  record.Classroom_Hours__c = parseNum(raw.Classroom_Hours);
  record.Cont_Hourly_Rate__c = parseNum(raw.Cont_Hourly_Rate);
  record.Cont_Estimated_Hours__c = parseNum(raw.Cont_Estimated_Hours);

  const salaryCategory = str(raw.Salary_Category).replace(
    /^Contigent$/i,
    "Cont",
  );
  if (salaryCategory) record.Salary_Category__c = salaryCategory;

  record.Additional_Comp_Other__c = parseNum(raw.True_Up_Bonus);

  const contractTemplate = str(raw.Contract_Template).trim();
  if (contractTemplate) record.Contract_Note__c = contractTemplate;

  setDate(record, "Hire_Date__c", raw.Hire_Date);
  setDate(record, "Term_Date__c", raw.Instructor_Term_Date);
  setDate(record, "Offer_Letter_Received__c", raw.Offer_Letter_Received);

  // Notes__c is a Long Text field — no truncation needed.
  const notes = str(raw.Notes)
    .replace(/_4DNL_/g, "\n")
    .trim();
  if (notes) record.Notes__c = notes;

  const jobRecordNum = str(raw.Job_Record_Number);
  if (jobRecordNum) record.Job_Record_Number__c = jobRecordNum;

  if (instructorId)
    record["Person_Employment__r.External_ID_4D__c"] =
      `instructor_${instructorId}`;

  return record;
}

function mapAssociateToStaffDetail(
  raw: RawCourseAssociateRecord,
  courseRecIdMap?: Map<string, string>,
  missingRecIds?: Set<string>,
): Partial<StaffDetailRecord> | null {
  const id = str(raw.ID);
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  const record: Partial<StaffDetailRecord> = {
    External_ID_4D__c: `CASSOC-SD-${id}`,
  };

  const courseRecId = str(raw.Course_RecID).trim();
  if (courseRecId) {
    const courseId = courseRecIdMap?.get(courseRecId);
    if (courseId) {
      const prefix = courseId.split("_")[0];
      if (!VALID_COURSE_ID_PREFIXES.has(prefix)) return null;
      record["Course_Offering__r.External_ID_4D__c"] = courseId;
    } else {
      missingRecIds?.add(courseRecId);
    }
  }

  const associateId = str(raw.Associate_ID);
  if (associateId) record["Staff_Member__r.Associate_ID_4D__c"] = associateId;

  record.Estimated_Comp__c = parseNum(raw.Estimated_Comp);
  record.Gross_Pay__c = parseNum(raw.Gross_Pay);
  record.Hi_Enroll_Bonus__c = parseNum(raw.Hi_Enroll_Bonus); // consolidated from Additional_Comp__c per Holly 7/10
  record.Additional_Comp_Other__c = parseNum(raw.True_Up_Bonus);
  record.Cont_Hourly_Rate__c = parseNum(raw.Cont_Hourly_Rate);
  record.Cont_Estimated_Hours__c = parseNum(raw.Cont_Estimated_Hours);

  const salaryCategory = str(raw.Salary_Category).replace(
    /^Contigent$/i,
    "Cont",
  );
  if (salaryCategory) record.Salary_Category__c = salaryCategory;

  const contractTemplate = str(raw.Contract_Template).trim();
  if (contractTemplate) record.Contract_Note__c = contractTemplate;

  setDate(record, "Speaking_Date__c", raw.Speaking_Date);
  setDate(record, "Hire_Date__c", raw.Hire_Date);
  setDate(record, "Term_Date__c", raw.Instructor_Term_Date);
  setDate(record, "Offer_Letter_Received__c", raw.Offer_Letter_Received);

  // Notes__c is a Long Text field — no truncation needed.
  const notes = str(raw.Notes)
    .replace(/_4DNL_/g, "\n")
    .trim();
  if (notes) record.Notes__c = notes;

  const jobRecordNum = str(raw.Job_Record_Number);
  if (jobRecordNum) record.Job_Record_Number__c = jobRecordNum;

  if (associateId)
    record["Person_Employment__r.External_ID_4D__c"] =
      `associate_${associateId}`;

  return record;
}

// ── Generic TSV streaming + parsing ───────────────────────────────────────────

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  mapRow: (raw: Record<string, string>) => Partial<StaffDetailRecord> | null,
): Promise<StreamResult<unknown>> {
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
    const records: Partial<StaffDetailRecord>[] = [];
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

        const mapped = mapRow(raw);
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

// ── Shared flow execution logic ────────────────────────────────────────────────

async function runStaffDetailFlow(
  context: Parameters<Parameters<typeof flow>[0]["onExecution"]>[0],
  params: Parameters<Parameters<typeof flow>[0]["onExecution"]>[1],
  opts: {
    flowLabel: string;
    fileIdConfigVar: string;
    flowName: string;
    mapRow: (raw: Record<string, string>) => Partial<StaffDetailRecord> | null;
  },
) {
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
    typeof triggerBody?.sheetId === "string" ? triggerBody.sheetId : undefined;

  logger.info(`[${opts.flowLabel}] Starting at byte offset ${byteOffset}`);

  const gdConn = configVars["Google Drive Connection"] as Connection;
  const sfConn = configVars["Salesforce Connection"] as Connection;
  const fileId = (configVars as Record<string, unknown>)[
    opts.fileIdConfigVar
  ] as string;
  const failedFolderId = configVars["Failed Records Folder ID"] as
    string | undefined;

  if (!fileId) throw new Error(`${opts.fileIdConfigVar} config var is empty.`);

  const gdToken = getAccessToken(gdConn);
  const sfToken = getAccessToken(sfConn);
  const sfInstanceUrl = getSfInstanceUrl(sfConn);

  logger.info(
    `[${opts.flowLabel}] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
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
    opts.mapRow,
  );

  logger.info(
    `[${opts.flowLabel}] Parsed ${records.length} records` +
      ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
      ` firstId=${firstId}, lastId=${lastId})`,
  );

  // PDF: "only if PE record exists. If no matching PE record, leave null."
  // An external-ID relationship pointing at a non-existent record fails the
  // whole upsert row, so verify existence first and drop the lookup rather
  // than send an unresolvable reference.
  const peCandidates = [
    ...new Set(
      records
        .map((r) => r["Person_Employment__r.External_ID_4D__c"])
        .filter((v): v is string => Boolean(v)),
    ),
  ];
  if (peCandidates.length > 0) {
    const existingPe = await queryExistingExternalIds(
      sfInstanceUrl,
      sfToken,
      "PersonEmployment",
      "External_ID_4D__c",
      peCandidates,
    );
    let droppedPe = 0;
    for (const r of records) {
      const peId = r["Person_Employment__r.External_ID_4D__c"];
      if (peId && !existingPe.has(peId)) {
        delete r["Person_Employment__r.External_ID_4D__c"];
        droppedPe++;
      }
    }
    if (droppedPe > 0) {
      logger.info(
        `[${opts.flowLabel}] ${droppedPe} row(s) had no matching PersonEmployment — lookup left null.`,
      );
    }
  }

  let nextSheetId = sheetId;

  if (records.length > 0) {
    const jobResult = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "Course_Offering_Staff_Detail__c",
      "External_ID_4D__c",
      records as Record<string, unknown>[],
      logger,
      `[${opts.flowLabel}]`,
    );

    try {
      const sheet = await createResultsSheetFromContacts({
        flowName: opts.flowName,
        objectName: "Course_Offering_Staff_Detail__c",
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
      logger.info(`[${opts.flowLabel}] Results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(
        `[${opts.flowLabel}] Could not update results sheet: ${String(err)}`,
      );
    }
  } else {
    logger.info(
      `[${opts.flowLabel}] Window contained no records; skipping upload.`,
    );
  }

  if (hasMore) {
    logger.info(
      `[${opts.flowLabel}] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
    );
    await (
      context as unknown as {
        invokeFlow(name: string, payload: unknown): Promise<void>;
      }
    ).invokeFlow(opts.flowName, {
      byteOffset: nextByteOffset,
      headers: parsedHeaders,
      windowNumber: windowNumber + 1,
      sheetId: nextSheetId,
    });
  } else {
    logger.info(`[${opts.flowLabel}] All rows processed — import complete.`);
  }

  return {
    data: {
      byteOffset,
      rowsProcessed: records.length,
      hasMore,
    },
  };
}

// ── Flow 1: Course Instructor Staff Detail ─────────────────────────────────────

export const courseInstructorStaffDetail = flow({
  name: "Course Instructor Staff Detail",
  stableKey: "c1d2e3f4-aaaa-4c1d-8e2f-112233445566",
  description:
    "Reads the Course_Instructor TSV from Google Drive, maps each row to a " +
    "Course_Offering_Staff_Detail__c record (prefix CINST-SD-), and upserts " +
    "via Bulk API 2.0. Recurses until the full file is processed.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, params) =>
    runStaffDetailFlow(context, params, {
      flowLabel: "Course Instructor SD",
      fileIdConfigVar: "Course Instructor File ID",
      flowName: "Course Instructor Staff Detail",
      mapRow: (raw) =>
        mapInstructorToStaffDetail(raw as RawCourseInstructorRecord),
    }),
});

// ── Flow 2: Course Associate Staff Detail ──────────────────────────────────────

export const courseAssociateStaffDetail = flow({
  name: "Course Associate Staff Detail",
  stableKey: "a2b3c4d5-bbbb-4a2b-9c3d-223344556677",
  description:
    "Reads the Course_Associate TSV from Google Drive, maps each row to a " +
    "Course_Offering_Staff_Detail__c record (prefix CASSOC-SD-), and upserts " +
    "via Bulk API 2.0. Recurses until the full file is processed.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, params) => {
    const { logger } = context;
    const gdConn = context.configVars["Google Drive Connection"] as Connection;
    const gdToken = getAccessToken(gdConn);
    const courseFileId = (context.configVars as Record<string, unknown>)[
      "Course File ID"
    ] as string;

    if (!courseFileId) {
      throw new Error(
        "Course File ID config var is required for Course Associate Staff Detail.",
      );
    }

    const courseRecIdMap = await loadCourseRecIdMap(courseFileId, gdToken);
    logger.info(
      `[Course Associate SD] Loaded ${courseRecIdMap.size} course RecID→ID mappings`,
    );

    const missingRecIds = new Set<string>();

    const result = await runStaffDetailFlow(context, params, {
      flowLabel: "Course Associate SD",
      fileIdConfigVar: "Course Associate File ID",
      flowName: "Course Associate Staff Detail",
      mapRow: (raw) =>
        mapAssociateToStaffDetail(
          raw as RawCourseAssociateRecord,
          courseRecIdMap,
          missingRecIds,
        ),
    });

    if (missingRecIds.size > 0) {
      logger.warn(
        `[Course Associate SD] ${missingRecIds.size} Course_RecID(s) not found in crosswalk: ${[...missingRecIds].join(", ")}`,
      );
    }

    return result;
  },
});

export default [courseInstructorStaffDetail, courseAssociateStaffDetail];
