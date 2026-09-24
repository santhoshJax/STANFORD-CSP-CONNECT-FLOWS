/**
 * Stanford CSP Migration – Course Filter by Quarter flow.
 *
 * Streams the course TSV from Google Drive, filters to rows whose Quarter
 * field falls within the 8 valid academic quarters (wi24–fa25), and maps
 * all 32 specified fields to Salesforce.
 *
 * Valid quarters: wi24, sp24, su24, fa24, wi25, sp25, su25, fa25
 * (generated dynamically from ANCHOR_QUARTER + YEARS_BACK)
 *
 * All dependent objects (Learning, LearningCourse, CourseOffering,
 * CourseOfferingSchedule, COP) derive from filtered course rows, so
 * filtering the course TSV is sufficient. Junction files
 * (Course_Instructor, Course_Department) are loaded in full but only
 * queried for course IDs that survive the quarter filter.
 *
 * Processing order per row:
 *   Learning → LearningCourse → Instructor → CourseOffering
 *   → CourseOfferingSchedule → COP (Instructor / Associate)
 *
 * Recurses via context.invokeFlow until the full file is processed.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse, unparse as papaUnparse } from "papaparse";
import { str, getAccessToken, getSfInstanceUrl } from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";
import {
  parseCourseDate,
  normaliseEnrollmentStatus,
  normaliseCatalogNotes,
  parseBool,
  normaliseFormat,
  parseWeekdays,
  parseCourseTime,
  parseIntVal,
  parseFloatVal,
  stripSectionSuffix,
  extractSectionSuffix,
  formatFromSuffix,
} from "./courseUtils";

// ── Constants ─────────────────────────────────────────────────────────────────

const MAX_ROWS = 50;
const TEST_MODE = false;
const TEST_MAX_ROWS = 100;
const SF_API = "v60.0";

// Quarter filter: include all {season}{year} combos for the last YEARS_BACK
// academic years ending at ANCHOR_QUARTER's year.
// e.g. ANCHOR_QUARTER="wi25", YEARS_BACK=2 → wi24,sp24,su24,fa24,wi25,sp25,su25,fa25
// e.g. ANCHOR_QUARTER="wi25", YEARS_BACK=5 → wi21,sp21,su21,fa21,...,wi25,sp25,su25,fa25
const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 5;

// ── Quarter filter helper ─────────────────────────────────────────────────────

function buildValidQuarters(anchor: string, yearsBack: number): Set<string> {
  const seasons = ["wi", "sp", "su", "fa"];
  const m = anchor.toLowerCase().match(/^([a-z]+)(\d+)$/);
  if (!m) throw new Error(`Invalid ANCHOR_QUARTER: "${anchor}"`);
  const anchorYear = parseInt(m[2], 10);
  const set = new Set<string>();
  for (let y = anchorYear - yearsBack + 1; y <= anchorYear; y++) {
    const yStr = String(y).slice(-2).padStart(2, "0");
    for (const s of seasons) set.add(`${s}${yStr}`);
  }
  return set;
}

// ── Salesforce REST helpers ───────────────────────────────────────────────────

function authHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}

function readHeaders(token: string): Record<string, string> {
  return { Authorization: `Bearer ${token}` };
}

async function sfUpsert(
  base: string,
  token: string,
  object: string,
  extIdField: string,
  extIdValue: string,
  payload: Record<string, unknown>,
): Promise<{ id: string; created: boolean }> {
  const url = `${base}/services/data/${SF_API}/sobjects/${object}/${extIdField}/${encodeURIComponent(extIdValue)}`;
  const { data, status } = await axios.patch<{
    id?: string;
    created?: boolean;
  }>(url, payload, {
    headers: authHeaders(token),
    validateStatus: (s) => s < 500,
  });
  if (status === 201) return { id: data.id!, created: true };
  if (status === 200) return { id: data.id!, created: data.created ?? false };
  if (status === 204) {
    const { data: rec } = await axios.get<{ Id: string }>(
      `${base}/services/data/${SF_API}/sobjects/${object}/${extIdField}/${encodeURIComponent(extIdValue)}`,
      { params: { fields: "Id" }, headers: readHeaders(token) },
    );
    return { id: rec.Id, created: false };
  }
  throw new Error(`Upsert ${object} HTTP ${status}: ${JSON.stringify(data)}`);
}

async function sfCreate(
  base: string,
  token: string,
  object: string,
  payload: Record<string, unknown>,
): Promise<string> {
  const { data } = await axios.post<{ id: string }>(
    `${base}/services/data/${SF_API}/sobjects/${object}`,
    payload,
    { headers: authHeaders(token) },
  );
  return data.id;
}

async function sfUpdate(
  base: string,
  token: string,
  object: string,
  id: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await axios.patch(
    `${base}/services/data/${SF_API}/sobjects/${object}/${id}`,
    payload,
    { headers: authHeaders(token) },
  );
}

async function sfQuery<T>(
  base: string,
  token: string,
  soql: string,
): Promise<T[]> {
  const { data } = await axios.get<{ records: T[] }>(
    `${base}/services/data/${SF_API}/query`,
    { params: { q: soql }, headers: readHeaders(token) },
  );
  return data.records;
}

// ── Cache builder (handles SOQL pagination) ───────────────────────────────────

async function buildFieldCache(
  base: string,
  token: string,
  soql: string,
  keyField: string,
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  let nextUrl: string | null = null;
  let done = false;

  const fetchPage = async (url: string | null) => {
    const { data } = await axios.get<{
      records: Record<string, string>[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${base}/services/data/${SF_API}/query`,
      url
        ? { headers: readHeaders(token) }
        : { params: { q: soql }, headers: readHeaders(token) },
    );
    for (const r of data.records) {
      if (r[keyField]) map.set(r[keyField], r.Id);
    }
    done = data.done;
    nextUrl = data.nextRecordsUrl ? `${base}${data.nextRecordsUrl}` : null;
  };

  await fetchPage(null);
  while (!done && nextUrl) await fetchPage(nextUrl);
  return map;
}

// ── Person resolvers ──────────────────────────────────────────────────────────

async function resolvePersonByName(
  base: string,
  token: string,
  name: string,
  cache: Map<string, string | null>,
): Promise<string | null> {
  const n = name.trim();
  if (!n) return null;
  if (cache.has(n)) return cache.get(n)!;
  const esc = n.replace(/'/g, "\\'");
  const rows = await sfQuery<{ PersonContactId: string }>(
    base,
    token,
    `SELECT PersonContactId FROM Account WHERE IsPersonAccount = true AND Name = '${esc}' LIMIT 1`,
  );
  const id = rows[0]?.PersonContactId ?? null;
  cache.set(n, id);
  return id;
}

async function resolvePersonByExtId(
  base: string,
  token: string,
  extId: string,
  cache: Map<string, string | null>,
): Promise<string | null> {
  const e = extId.trim();
  if (!e || e === "0") return null;
  if (cache.has(e)) return cache.get(e)!;
  const rows = await sfQuery<{ PersonContactId: string }>(
    base,
    token,
    `SELECT PersonContactId FROM Account WHERE IsPersonAccount = true AND External_ID_4D__c = '${e.replace(/'/g, "\\'")}' LIMIT 1`,
  );
  const id = rows[0]?.PersonContactId ?? null;
  cache.set(e, id);
  return id;
}

// ── Course-Instructor junction ────────────────────────────────────────────────

interface CourseInstructorRow {
  Course_ID?: string;
  Instructor_ID?: string;
  IsPrimary?: string;
  Do_Not_Show_On_Web?: string;
  Gross_Pay?: string;
  Cont_Hourly_Rate?: string;
  Cont_Estimated_Hours?: string;
  Hire_Date?: string;
  Instructor_Term_Date?: string;
  Contract_Template?: string;
  Salary_Category?: string;
  Classroom_Hours?: string;
  Notes?: string;
}

async function loadCourseInstructors(
  fileId: string,
  token: string,
): Promise<Map<string, CourseInstructorRow[]>> {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`;
  const { data } = await axios.get<string>(url, {
    headers: { Authorization: `Bearer ${token}` },
    responseType: "text",
  });
  const content = data.replace(/^﻿/, "");
  const { data: rows } = papaParse<CourseInstructorRow>(content, {
    header: true,
    skipEmptyLines: true,
    delimiter: "\t",
  });
  const map = new Map<string, CourseInstructorRow[]>();
  for (const row of rows) {
    const cid = row.Course_ID?.trim();
    if (!cid) continue;
    if (!map.has(cid)) map.set(cid, []);
    map.get(cid)!.push(row);
  }
  return map;
}

// ── Raw row type ──────────────────────────────────────────────────────────────

interface RawCourseRow {
  id?: string;
  Code?: string;
  Title?: string;
  Quarter?: string;
  Start_Date?: string;
  End_Date?: string;
  Enrollment_Count?: string;
  Max_Enrollment?: string;
  Duration?: string;
  Catalog_Notes?: string;
  Description?: string;
  Units?: string;
  Tuition?: string;
  Weekday?: string;
  Course_Time?: string;
  Additional_Fee?: string;
  Limited_Enrollment?: string;
  Cancelled?: string;
  Exception_Text?: string;
  Primary_Instructor?: string;
  Primary_Associate?: string;
  Dropped_Special_Percent?: string;
  Global_Eval?: string;
  Return_Rate?: string;
  Enroll_On_Start_Date?: string;
  Format?: string;
  Coordinator_ID?: string;
  Grade_Restriction?: string;
  Course_Version?: string;
  Recording?: string;
  Instructor_Preferences?: string;
  Staff_Notes?: string;
  Last_Modified_Date?: string;
  Hybrid?: string;
  [key: string]: string | undefined;
}

// ── Result rows ───────────────────────────────────────────────────────────────

interface SuccessRow {
  Source_ID: string;
  Code: string;
  Title: string;
  Quarter: string;
  Start_Date: string;
  End_Date: string;
  Primary_Instructor: string;
  Primary_Associate: string;
  Format: string;
  Max_Enrollment: string;
  Cancelled: string;
  Object: string;
  sf__Id: string;
  sf__Created: string;
}

interface ErrorRow {
  Source_ID: string;
  Code: string;
  Title: string;
  Quarter: string;
  Start_Date: string;
  End_Date: string;
  Primary_Instructor: string;
  Primary_Associate: string;
  Format: string;
  Max_Enrollment: string;
  Cancelled: string;
  Object: string;
  sf__Error: string;
}

// ── TSV streaming with Quarter filter ────────────────────────────────────────

interface StreamResult {
  rows: RawCourseRow[];
  hasMore: boolean;
  nextStartRow: number;
  skippedByQuarter: number;
  unrecognisedQuarters: Map<string, number>; // quarter value → count of skipped rows
}

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  startRow: number,
  maxRows: number,
  validQuarters: Set<string>,
): Promise<StreamResult> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    let headers: string[] = [];
    let idField = "id";
    let dataRowIndex = 0;
    const rows: RawCourseRow[] = [];
    let skippedByQuarter = 0;
    const unrecognisedQuarters = new Map<string, number>();
    let aborted = false;

    papaParse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      skipEmptyLines: true,

      step: (result: Papa.ParseResult<string[]>, parser: Papa.Parser) => {
        if (aborted) return;
        const raw = result.data as unknown as string[];

        if (headers.length === 0) {
          headers = raw.map(
            (h, i) =>
              h.replace(/^﻿/, "").replace(/\r/g, "").trim() || `__blank_${i}`,
          );
          idField =
            headers.find((h) => h.replace(/^﻿/, "").toLowerCase() === "id") ??
            "id";
          return;
        }

        if (dataRowIndex < startRow) {
          dataRowIndex++;
          return;
        }

        if (rows.length >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        const record: RawCourseRow = {};
        headers.forEach((header, i) => {
          if (!header.startsWith("__blank_")) {
            record[header] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        if (idField !== "id" && record[idField] !== undefined) {
          record.id = record[idField];
        }

        if (!(record.id ?? "").trim()) {
          dataRowIndex++;
          return;
        }

        // Skip rows whose Quarter is not in the valid set
        const quarterNorm = (record.Quarter ?? "").trim().toLowerCase();
        if (!validQuarters.has(quarterNorm)) {
          skippedByQuarter++;
          const label = quarterNorm || "(blank)";
          unrecognisedQuarters.set(
            label,
            (unrecognisedQuarters.get(label) ?? 0) + 1,
          );
          dataRowIndex++;
          return;
        }

        rows.push(record);
        dataRowIndex++;
      },

      complete: () =>
        resolve({
          rows,
          hasMore: aborted,
          nextStartRow: dataRowIndex,
          skippedByQuarter,
          unrecognisedQuarters,
        }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Error message helper ──────────────────────────────────────────────────────

function sfErrMsg(err: unknown): string {
  const e = err as {
    response?: { status?: number; data?: unknown };
    message?: string;
  };
  if (e.response?.data) {
    const d = e.response.data;
    if (Array.isArray(d)) {
      return (
        `HTTP ${e.response.status} — ` +
        (d as { errorCode?: string; message?: string }[])
          .map((r) => [r.errorCode, r.message].filter(Boolean).join(": "))
          .join("; ")
      );
    }
    return `HTTP ${e.response.status} — ${JSON.stringify(d)}`;
  }
  return e.message ?? String(err);
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const coursefilterbyquarterImport = flow({
  name: "coursefilterbyquarter",
  stableKey: "e2f3a4b5-5c6d-4e7f-8a9b-001122334455",
  description:
    "Streams the course TSV from Google Drive, filters to the 8 valid academic " +
    "quarters (wi24–fa25), and maps all 32 specified fields per row to Salesforce.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    const triggerBody = (
      params.onTrigger.results as unknown as
        | { body?: { data?: unknown } }
        | undefined
    )?.body?.data as Record<string, unknown> | undefined;

    const startRow =
      typeof triggerBody?.startRow === "number" ? triggerBody.startRow : 0;
    const courseSheetId =
      typeof triggerBody?.courseSheetId === "string"
        ? triggerBody.courseSheetId
        : undefined;

    // Build valid quarter set once per execution
    const validQuarters = buildValidQuarters(ANCHOR_QUARTER, YEARS_BACK);
    logger.info(
      `[CourseFilter] Starting at row ${startRow} — valid quarters: ${[...validQuarters].join(", ")}`,
    );

    const gdConn = configVars[
      "Google Drive Connection"
    ] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Course File ID"] as unknown as string;
    const instructorFileId = configVars[
      "Course Instructor File ID"
    ] as unknown as string | undefined;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Course File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── Pre-loop caches ───────────────────────────────────────────────────────
    logger.info("[CourseFilter] Building lookup caches…");
    const [sessionCache, learningCourseCache] = await Promise.all([
      buildFieldCache(
        sfBase,
        sfToken,
        "SELECT Id, Abbreviation__c FROM AcademicSession WHERE Abbreviation__c != null",
        "Abbreviation__c",
      ),
      buildFieldCache(
        sfBase,
        sfToken,
        "SELECT Id, External_ID_4D__c FROM LearningCourse WHERE External_ID_4D__c != null",
        "External_ID_4D__c",
      ),
    ]);
    logger.info(
      `[CourseFilter] Caches — sessions=${sessionCache.size}, learningCourses=${learningCourseCache.size}`,
    );

    // ── Course-Instructor junction ────────────────────────────────────────────
    let courseInstructorMap = new Map<string, CourseInstructorRow[]>();
    if (instructorFileId) {
      try {
        courseInstructorMap = await loadCourseInstructors(
          instructorFileId,
          gdToken,
        );
        logger.info(
          `[CourseFilter] Course-Instructor junction loaded — ${courseInstructorMap.size} courses`,
        );
      } catch (err) {
        logger.warn(
          `[CourseFilter] Could not load Course-Instructor file: ${String(err)} — falling back to name lookup`,
        );
      }
    }

    const personByNameCache = new Map<string, string | null>();
    const personByExtIdCache = new Map<string, string | null>();

    // ── Stream TSV window ─────────────────────────────────────────────────────
    logger.info(
      `[CourseFilter] Streaming rows ${startRow}–${startRow + MAX_ROWS - 1}…`,
    );
    const {
      rows,
      hasMore,
      nextStartRow,
      skippedByQuarter,
      unrecognisedQuarters,
    } = await streamAndParseTsv(
      fileId,
      gdToken,
      startRow,
      MAX_ROWS,
      validQuarters,
    );
    logger.info(
      `[CourseFilter] Parsed ${rows.length} rows, skipped ${skippedByQuarter} (quarter not in filter) (hasMore=${hasMore})`,
    );
    if (unrecognisedQuarters.size > 0) {
      const detail = [...unrecognisedQuarters.entries()]
        .map(([q, n]) => `"${q}" ×${n}`)
        .join(", ");
      logger.warn(
        `[CourseFilter] WARNING — ${skippedByQuarter} rows skipped due to unrecognised/out-of-range Quarter values: ${detail}. ` +
          `Valid quarters are: ${[...validQuarters].join(", ")}`,
      );
    }

    // ── Counters ──────────────────────────────────────────────────────────────
    const counts = {
      learning: { ok: 0, err: 0 },
      learningCourse: { ok: 0, err: 0 },
      courseOffering: { ok: 0, err: 0 },
      schedule: { ok: 0, skipped: 0, err: 0 },
      copInstructor: { ok: 0, skipped: 0, err: 0 },
      copAssociate: { ok: 0, skipped: 0, err: 0 },
    };

    const successRows: SuccessRow[] = [];
    const errorRows: ErrorRow[] = [];

    // ── Row loop ──────────────────────────────────────────────────────────────
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const sourceId = str(row.id);
      const code = str(row.Code);
      const baseCode = stripSectionSuffix(code) || code;
      const sectionSuffix = extractSectionSuffix(code);
      const title = str(row.Title);
      const quarter = str(row.Quarter);
      // Human-readable row identifier used in every log/warn/error message
      const rowCtx = `row ${startRow + i} | id=${sourceId} | code=${code} | quarter=${quarter}`;

      // Common source fields shared by every success/error row for this course
      const rowMeta = {
        Source_ID: sourceId,
        Code: code,
        Title: title,
        Quarter: quarter,
        Start_Date: str(row.Start_Date),
        End_Date: str(row.End_Date),
        Primary_Instructor: str(row.Primary_Instructor),
        Primary_Associate: str(row.Primary_Associate),
        Format: str(row.Format),
        Max_Enrollment: str(row.Max_Enrollment),
        Cancelled: str(row.Cancelled),
      };

      const ok = (object: string, sfId: string, created: boolean) =>
        successRows.push({
          ...rowMeta,
          Object: object,
          sf__Id: sfId,
          sf__Created: String(created),
        });

      const fail = (object: string, error: string) =>
        errorRows.push({
          ...rowMeta,
          Object: object,
          sf__Error: error,
        });

      // 1. Learning
      let learningId: string | null = null;
      try {
        const r = await sfUpsert(
          sfBase,
          sfToken,
          "Learning",
          "External_ID_4D__c",
          baseCode,
          {
            Name: (title || baseCode).slice(0, 255),
            Type: "LearningCourse",
            IsActive: true,
          },
        );
        learningId = r.id;
        counts.learning.ok++;
        ok("Learning", r.id, r.created);
      } catch (err) {
        counts.learning.err++;
        const msg = sfErrMsg(err);
        logger.error(`[CourseFilter] ${rowCtx} Learning FAILED: ${msg}`);
        fail("Learning", msg);
        continue;
      }

      // 2. LearningCourse
      let learningCourseId: string | null = null;
      try {
        let existingLcId = learningCourseCache.get(baseCode) ?? null;
        if (!existingLcId && learningId) {
          const found = await sfQuery<{ Id: string }>(
            sfBase,
            sfToken,
            `SELECT Id FROM LearningCourse WHERE LearningId = '${learningId}' LIMIT 1`,
          );
          existingLcId = found[0]?.Id ?? null;
        }

        const lc: Record<string, unknown> = {
          Name: (title || baseCode).slice(0, 255),
          External_ID_4D__c: baseCode,
          CourseNumber: baseCode.replace(/\s+/g, ""),
        };
        const catalogNotes = normaliseCatalogNotes(row.Catalog_Notes);
        if (catalogNotes) lc.Catalog_Notes__c = catalogNotes;
        const description = normaliseCatalogNotes(row.Description);
        if (description) lc.Description = description.slice(0, 32000);
        const units = parseFloatVal(row.Units);
        if (units !== null) lc.Units__c = units;

        if (existingLcId) {
          await sfUpdate(sfBase, sfToken, "LearningCourse", existingLcId, lc);
          learningCourseId = existingLcId;
          learningCourseCache.set(baseCode, existingLcId);
          counts.learningCourse.ok++;
          ok("LearningCourse", existingLcId, false);
        } else {
          lc.LearningId = learningId;
          const newId = await sfCreate(sfBase, sfToken, "LearningCourse", lc);
          learningCourseId = newId;
          learningCourseCache.set(baseCode, newId);
          counts.learningCourse.ok++;
          ok("LearningCourse", newId, true);
        }
      } catch (err) {
        counts.learningCourse.err++;
        const msg = sfErrMsg(err);
        logger.error(`[CourseFilter] ${rowCtx} LearningCourse FAILED: ${msg}`);
        fail("LearningCourse", msg);
      }

      // 3. Primary Instructor — resolve via Course-Instructor junction
      const junctionRows = (courseInstructorMap.get(sourceId) ?? []).sort(
        (a, b) =>
          (parseBool(b.IsPrimary) ? 1 : 0) - (parseBool(a.IsPrimary) ? 1 : 0),
      );

      let primaryInstructorId: string | null = null;

      if (junctionRows.length > 0) {
        const primaryRow =
          junctionRows.find((r) => parseBool(r.IsPrimary) === true) ??
          junctionRows[0];
        const instrExtId = primaryRow.Instructor_ID?.trim();
        if (instrExtId) {
          try {
            primaryInstructorId = await resolvePersonByExtId(
              sfBase,
              sfToken,
              `person${instrExtId}`,
              personByExtIdCache,
            );
            if (!primaryInstructorId)
              logger.warn(
                `[CourseFilter] ${rowCtx} WARNING — Primary instructor not found in Salesforce: Instructor_ID="${instrExtId}"`,
              );
          } catch (err) {
            logger.warn(
              `[CourseFilter] ${rowCtx} WARNING — Instructor lookup error: ${sfErrMsg(err)}`,
            );
          }
        }
      } else if (str(row.Primary_Instructor)) {
        try {
          primaryInstructorId = await resolvePersonByName(
            sfBase,
            sfToken,
            str(row.Primary_Instructor),
            personByNameCache,
          );
          if (!primaryInstructorId)
            logger.warn(
              `[CourseFilter] ${rowCtx} WARNING — Instructor not found by name: "${str(row.Primary_Instructor)}"`,
            );
        } catch (err) {
          logger.warn(
            `[CourseFilter] ${rowCtx} WARNING — Instructor name lookup error: ${sfErrMsg(err)}`,
          );
        }
      }

      // Resolve Coordinator_ID → Contact SF ID
      let coordId: string | null = null;
      const coordExtId = str(row.Coordinator_ID);
      if (coordExtId && coordExtId !== "0") {
        try {
          coordId = await resolvePersonByExtId(
            sfBase,
            sfToken,
            `person${coordExtId}`,
            personByExtIdCache,
          );
          if (!coordId)
            logger.warn(
              `[CourseFilter] ${rowCtx} WARNING — Coordinator not found: Coordinator_ID="${coordExtId}"`,
            );
        } catch (err) {
          logger.warn(
            `[CourseFilter] ${rowCtx} WARNING — Coordinator lookup error: ${sfErrMsg(err)}`,
          );
        }
      }

      // 4. CourseOffering
      let courseOfferingId: string | null = null;
      const isCancelled = parseBool(row.Cancelled) === true;
      const effectiveQuarter = (() => {
        const m = /^([a-z]+)(\d+)$/i.exec(quarter);
        if (m && parseInt(m[2], 10) < 13) return `${m[1].toLowerCase()}13`;
        return quarter;
      })();
      const sessionId = sessionCache.get(effectiveQuarter) ?? null;
      if (!sessionId && effectiveQuarter)
        logger.warn(
          `[CourseFilter] ${rowCtx} WARNING — AcademicSession not found for quarter "${effectiveQuarter}" — CourseOffering will have no session`,
        );

      try {
        const co: Record<string, unknown> = {
          Name: code || sourceId,
          ...(sectionSuffix ? { SectionNumber: sectionSuffix } : {}),
          Enrollment_Status__c: normaliseEnrollmentStatus(
            undefined,
            isCancelled,
          ),
        };
        if (learningCourseId) co.LearningCourseId = learningCourseId;
        if (sessionId) co.AcademicSessionId = sessionId;
        if (primaryInstructorId) co.PrimaryFacultyId = primaryInstructorId;
        if (coordId) co.Coordinator__c = coordId;

        const setDate = (field: string, v: string | undefined) => {
          const d = parseCourseDate(v);
          if (d) co[field] = d;
        };
        const setInt = (field: string, v: string | undefined) => {
          const n = parseIntVal(v);
          if (n !== null) co[field] = n;
        };
        const setFloat = (field: string, v: string | undefined) => {
          const n = parseFloatVal(v);
          if (n !== null) co[field] = n;
        };
        const setBool = (field: string, v: string | undefined) => {
          const b = parseBool(v);
          if (b !== null) co[field] = b;
        };
        const setStr = (field: string, v: string | undefined) => {
          const s = str(v);
          if (s) co[field] = s;
        };

        setDate("StartDate", row.Start_Date);
        setDate("EndDate", row.End_Date);
        setInt("EnrollmentCapacity", row.Max_Enrollment);
        setFloat("Additional_Fee__c", row.Additional_Fee);
        setFloat("Tuition__c", row.Tuition);
        const staffNotes = str(row.Staff_Notes);
        if (staffNotes) co.Staff_Notes__c = staffNotes.slice(0, 255);
        setBool("Limited_Enrollment__c", row.Limited_Enrollment);
        setStr("Exception_Text__c", row.Exception_Text);
        const droppedSpecialPct = parseFloatVal(row.Dropped_Special_Percent);
        if (droppedSpecialPct !== null && droppedSpecialPct < 100)
          co.Dropped_Special_Percent__c = droppedSpecialPct;
        setFloat("Global_Eval__c", row.Global_Eval);
        setFloat("Return_Rate__c", row.Return_Rate);
        setInt("Enroll_On_Start_Date__c", row.Enroll_On_Start_Date);
        const fmt =
          normaliseFormat(row.Format, row.Hybrid) ??
          formatFromSuffix(sectionSuffix);
        if (fmt) co.Format__c = fmt;
        setStr("Grade_Restriction__c", row.Grade_Restriction);
        setStr("Course_Version__c", row.Course_Version);
        setBool("Recording__c", row.Recording);
        setStr(
          "Textbook_Instructor_Preferences__c",
          row.Instructor_Preferences,
        );

        logger.info(
          `[CourseFilter] ${rowCtx} CourseOffering:` +
            `\n  LearningCourseId  = ${co.LearningCourseId ?? "NULL"}` +
            `\n  AcademicSessionId = ${co.AcademicSessionId ?? "NULL"} (quarter="${effectiveQuarter}")` +
            `\n  EnrollmentCapacity = ${co.EnrollmentCapacity ?? "NULL"}` +
            `\n  PrimaryFacultyId  = ${co.PrimaryFacultyId ?? "NULL"}`,
        );

        const r = await sfUpsert(
          sfBase,
          sfToken,
          "CourseOffering",
          "External_ID_4D__c",
          sourceId,
          co,
        );
        courseOfferingId = r.id;
        counts.courseOffering.ok++;
        ok("CourseOffering", r.id, r.created);
      } catch (err) {
        counts.courseOffering.err++;
        const msg = sfErrMsg(err);
        logger.error(`[CourseFilter] ${rowCtx} CourseOffering FAILED: ${msg}`);
        fail("CourseOffering", msg);
        continue;
      }

      // 5. CourseOfferingSchedule
      if (str(row.Weekday) || str(row.Course_Time)) {
        try {
          const existing = await sfQuery<{ Id: string }>(
            sfBase,
            sfToken,
            `SELECT Id FROM CourseOfferingSchedule WHERE CourseOfferingId = '${courseOfferingId}' LIMIT 1`,
          );
          if (existing.length === 0) {
            const dayFlags = parseWeekdays(row.Weekday);
            const { startTime, endTime } = parseCourseTime(row.Course_Time);
            const sched: Record<string, unknown> = {
              CourseOfferingId: courseOfferingId,
              Description: title || code || sourceId,
              ...dayFlags,
            };
            if (startTime) sched.StartTime = startTime;
            if (endTime && startTime && endTime > startTime)
              sched.EndTime = endTime;
            const schedId = await sfCreate(
              sfBase,
              sfToken,
              "CourseOfferingSchedule",
              sched,
            );
            counts.schedule.ok++;
            ok("CourseOfferingSchedule", schedId, true);
          } else {
            counts.schedule.skipped++;
          }
        } catch (err) {
          counts.schedule.err++;
          const msg = sfErrMsg(err);
          logger.warn(`[CourseFilter] ${rowCtx} Schedule FAILED: ${msg}`);
          fail("CourseOfferingSchedule", msg);
        }
      } else {
        counts.schedule.skipped++;
      }

      // 6. COP – Instructor
      const instructorRows =
        junctionRows.length > 0
          ? junctionRows
          : primaryInstructorId
            ? [
                {
                  Instructor_ID: undefined,
                  IsPrimary: "true",
                } as CourseInstructorRow,
              ]
            : [];

      for (const jRow of instructorRows) {
        const contactId = jRow.Instructor_ID?.trim()
          ? await resolvePersonByExtId(
              sfBase,
              sfToken,
              `person${jRow.Instructor_ID.trim()}`,
              personByExtIdCache,
            ).catch(() => null)
          : primaryInstructorId;

        if (!contactId) {
          const instrLabel = jRow.Instructor_ID?.trim()
            ? `Instructor_ID="${jRow.Instructor_ID.trim()}"`
            : `Primary_Instructor="${str(row.Primary_Instructor)}"`;
          const msg = `Instructor not found in Salesforce — ${instrLabel} — COP-Instructor skipped`;
          logger.warn(`[CourseFilter] ${rowCtx} WARNING — ${msg}`);
          fail("COP-Instructor", msg);
          counts.copInstructor.skipped++;
          continue;
        }
        try {
          const existing = await sfQuery<{ Id: string }>(
            sfBase,
            sfToken,
            `SELECT Id FROM CourseOfferingParticipant WHERE CourseOfferingId = '${courseOfferingId}' AND ParticipantContactId = '${contactId}' AND ParticipantAffiliation = 'Instructor' LIMIT 1`,
          );
          if (existing.length === 0) {
            const copId = await sfCreate(
              sfBase,
              sfToken,
              "CourseOfferingParticipant",
              {
                CourseOfferingId: courseOfferingId,
                ParticipantContactId: contactId,
                ParticipantAffiliation: "Instructor",
              },
            );
            counts.copInstructor.ok++;
            ok("COP-Instructor", copId, true);
          } else {
            counts.copInstructor.skipped++;
          }
        } catch (err) {
          counts.copInstructor.err++;
          const msg = sfErrMsg(err);
          logger.warn(`[CourseFilter] ${rowCtx} COP-Instructor FAILED: ${msg}`);
          fail("COP-Instructor", msg);
        }
      }

      // 7. COP – Associate
      const assocName = str(row.Primary_Associate);
      if (assocName && assocName !== "0") {
        try {
          const assocId = await resolvePersonByName(
            sfBase,
            sfToken,
            assocName,
            personByNameCache,
          );
          if (assocId) {
            const existing = await sfQuery<{ Id: string }>(
              sfBase,
              sfToken,
              `SELECT Id FROM CourseOfferingParticipant WHERE CourseOfferingId = '${courseOfferingId}' AND ParticipantContactId = '${assocId}' AND ParticipantAffiliation = 'Associate' LIMIT 1`,
            );
            if (existing.length === 0) {
              const copId = await sfCreate(
                sfBase,
                sfToken,
                "CourseOfferingParticipant",
                {
                  CourseOfferingId: courseOfferingId,
                  ParticipantContactId: assocId,
                  ParticipantAffiliation: "Associate",
                },
              );
              counts.copAssociate.ok++;
              ok("COP-Associate", copId, true);
            } else {
              counts.copAssociate.skipped++;
            }
          } else {
            logger.warn(
              `[CourseFilter] ${rowCtx} WARNING — Associate "${assocName}" not found in Salesforce — COP-Associate skipped`,
            );
            counts.copAssociate.skipped++;
            fail(
              "COP-Associate",
              `Associate "${assocName}" not found in Salesforce`,
            );
          }
        } catch (err) {
          counts.copAssociate.err++;
          const msg = sfErrMsg(err);
          logger.warn(`[CourseFilter] ${rowCtx} COP-Associate FAILED: ${msg}`);
          fail("COP-Associate", msg);
        }
      } else {
        counts.copAssociate.skipped++;
      }
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    const RESULT_OBJECTS = [
      "Learning",
      "LearningCourse",
      "CourseOffering",
      "CourseOfferingSchedule",
      "COP-Instructor",
      "COP-Associate",
    ] as const;

    let nextSheetId = courseSheetId;
    if (successRows.length > 0 || errorRows.length > 0) {
      try {
        const objectData = RESULT_OBJECTS.map((objName) => {
          const sRows = successRows
            .filter((r) => r.Object === objName)
            .map(
              ({
                Source_ID,
                Code,
                Title,
                Quarter,
                Start_Date,
                End_Date,
                Primary_Instructor,
                Primary_Associate,
                Format,
                Max_Enrollment,
                Cancelled,
                sf__Id,
                sf__Created,
              }) => ({
                Source_ID,
                Code,
                Title,
                Quarter,
                Start_Date,
                End_Date,
                Primary_Instructor,
                Primary_Associate,
                Format,
                Max_Enrollment,
                Cancelled,
                sf__Id,
                sf__Created,
              }),
            );
          const eRows = errorRows
            .filter((r) => r.Object === objName)
            .map(
              ({
                Source_ID,
                Code,
                Title,
                Quarter,
                Start_Date,
                End_Date,
                Primary_Instructor,
                Primary_Associate,
                Format,
                Max_Enrollment,
                Cancelled,
                sf__Error,
              }) => ({
                Source_ID,
                Code,
                Title,
                Quarter,
                Start_Date,
                End_Date,
                Primary_Instructor,
                Primary_Associate,
                Format,
                Max_Enrollment,
                Cancelled,
                sf__Error,
              }),
            );
          return {
            objectName: objName,
            successfulCsv:
              sRows.length > 0
                ? papaUnparse(sRows as unknown as Record<string, unknown>[], {
                    newline: "\n",
                  })
                : "",
            failedCsv:
              eRows.length > 0
                ? papaUnparse(eRows as unknown as Record<string, unknown>[], {
                    newline: "\n",
                  })
                : "",
          };
        });

        const sheet = await createPerObjectResultsSheet({
          flowName: "coursefilterbyquarter",
          objects: objectData,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: courseSheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[CourseFilter] Results sheet: ${sheet.url}`);
      } catch (err) {
        logger.warn(
          `[CourseFilter] Could not update results sheet: ${String(err)}`,
        );
      }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[CourseFilter] Window summary (rows ${startRow}–${nextStartRow - 1}):` +
        `\n  Skipped (quarter filter): ${skippedByQuarter}` +
        `\n  Learning:       ${counts.learning.ok} ok, ${counts.learning.err} err` +
        `\n  LearningCourse: ${counts.learningCourse.ok} ok, ${counts.learningCourse.err} err` +
        `\n  CourseOffering: ${counts.courseOffering.ok} ok, ${counts.courseOffering.err} err` +
        `\n  Schedule:       ${counts.schedule.ok} ok, ${counts.schedule.skipped} skip, ${counts.schedule.err} err` +
        `\n  COP-Instructor: ${counts.copInstructor.ok} ok, ${counts.copInstructor.skipped} skip, ${counts.copInstructor.err} err` +
        `\n  COP-Associate:  ${counts.copAssociate.ok} ok, ${counts.copAssociate.skipped} skip, ${counts.copAssociate.err} err`,
    );

    // ── Recurse ───────────────────────────────────────────────────────────────
    if (hasMore && (!TEST_MODE || nextStartRow < TEST_MAX_ROWS)) {
      logger.info(
        `[CourseFilter] Invoking next iteration at startRow=${nextStartRow}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("coursefilterbyquarter", {
        startRow: nextStartRow,
        courseSheetId: nextSheetId,
      });
    } else {
      logger.info("[CourseFilter] All rows processed — import complete.");
    }

    return {
      data: {
        startRow,
        rowsProcessed: rows.length,
        skippedByQuarter,
        hasMore,
        counts,
      },
    };
  },
});

export default [coursefilterbyquarterImport];
