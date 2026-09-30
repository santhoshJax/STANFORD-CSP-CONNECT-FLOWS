/**
 * Stanford CSP Migration – Course 32-Field Import flow (Tier 2).
 *
 * Same processing as the Course Import flow (courseFlow.ts), but maps only the
 * 32 Tier 2 course fields and covers the three academic years before Tier 1.
 *
 * Course data tiers (Julia / architect, Sep 2026):
 *   Tier 1 – all fields      – AY 2024-25, 2025-26 (courseFlow.ts)
 *   Tier 2 – 32 fields       – AY 2021-22, 2022-23, 2023-24 (this flow)
 *   Tier 3 – minimal fields  – AY 2020-21 and older (not built yet)
 * The number in a 4D quarter code is the academic year (starts in Fall), so
 * Tier 2 = fa/wi/sp/su 21, 22, 23.
 *
 * Each row triggers sequential Salesforce REST API calls in dependency order:
 *   Learning → LearningCourse → Instructor → CourseOffering
 *   → CourseOfferingSchedule → COP (Instructor / Associate)
 *
 * Not written by this flow (fields outside the 32): Location, department
 * (Learning.ProviderId, CourseOffering.Department__r, Course_Department__c),
 * Course_Submission fields, Textbooks, Enrollment_Status (derived from
 * Cancelled only).
 *
 * Learning / LearningCourse are shared with Tier 1 by base course code. If one
 * already exists in Salesforce it is left untouched, so an older Tier 2 title
 * or description never overwrites the newer Tier 1 catalog data.
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
  parseDurationSplit,
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
const MAX_ROWS = 100;
const TEST_MODE = false; // set false to process all rows
const TEST_MAX_ROWS = 100;
const SF_API = "v60.0";

// ── Quarter filter ────────────────────────────────────────────────────────────
// Tier 2 = the 3 academic years before Tier 1 (courseFlow: wi25, 2 years → 24–25).
// ANCHOR_QUARTER="wi23", YEARS_BACK=3 → fa/wi/sp/su 21, 22, 23
const ANCHOR_QUARTER = "wi23";
const YEARS_BACK = 3;

function buildValidQuarters(anchor: string, yearsBack: number): Set<string> {
  const seasons = ["wi", "sp", "su", "fa"];
  const m = /^([a-z]+)(\d+)$/.exec(anchor.toLowerCase());
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

// PDF: filter out junk values '00:00:00', 'False', 'None' — only load meaningful text.
function normaliseGradeRestriction(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  if (lower === "false" || lower === "none" || lower === "00:00:00")
    return null;
  return s;
}

// PDF: filter out junk values '00/00/00', '0.0', 'False'.
function normaliseInstructorPrefs(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  if (lower === "false" || s === "0.0" || lower === "00/00/00") return null;
  return s;
}

// ── Person resolvers ──────────────────────────────────────────────────────────

/**
 * Splits a raw display name into (first, last) parts for matching against
 * Salesforce's separate FirstName/LastName fields — ignoring any middle
 * name/initial. See courseFlow.ts for the full rationale.
 */
function splitName(raw: string): { first: string; last: string } | null {
  const s = raw.trim();
  if (!s) return null;
  if (s.includes(",")) {
    const [last, first] = s.split(",").map((p) => p.trim());
    return first && last ? { first, last } : null;
  }
  const parts = s.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return null;
  return { first: parts[0], last: parts[parts.length - 1] };
}

async function resolvePersonByName(
  base: string,
  token: string,
  name: string,
  cache: Map<string, string | null>,
  constituentRole?: string,
): Promise<string | null> {
  const n = name.trim();
  if (!n) return null;
  const cacheKey = constituentRole ? `${n}|${constituentRole}` : n;
  if (cache.has(cacheKey)) return cache.get(cacheKey)!;

  const roleClause =
    constituentRole === "Instructor"
      ? ` AND Instructor_ID_4D__c != null`
      : constituentRole === "Associate"
        ? ` AND Associate_ID_4D__c != null`
        : "";

  const parsed = splitName(n);
  let id: string | null = null;
  if (parsed) {
    const f = parsed.first.replace(/'/g, "\\'");
    const l = parsed.last.replace(/'/g, "\\'");
    const rows = await sfQuery<{ PersonContactId: string }>(
      base,
      token,
      `SELECT PersonContactId FROM Account WHERE IsPersonAccount = true AND ` +
        `((FirstName = '${f}' AND LastName = '${l}') OR (FirstName = '${l}' AND LastName = '${f}'))` +
        `${roleClause} LIMIT 1`,
    );
    id = rows[0]?.PersonContactId ?? null;
  }

  // Fallback: exact match on the combined Name field
  if (!id) {
    const esc = n.replace(/'/g, "\\'");
    const rows = await sfQuery<{ PersonContactId: string }>(
      base,
      token,
      `SELECT PersonContactId FROM Account WHERE IsPersonAccount = true AND Name = '${esc}'${roleClause} LIMIT 1`,
    );
    id = rows[0]?.PersonContactId ?? null;
  }

  cache.set(cacheKey, id);
  return id;
}

async function resolveInstructorContactId(
  base: string,
  token: string,
  instrId: string,
  cache: Map<string, string | null>,
): Promise<string | null> {
  const e = instrId.trim();
  if (!e || e === "0") return null;
  const key = `instr_${e}`;
  if (cache.has(key)) return cache.get(key)!;
  const rows = await sfQuery<{ PersonContactId: string }>(
    base,
    token,
    `SELECT PersonContactId FROM Account WHERE IsPersonAccount = true AND Instructor_ID_4D__c = '${e.replace(/'/g, "\\'")}' LIMIT 1`,
  );
  const id = rows[0]?.PersonContactId ?? null;
  cache.set(key, id);
  return id;
}

async function resolveAssociateContactId(
  base: string,
  token: string,
  assocId: string,
  cache: Map<string, string | null>,
): Promise<string | null> {
  const e = assocId.trim();
  if (!e || e === "0") return null;
  const key = `assoc_${e}`;
  if (cache.has(key)) return cache.get(key)!;
  const rows = await sfQuery<{ PersonContactId: string }>(
    base,
    token,
    `SELECT PersonContactId FROM Account WHERE IsPersonAccount = true AND Associate_ID_4D__c = '${e.replace(/'/g, "\\'")}' LIMIT 1`,
  );
  const id = rows[0]?.PersonContactId ?? null;
  cache.set(key, id);
  return id;
}

// ── Course-Instructor junction ────────────────────────────────────────────────

interface CourseInstructorRow {
  ID?: string;
  Course_ID?: string;
  Instructor_ID?: string;
  IsPrimary?: string;
}

async function loadCourseInstructors(
  fileId: string,
  token: string,
): Promise<Map<string, CourseInstructorRow[]>> {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`;
  const { data } = await axios.get<string>(url, {
    params: { alt: "media", supportsAllDrives: "true" },
    headers: { Authorization: `Bearer ${token}` },
    responseType: "text",
  });
  const content = data.replace(/^\uFEFF/, "");
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

// ── Course-Associate junction ─────────────────────────────────────────────────

interface CourseAssociateRow {
  ID?: string;
  Course_RecID?: string;
  Associate_ID?: string;
  Speaking_Date?: string;
}

async function loadCourseAssociates(
  fileId: string,
  token: string,
): Promise<Map<string, CourseAssociateRow[]>> {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`;
  const { data } = await axios.get<string>(url, {
    params: { alt: "media", supportsAllDrives: "true" },
    headers: { Authorization: `Bearer ${token}` },
    responseType: "text",
  });
  const content = data.replace(/^\uFEFF/, "");
  const { data: rows } = papaParse<CourseAssociateRow>(content, {
    header: true,
    skipEmptyLines: true,
    delimiter: "\t",
  });
  const map = new Map<string, CourseAssociateRow[]>();
  for (const row of rows) {
    const cid = row.Course_RecID?.trim();
    if (!cid) continue;
    if (!map.has(cid)) map.set(cid, []);
    map.get(cid)!.push(row);
  }
  return map;
}

// ── Raw row type (32 mapped fields + join keys) ───────────────────────────────

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
  Hybrid?: string; // used to derive Format__c, not sent to SF directly
  RecID?: string; // join key for Course_Associate junction, not sent to SF
  [key: string]: string | undefined;
}

// ── Result rows ───────────────────────────────────────────────────────────────

interface SuccessRow {
  Source_ID: string;
  Code: string;
  Title: string;
  Quarter: string;
  Object: string;
  fields?: Record<string, unknown>;
  sf__Id: string;
  sf__Created: string;
}

interface ErrorRow {
  Source_ID: string;
  Code: string;
  Title: string;
  Quarter: string;
  Object: string;
  fields?: Record<string, unknown>;
  sf__Error: string;
}

// ── TSV streaming ─────────────────────────────────────────────────────────────

interface StreamResult {
  rows: RawCourseRow[];
  hasMore: boolean;
  nextStartRow: number;
  skippedByQuarter: number;
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
    let aborted = false;
    let skippedByQuarter = 0;

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
              h
                .replace(/^\uFEFF/, "")
                .replace(/\r/g, "")
                .trim() || `__blank_${i}`,
          );
          idField =
            headers.find(
              (h) => h.replace(/^\uFEFF/, "").toLowerCase() === "id",
            ) ?? "id";
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

        // Quarter filter — Tier 2 academic years only. Most of the file is
        // outside this window (Tier 1 / Tier 3), so skips are counted, not logged.
        const quarterNorm = (record.Quarter ?? "").trim().toLowerCase();
        if (!validQuarters.has(quarterNorm)) {
          skippedByQuarter++;
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

export const course32fieldsImport = flow({
  name: "course32fields",
  stableKey: "d4e5f6a7-2b3c-4d5e-9f6a-bb22cc33dd44",
  description:
    "Tier 2 course import: streams the course TSV, keeps academic years 2021-22 " +
    "through 2023-24, and maps the 32 Tier 2 fields to Learning, LearningCourse, " +
    "CourseOffering, CourseOfferingSchedule, and CourseOfferingParticipant.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    // ── Cursor from trigger payload ───────────────────────────────────────────
    const triggerBody = (
      params.onTrigger.results as unknown as
        { body?: { data?: unknown } } | undefined
    )?.body?.data as Record<string, unknown> | undefined;

    const startRow =
      typeof triggerBody?.startRow === "number" ? triggerBody.startRow : 0;
    const courseSheetId =
      typeof triggerBody?.courseSheetId === "string"
        ? triggerBody.courseSheetId
        : undefined;

    logger.info(`[Course32] Starting at row ${startRow}`);

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars[
      "Google Drive Connection"
    ] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Course File ID"] as unknown as string;
    const instructorFileId = configVars[
      "Course Instructor File ID"
    ] as unknown as string | undefined;
    const associateFileId = configVars[
      "Course Associate File ID"
    ] as unknown as string | undefined;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      string | undefined;

    if (!fileId) throw new Error("Course File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── Existing catalog records (owned by Tier 1 / newer rows) ───────────────
    logger.info("[Course32] Building lookup caches…");
    const existingLearningCache = await buildFieldCache(
      sfBase,
      sfToken,
      "SELECT Id, External_ID_4D__c FROM Learning WHERE External_ID_4D__c != null",
      "External_ID_4D__c",
    );
    logger.info(
      `[Course32] Caches — existing Learning=${existingLearningCache.size}`,
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
          `[Course32] Course-Instructor junction loaded — ${courseInstructorMap.size} courses`,
        );
      } catch (err) {
        logger.warn(
          `[Course32] Could not load Course-Instructor file: ${String(err)} — falling back to name lookup`,
        );
      }
    }

    // ── Course-Associate junction ─────────────────────────────────────────────
    let courseAssociateMap = new Map<string, CourseAssociateRow[]>();
    if (associateFileId) {
      try {
        courseAssociateMap = await loadCourseAssociates(
          associateFileId,
          gdToken,
        );
        logger.info(
          `[Course32] Course-Associate junction loaded — ${courseAssociateMap.size} courses`,
        );
      } catch (err) {
        logger.warn(
          `[Course32] Could not load Course-Associate file: ${String(err)} — falling back to Primary_Associate field`,
        );
      }
    }

    const personByNameCache = new Map<string, string | null>();
    const personByExtIdCache = new Map<string, string | null>();

    // ── Stream TSV window ─────────────────────────────────────────────────────
    const validQuarters = buildValidQuarters(ANCHOR_QUARTER, YEARS_BACK);
    logger.info(
      `[Course32] Streaming rows ${startRow}–${startRow + MAX_ROWS - 1}… valid quarters: ${[...validQuarters].join(", ")}`,
    );
    const { rows, hasMore, nextStartRow, skippedByQuarter } =
      await streamAndParseTsv(
        fileId,
        gdToken,
        startRow,
        MAX_ROWS,
        validQuarters,
      );
    logger.info(
      `[Course32] Parsed ${rows.length} rows, skipped ${skippedByQuarter} (outside Tier 2 quarters) (hasMore=${hasMore})`,
    );

    // ── Pre-warm caches for this window ──────────────────────────────────────
    if (rows.length > 0) {
      const allInstrIds = new Set<string>();
      const allAssocIds = new Set<string>();
      for (const row of rows) {
        for (const jr of courseInstructorMap.get(str(row.id)) ?? []) {
          if (jr.Instructor_ID?.trim())
            allInstrIds.add(jr.Instructor_ID.trim());
        }
        const coord = str(row.Coordinator_ID).trim();
        if (coord && coord !== "0") {
          allInstrIds.add(coord);
          allAssocIds.add(coord);
        }
        const assocRecId = str(row.RecID).trim();
        for (const jr of courseAssociateMap.get(assocRecId) ?? []) {
          if (jr.Associate_ID?.trim()) allAssocIds.add(jr.Associate_ID.trim());
        }
      }

      await Promise.all([
        allInstrIds.size > 0
          ? sfQuery<{ PersonContactId: string; Instructor_ID_4D__c: string }>(
              sfBase,
              sfToken,
              `SELECT PersonContactId, Instructor_ID_4D__c FROM Account WHERE IsPersonAccount = true AND Instructor_ID_4D__c IN (${[...allInstrIds].map((id) => `'${id.replace(/'/g, "\\'")}'`).join(",")})`,
            ).then((rs) =>
              rs.forEach((r) =>
                personByExtIdCache.set(
                  `instr_${r.Instructor_ID_4D__c}`,
                  r.PersonContactId,
                ),
              ),
            )
          : Promise.resolve(),

        allAssocIds.size > 0
          ? sfQuery<{ PersonContactId: string; Associate_ID_4D__c: string }>(
              sfBase,
              sfToken,
              `SELECT PersonContactId, Associate_ID_4D__c FROM Account WHERE IsPersonAccount = true AND Associate_ID_4D__c IN (${[...allAssocIds].map((id) => `'${id.replace(/'/g, "\\'")}'`).join(",")})`,
            ).then((rs) =>
              rs.forEach((r) =>
                personByExtIdCache.set(
                  `assoc_${r.Associate_ID_4D__c}`,
                  r.PersonContactId,
                ),
              ),
            )
          : Promise.resolve(),
      ]);

      logger.info(
        `[Course32] Pre-warmed — instrs:${allInstrIds.size} assocs:${allAssocIds.size}`,
      );
    }

    // ── Counters ──────────────────────────────────────────────────────────────
    const counts = {
      learning: { ok: 0, skipped: 0, err: 0 },
      learningCourse: { ok: 0, skipped: 0, err: 0 },
      courseOffering: { ok: 0, err: 0 },
      schedule: { ok: 0, skipped: 0, err: 0 },
      copInstructor: { ok: 0, skipped: 0, err: 0 },
      copAssociate: { ok: 0, skipped: 0, err: 0 },
    };

    const successRows: SuccessRow[] = [];
    const errorRows: ErrorRow[] = [];

    // Post-offering tasks collected during the row loop, fired all at once after.
    const postOfferingTasks: (() => Promise<void>)[] = [];

    // ── Row loop ──────────────────────────────────────────────────────────────
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const sourceId = str(row.id);
      const code = str(row.Code);
      const baseCode = stripSectionSuffix(code) || code;
      const sectionSuffix = extractSectionSuffix(code);
      const title = str(row.Title)
        .replace(/_4DNL_/g, " ")
        .trim();
      const quarter = str(row.Quarter);

      const ok = (
        object: string,
        sfId: string,
        created: boolean,
        fields?: Record<string, unknown>,
      ) =>
        successRows.push({
          Source_ID: sourceId,
          Code: code,
          Title: title,
          Quarter: quarter,
          Object: object,
          fields,
          sf__Id: sfId,
          sf__Created: String(created),
        });

      const fail = (
        object: string,
        error: string,
        fields?: Record<string, unknown>,
      ) =>
        errorRows.push({
          Source_ID: sourceId,
          Code: code,
          Title: title,
          Quarter: quarter,
          Object: object,
          fields,
          sf__Error: error,
        });

      // 1–2. Learning + LearningCourse — only when this base course doesn't
      // exist yet, so Tier 1 (newer) catalog data is never overwritten.
      if (existingLearningCache.has(baseCode)) {
        counts.learning.skipped++;
        counts.learningCourse.skipped++;
      } else {
        const learningPayload: Record<string, unknown> = {
          Name: (title || baseCode).slice(0, 255),
          Type: "LearningCourse",
          IsActive: true,
        };
        try {
          const r = await sfUpsert(
            sfBase,
            sfToken,
            "Learning",
            "External_ID_4D__c",
            baseCode,
            learningPayload,
          );
          existingLearningCache.set(baseCode, r.id);
          counts.learning.ok++;
          ok("Learning", r.id, r.created, {
            External_ID_4D__c: baseCode,
            ...learningPayload,
          });
        } catch (err) {
          counts.learning.err++;
          const msg = sfErrMsg(err);
          logger.error(
            `[Course32] Row ${startRow + i} (${sourceId}) Learning: ${msg}`,
          );
          fail("Learning", msg, {
            External_ID_4D__c: baseCode,
            ...learningPayload,
          });
          continue;
        }

        const lc: Record<string, unknown> = {
          Name: (title || baseCode).slice(0, 255),
          CourseNumber: baseCode.replace(/\s+/g, ""),
        };
        const catalogNotes = normaliseCatalogNotes(row.Catalog_Notes);
        if (catalogNotes) lc.Catalog_Notes__c = catalogNotes;
        const description = normaliseCatalogNotes(row.Description);
        if (description) lc.Description = description.slice(0, 32000);
        lc.Learning = { External_ID_4D__c: baseCode };
        try {
          const r = await sfUpsert(
            sfBase,
            sfToken,
            "LearningCourse",
            "External_ID_4D__c",
            baseCode,
            lc,
          );
          counts.learningCourse.ok++;
          ok("LearningCourse", r.id, r.created, lc);
        } catch (err) {
          counts.learningCourse.err++;
          logger.error(
            `[Course32] Row ${startRow + i} (${sourceId}) LearningCourse: ${sfErrMsg(err)}`,
          );
          fail("LearningCourse", sfErrMsg(err), lc);
        }
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
            primaryInstructorId = await resolveInstructorContactId(
              sfBase,
              sfToken,
              instrExtId,
              personByExtIdCache,
            );
            if (!primaryInstructorId)
              logger.warn(
                `[Course32] Row ${startRow + i} (${sourceId}) Primary instructor not found: Instructor_ID="${instrExtId}"`,
              );
          } catch (err) {
            logger.warn(
              `[Course32] Row ${startRow + i} (${sourceId}) Instructor lookup: ${sfErrMsg(err)}`,
            );
          }
        }
      } else if (str(row.Primary_Instructor)) {
        // Fallback: no junction data — look up by name
        try {
          primaryInstructorId = await resolvePersonByName(
            sfBase,
            sfToken,
            str(row.Primary_Instructor),
            personByNameCache,
            "Instructor",
          );
          if (!primaryInstructorId)
            logger.warn(
              `[Course32] Row ${startRow + i} (${sourceId}) Instructor not found by name: "${str(row.Primary_Instructor)}"`,
            );
        } catch (err) {
          logger.warn(
            `[Course32] Row ${startRow + i} (${sourceId}) Instructor name lookup: ${sfErrMsg(err)}`,
          );
        }
      }

      // Resolve Coordinator_ID → Contact SF ID
      let coordId: string | null = null;
      const coordExtId = str(row.Coordinator_ID);
      if (coordExtId && coordExtId !== "0") {
        try {
          // Coordinator may be an instructor or associate — try both typed fields
          coordId = await resolveInstructorContactId(
            sfBase,
            sfToken,
            coordExtId,
            personByExtIdCache,
          );
          if (!coordId) {
            coordId = await resolveAssociateContactId(
              sfBase,
              sfToken,
              coordExtId,
              personByExtIdCache,
            );
          }
          if (!coordId)
            logger.warn(
              `[Course32] Row ${startRow + i} (${sourceId}) Coordinator not found: Coordinator_ID="${coordExtId}"`,
            );
        } catch (err) {
          logger.warn(
            `[Course32] Row ${startRow + i} (${sourceId}) Coordinator lookup: ${sfErrMsg(err)}`,
          );
        }
      }

      // 4. CourseOffering
      let courseOfferingId: string | null = null;
      const isCancelled = parseBool(row.Cancelled) === true;
      const co: Record<string, unknown> = {
        Name: code || sourceId,
        ...(sectionSuffix ? { SectionNumber: sectionSuffix } : {}),
        // Enrollment_Status is not a Tier 2 field — derive from Cancelled only
        Enrollment_Status__c: normaliseEnrollmentStatus(undefined, isCancelled),
      };
      co.LearningCourse = { External_ID_4D__c: baseCode };
      if (quarter) co.AcademicSession = { Abbreviation__c: quarter };
      if (primaryInstructorId) co.PrimaryFacultyId = primaryInstructorId;
      if (coordId) co.Coordinator__c = coordId;
      try {
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
        // EnrolleeCount is system-calculated (read-only) — cannot be written via API
        // setInt("EnrolleeCount", row.Enrollment_Count);
        setInt("EnrollmentCapacity", row.Max_Enrollment);
        const dur = parseDurationSplit(row.Duration);
        if (dur !== null) {
          co.Duration_Value__c = dur.value;
          co.Duration_Unit__c = dur.unit;
        }
        setFloat("Units__c", row.Units);
        setFloat("Additional_Fee__c", row.Additional_Fee);
        setFloat("Tuition_Amount__c", row.Tuition);
        const staffNotes = str(row.Staff_Notes);
        if (staffNotes) co.Staff_Notes__c = staffNotes;
        setBool("Limited_Enrollment__c", row.Limited_Enrollment);
        const exceptionText = normaliseCatalogNotes(row.Exception_Text);
        if (exceptionText) co.Exception_Text__c = exceptionText;
        const droppedSpecialPct = parseFloatVal(row.Dropped_Special_Percent);
        if (droppedSpecialPct !== null && droppedSpecialPct <= 100)
          co.Dropped_Special_Percent__c = droppedSpecialPct;
        setFloat("Global_Eval__c", row.Global_Eval);
        setFloat("Return_Rate__c", row.Return_Rate);
        setInt("Enroll_On_Start_Date__c", row.Enroll_On_Start_Date);
        const fmt =
          normaliseFormat(row.Format, row.Hybrid) ??
          formatFromSuffix(sectionSuffix);
        if (fmt) co.Format__c = fmt;
        const gradeRestriction = normaliseGradeRestriction(
          row.Grade_Restriction,
        );
        if (gradeRestriction) co.Grade_Restriction__c = gradeRestriction;
        setStr("Course_Version__c", row.Course_Version);
        setBool("Recording__c", row.Recording);
        const instrPrefs = normaliseInstructorPrefs(row.Instructor_Preferences);
        if (instrPrefs) co.Textbook_Instructor_Preferences__c = instrPrefs;

        logger.info(
          `[Course32] Row ${startRow + i} (${sourceId}) CourseOffering required fields:` +
            `\n  LearningCourse     = ${baseCode} (External_ID_4D__c)` +
            `\n  AcademicSession    = ${quarter || "NULL"} (Abbreviation__c)` +
            `\n  EnrollmentCapacity = ${co.EnrollmentCapacity ?? "NULL"}` +
            `\n  PrimaryFacultyId   = ${co.PrimaryFacultyId ?? "NULL"}`,
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
        ok("CourseOffering", r.id, r.created, {
          External_ID_4D__c: sourceId,
          ...co,
        });
      } catch (err) {
        counts.courseOffering.err++;
        const msg = sfErrMsg(err);
        logger.error(
          `[Course32] Row ${startRow + i} (${sourceId}) CourseOffering: ${msg}`,
        );
        fail("CourseOffering", msg, { External_ID_4D__c: sourceId, ...co });
        continue;
      }

      // 5-7. Post-offering operations — all independent once courseOfferingId is set.
      // Run Schedule, COP-Instructor, COP-Associate in parallel.
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

      postOfferingTasks.push(() =>
        Promise.all([
          // Task A: CourseOfferingSchedule (no Location — Building/Room not in Tier 2)
          (async () => {
            if (str(row.Weekday) || str(row.Course_Time)) {
              try {
                const existing = await sfQuery<{ Id: string }>(
                  sfBase,
                  sfToken,
                  `SELECT Id FROM CourseOfferingSchedule WHERE CourseOfferingId = '${courseOfferingId}' LIMIT 1`,
                );
                if (existing.length === 0) {
                  const dayFlags = parseWeekdays(row.Weekday);
                  const { startTime, endTime } = parseCourseTime(
                    row.Course_Time,
                  );
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
                  ok("CourseOfferingSchedule", schedId, true, sched);
                } else {
                  counts.schedule.skipped++;
                }
              } catch (err) {
                counts.schedule.err++;
                const msg = sfErrMsg(err);
                logger.warn(
                  `[Course32] Row ${startRow + i} (${sourceId}) Schedule: ${msg}`,
                );
                fail("CourseOfferingSchedule", msg, {
                  CourseOfferingId: courseOfferingId,
                  Weekday: str(row.Weekday),
                  Course_Time: str(row.Course_Time),
                });
              }
            } else {
              counts.schedule.skipped++;
            }
          })(),

          // Task B: COP-Instructor
          (async () => {
            for (const jRow of instructorRows) {
              const contactId = jRow.Instructor_ID?.trim()
                ? await resolveInstructorContactId(
                    sfBase,
                    sfToken,
                    jRow.Instructor_ID.trim(),
                    personByExtIdCache,
                  ).catch(() => null)
                : primaryInstructorId;

              if (!contactId) {
                counts.copInstructor.skipped++;
                continue;
              }
              const instrExternal: Record<string, unknown> = {
                CourseOfferingId: courseOfferingId,
                ParticipantContactId: contactId,
                ParticipantAffiliation: "Instructor",
                IsPrimary__c: parseBool(jRow.IsPrimary) ?? false,
              };
              const instrJunctionId = jRow.ID?.trim();
              if (instrJunctionId)
                instrExternal.External_ID_4D__c = `CINST-${instrJunctionId}`;
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
                    instrExternal,
                  );
                  counts.copInstructor.ok++;
                  ok("COP-Instructor", copId, true, instrExternal);
                } else {
                  counts.copInstructor.skipped++;
                }
              } catch (err) {
                counts.copInstructor.err++;
                const msg = sfErrMsg(err);
                logger.warn(
                  `[Course32] Row ${startRow + i} (${sourceId}) COP-Instructor: ${msg}`,
                );
                fail("COP-Instructor", msg, instrExternal);
              }
            }
          })(),

          // Task C: COP-Associate
          // Preferred: one COP per Course_Associate junction row (matched via Course.RecID).
          // Fallback: Primary_Associate field on course row (mixed names / numeric IDs).
          (async () => {
            const assocRecId = str(row.RecID).trim();
            const assocJunctionRows = assocRecId
              ? (courseAssociateMap.get(assocRecId) ?? [])
              : [];

            if (assocJunctionRows.length > 0) {
              for (const jRow of assocJunctionRows) {
                const assocExtId = jRow.Associate_ID?.trim();
                if (!assocExtId) {
                  counts.copAssociate.skipped++;
                  continue;
                }
                // Null/placeholder Speaking_Date = Course Support Specialist,
                // real date = Guest Speaker (same rule as courseFlow.ts).
                const affiliation =
                  parseCourseDate(jRow.Speaking_Date) !== null
                    ? "Guest Speaker"
                    : "Course Support Specialist";
                try {
                  const assocId = await resolveAssociateContactId(
                    sfBase,
                    sfToken,
                    assocExtId,
                    personByExtIdCache,
                  ).catch(() => null);
                  if (assocId) {
                    const existing = await sfQuery<{ Id: string }>(
                      sfBase,
                      sfToken,
                      `SELECT Id FROM CourseOfferingParticipant WHERE CourseOfferingId = '${courseOfferingId}' AND ParticipantContactId = '${assocId}' AND ParticipantAffiliation IN ('Associate','Course Support Specialist','Guest Speaker') LIMIT 1`,
                    );
                    if (existing.length === 0) {
                      const assocExternal: Record<string, unknown> = {
                        CourseOfferingId: courseOfferingId,
                        ParticipantContactId: assocId,
                        ParticipantAffiliation: affiliation,
                      };
                      const assocJunctionId = jRow.ID?.trim();
                      if (assocJunctionId)
                        assocExternal.External_ID_4D__c = `CASSOC-${assocJunctionId}`;
                      const copId = await sfCreate(
                        sfBase,
                        sfToken,
                        "CourseOfferingParticipant",
                        assocExternal,
                      );
                      counts.copAssociate.ok++;
                      ok("COP-Associate", copId, true, assocExternal);
                    } else {
                      counts.copAssociate.skipped++;
                    }
                  } else {
                    const msg = `Associate ID "${assocExtId}" not found in Salesforce`;
                    logger.warn(
                      `[Course32] Row ${startRow + i} (${sourceId}) COP-Associate: ${msg}`,
                    );
                    counts.copAssociate.skipped++;
                    fail("COP-Associate", msg, {
                      CourseOfferingId: courseOfferingId,
                      Associate_ID: assocExtId,
                      ParticipantAffiliation: affiliation,
                    });
                  }
                } catch (err) {
                  counts.copAssociate.err++;
                  const msg = sfErrMsg(err);
                  logger.warn(
                    `[Course32] Row ${startRow + i} (${sourceId}) COP-Associate: ${msg}`,
                  );
                  fail("COP-Associate", msg, {
                    CourseOfferingId: courseOfferingId,
                    Associate_ID: assocExtId ?? "",
                    ParticipantAffiliation: affiliation,
                  });
                }
              }
            } else {
              // Fallback: Primary_Associate field — mixed numeric IDs and text names
              const assocRaw = str(row.Primary_Associate)
                .trim()
                .replace(/\.0$/, "");
              if (assocRaw && assocRaw !== "0") {
                const isNumericAssoc = /^\d+$/.test(assocRaw);
                const fallbackAffiliation = "Course Support Specialist";
                try {
                  const assocId = isNumericAssoc
                    ? await resolveAssociateContactId(
                        sfBase,
                        sfToken,
                        assocRaw,
                        personByExtIdCache,
                      )
                    : await resolvePersonByName(
                        sfBase,
                        sfToken,
                        assocRaw,
                        personByNameCache,
                        "Associate",
                      );
                  if (assocId) {
                    const existing = await sfQuery<{ Id: string }>(
                      sfBase,
                      sfToken,
                      `SELECT Id FROM CourseOfferingParticipant WHERE CourseOfferingId = '${courseOfferingId}' AND ParticipantContactId = '${assocId}' AND ParticipantAffiliation IN ('Associate','Course Support Specialist','Guest Speaker') LIMIT 1`,
                    );
                    if (existing.length === 0) {
                      const fallbackAssocSfPayload = {
                        CourseOfferingId: courseOfferingId,
                        ParticipantContactId: assocId,
                        ParticipantAffiliation: fallbackAffiliation,
                      };
                      const copId = await sfCreate(
                        sfBase,
                        sfToken,
                        "CourseOfferingParticipant",
                        fallbackAssocSfPayload,
                      );
                      counts.copAssociate.ok++;
                      ok("COP-Associate", copId, true, {
                        ...fallbackAssocSfPayload,
                        Primary_Associate: assocRaw,
                      });
                    } else {
                      counts.copAssociate.skipped++;
                    }
                  } else {
                    const msg = `Associate "${assocRaw}" not found in Salesforce`;
                    logger.warn(
                      `[Course32] Row ${startRow + i} (${sourceId}) ${msg}`,
                    );
                    counts.copAssociate.skipped++;
                    fail("COP-Associate", msg, {
                      CourseOfferingId: courseOfferingId,
                      Primary_Associate: assocRaw,
                      ParticipantAffiliation: fallbackAffiliation,
                    });
                  }
                } catch (err) {
                  counts.copAssociate.err++;
                  const msg = sfErrMsg(err);
                  logger.warn(
                    `[Course32] Row ${startRow + i} (${sourceId}) COP-Associate: ${msg}`,
                  );
                  fail("COP-Associate", msg, {
                    CourseOfferingId: courseOfferingId,
                    Primary_Associate: assocRaw,
                    ParticipantAffiliation: fallbackAffiliation,
                  });
                }
              } else {
                counts.copAssociate.skipped++;
              }
            }
          })(),
        ]).then(() => undefined),
      );
    }

    // ── Batch post-offering operations ────────────────────────────────────────
    if (postOfferingTasks.length > 0) {
      logger.info(
        `[Course32] Firing post-offering batch for ${postOfferingTasks.length} rows simultaneously`,
      );
      await Promise.all(postOfferingTasks.map((t) => t()));
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
                fields,
                sf__Id,
                sf__Created,
              }) => ({
                Source_ID,
                Code,
                Title,
                Quarter,
                ...fields,
                sf__Id,
                sf__Created,
              }),
            );
          const eRows = errorRows
            .filter((r) => r.Object === objName)
            .map(({ Source_ID, Code, Title, Quarter, fields, sf__Error }) => ({
              Source_ID,
              Code,
              Title,
              Quarter,
              ...fields,
              sf__Error,
            }));
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
          flowName: "course32fields",
          objects: objectData,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: courseSheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Course32] Results sheet: ${sheet.url}`);
      } catch (err) {
        logger.warn(
          `[Course32] Could not update results sheet: ${String(err)}`,
        );
      }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Course32] Window summary (rows ${startRow}–${nextStartRow - 1}):` +
        `\n  Learning:       ${counts.learning.ok} ok, ${counts.learning.skipped} skip (exists), ${counts.learning.err} err` +
        `\n  LearningCourse: ${counts.learningCourse.ok} ok, ${counts.learningCourse.skipped} skip (exists), ${counts.learningCourse.err} err` +
        `\n  CourseOffering: ${counts.courseOffering.ok} ok, ${counts.courseOffering.err} err` +
        `\n  Schedule:       ${counts.schedule.ok} ok, ${counts.schedule.skipped} skip, ${counts.schedule.err} err` +
        `\n  COP-Instructor: ${counts.copInstructor.ok} ok, ${counts.copInstructor.skipped} skip, ${counts.copInstructor.err} err` +
        `\n  COP-Associate:  ${counts.copAssociate.ok} ok, ${counts.copAssociate.skipped} skip, ${counts.copAssociate.err} err`,
    );

    // ── Recurse ───────────────────────────────────────────────────────────────
    if (hasMore && (!TEST_MODE || nextStartRow < TEST_MAX_ROWS)) {
      logger.info(
        `[Course32] Invoking next iteration at startRow=${nextStartRow}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("course32fields", {
        startRow: nextStartRow,
        courseSheetId: nextSheetId,
      });
    } else {
      logger.info("[Course32] All rows processed — import complete.");
    }

    return { data: { startRow, rowsProcessed: rows.length, hasMore, counts } };
  },
});

export default [course32fieldsImport];
