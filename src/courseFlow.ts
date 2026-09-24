/**
 * Stanford CSP Migration – Course Import flow.
 *
 * Streams the course TSV from Google Drive one window (MAX_ROWS rows) at a time.
 * Each row triggers sequential Salesforce REST API calls in dependency order:
 *   Learning → LearningCourse → Location → Instructor → CourseOffering
 *   → CourseOfferingSchedule → COP (Instructor / Coordinator / Associate)
 *   → AcademicTerm RegistrationOpenDate update
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
  normaliseRosterEmail,
  parseClosedDTS,
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
// e.g. ANCHOR_QUARTER="wi25", YEARS_BACK=2 → wi24,sp24,su24,fa24,wi25,sp25,su25,fa25
const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 2;

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

// ── Location resolver ─────────────────────────────────────────────────────────

function buildLocationName(building: string, room: string): string | null {
  const b = building
    .trim()
    .replace(/_4DNL_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!b) return null;
  const r = room.trim();
  return r ? `${b} - Rm ${r}` : b;
}

// PDF: only load values that are valid https:// URLs; reject 'False', numerics, etc.
function normaliseHttpsUrl(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  return s.startsWith("https://") ? s : null;
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

// PDF: filter '00:00:00'; strip trailing '.0' from numeric passwords.
function normaliseZoomPw(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  if (!s || s === "00:00:00") return null;
  return s.replace(/\.0$/, "");
}

// PDF: strip trailing '.0'; load 0 as null.
function normaliseEvalDocId(v: string | undefined): string | null {
  const s = (v ?? "").trim().replace(/\.0$/, "");
  if (!s || s === "0") return null;
  return s;
}

// PDF: same rule as Evaluation_Document_ID — strip trailing '.0'; load 0 as null.
function normaliseSurveyId(v: string | undefined): string | null {
  return normaliseEvalDocId(v);
}

// PDF: filter out 'False' values. Load only actual waiver type text.
function normaliseWaiverType(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  if (!s || s.toLowerCase() === "false") return null;
  return s;
}

// PDF: filter out '0.0' junk values. Load only meaningful text.
function normaliseStudentRole(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  if (!s || s === "0.0") return null;
  return s;
}

// PDF: filter out numeric junk values ('424.0', '1274.0'). Only load actual text labels.
function normaliseAddFeeLabel(v: string | undefined): string | null {
  const s = (v ?? "").trim();
  if (!s || /^\d+(\.\d+)?$/.test(s)) return null;
  return s;
}

async function resolveLocation(
  base: string,
  token: string,
  building: string,
  room: string,
  cache: Map<string, string>,
): Promise<string | null> {
  const name = buildLocationName(building, room);
  if (!name) return null;

  const cached = cache.get(name);
  if (cached) return cached;

  const esc = name.replace(/'/g, "\\'");
  const rows = await sfQuery<{ Id: string }>(
    base,
    token,
    `SELECT Id FROM Location WHERE Name = '${esc}' LIMIT 1`,
  );
  const id =
    rows[0]?.Id ?? (await sfCreate(base, token, "Location", { Name: name }));
  cache.set(name, id);
  return id;
}

// ── Person resolvers ──────────────────────────────────────────────────────────

/**
 * Splits a raw display name into (first, last) parts for matching against
 * Salesforce's separate FirstName/LastName fields — deliberately ignoring
 * any middle name/initial. A raw source name like "Jill Fordyce" often omits
 * a middle name/initial that IS present on the real Account ("Jill A
 * Fordyce"), which made an exact match on the combined Name field fail even
 * though the person genuinely exists.
 *   "Last, First" (comma present)  → split on the comma
 *   "First [Middle...] Last"       → first token = first, last token = last,
 *                                     anything in between (middle name/
 *                                     initial) discarded
 * Returns null for anything that can't be split into two parts (e.g. a
 * single-word name) — caller falls back to a full-Name match for those.
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

  // Preferred: match FirstName/LastName separately (forward OR swapped),
  // same semantics as nameMatches() used elsewhere for Student/Instructor/
  // Associate pre-matching. Ignoring MiddleName is what lets "Jill Fordyce"
  // correctly match an Account stored as FirstName="Jill", MiddleName="A",
  // LastName="Fordyce".
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

  // Fallback: exact match on the combined Name field — covers single-token
  // names (e.g. a mononym) or anything splitName() couldn't parse.
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
  Estimated_Comp?: string;
  Salary_Category?: string;
  Gross_Pay?: string;
  Hire_Date?: string;
  Instructor_Term_Date?: string;
  Hi_Enroll_Bonus?: string;
  Notes?: string;
  Offer_Letter_Received?: string;
  Job_Record_Number?: string;
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
  const content = data.replace(/^﻿/, "");
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

// ── Course-Submission data ────────────────────────────────────────────────────

interface CourseSubmissionRow {
  Course_Code?: string;
  Course_Quarter?: string;
  Course_Occurrences?: string;
  Course_Hours_Per?: string;
  Course_Total_Hours?: string;
  Tuition_Adjustment?: string;
  Tuition_Adjustment_Note?: string;
  Course_Async_Hours?: string;
}

async function loadCourseSubmissions(
  fileId: string,
  token: string,
): Promise<Map<string, CourseSubmissionRow>> {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`;
  const { data } = await axios.get<string>(url, {
    params: { alt: "media", supportsAllDrives: "true" },
    headers: { Authorization: `Bearer ${token}` },
    responseType: "text",
  });
  const content = data.replace(/^﻿/, "");
  const { data: rows } = papaParse<CourseSubmissionRow>(content, {
    header: true,
    skipEmptyLines: true,
    delimiter: "\t",
  });
  const map = new Map<string, CourseSubmissionRow>();
  for (const row of rows) {
    const code = row.Course_Code?.trim();
    const quarter = row.Course_Quarter?.trim().toLowerCase();
    if (!code || !quarter) continue;
    const key = `${code}|${quarter}`;
    if (!map.has(key)) map.set(key, row);
  }
  return map;
}

// ── Course-Department junction ────────────────────────────────────────────────

interface CourseDepartmentRow {
  ID?: string;
  Course_ID?: string;
  Department_ID?: string;
  IsPrimary?: string;
}

async function loadCourseDepartments(
  fileId: string,
  token: string,
): Promise<Map<string, CourseDepartmentRow[]>> {
  const url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`;
  const { data } = await axios.get<string>(url, {
    params: { alt: "media", supportsAllDrives: "true" },
    headers: { Authorization: `Bearer ${token}` },
    responseType: "text",
  });
  const content = data.replace(/^﻿/, "");
  const { data: rows } = papaParse<CourseDepartmentRow>(content, {
    header: true,
    skipEmptyLines: true,
    delimiter: "\t",
  });
  const map = new Map<string, CourseDepartmentRow[]>();
  for (const row of rows) {
    const cid = row.Course_ID?.trim();
    if (!cid) continue;
    if (!map.has(cid)) map.set(cid, []);
    map.get(cid)!.push(row);
  }
  return map;
}

// Returns Map<baseCode, departmentId> using the most recent offering's primary dept.
// Course_ID format: "{quarterCode}_{courseCode}" e.g. "20253_OWC 303 A"
function buildDeptByBaseCourseMap(
  deptMap: Map<string, CourseDepartmentRow[]>,
): Map<string, string> {
  const result = new Map<string, string>();

  const byBase = new Map<
    string,
    Array<{ quarterNum: number; row: CourseDepartmentRow }>
  >();

  for (const [courseId, rows] of deptMap) {
    const sep = courseId.indexOf("_");
    const quarterNum = sep >= 0 ? parseInt(courseId.slice(0, sep), 10) || 0 : 0;
    const codeStr = sep >= 0 ? courseId.slice(sep + 1).trim() : courseId.trim();
    const baseCode = stripSectionSuffix(codeStr) || codeStr;

    if (!byBase.has(baseCode)) byBase.set(baseCode, []);
    for (const row of rows) {
      byBase.get(baseCode)!.push({ quarterNum, row });
    }
  }

  for (const [baseCode, entries] of byBase) {
    // Most recent offering first
    entries.sort((a, b) => b.quarterNum - a.quarterNum);
    // Prefer IsPrimary=True; fall back to any row
    const primary = entries.filter((e) => parseBool(e.row.IsPrimary) === true);
    const candidates = primary.length > 0 ? primary : entries;
    const deptId = candidates[0].row.Department_ID?.trim();
    if (deptId && deptId !== "0") result.set(baseCode, deptId);
  }

  return result;
}

// Per-offering dept map: Course_ID → Department_ID
// Course_Department.Course_ID format ({quarterNum}_{courseCode}) matches
// CourseOffering.External_ID_4D__c — gives the exact department for each specific offering,
// not just the catalog-level department. Used for CourseOffering.Department__c.
function buildDeptByCourseIdMap(
  deptMap: Map<string, CourseDepartmentRow[]>,
): Map<string, string> {
  const result = new Map<string, string>();
  for (const [courseId, rows] of deptMap) {
    const primary = rows.filter((r) => parseBool(r.IsPrimary) === true);
    const candidates = primary.length > 0 ? primary : rows;
    const deptId = candidates[0]?.Department_ID?.trim();
    if (deptId && deptId !== "0") result.set(courseId, deptId);
  }
  return result;
}

async function resolveAccountByExtId(
  base: string,
  token: string,
  extId: string,
  cache: Map<string, string | null>,
): Promise<string | null> {
  const e = extId.trim();
  if (!e) return null;
  if (cache.has(e)) return cache.get(e)!;
  const rows = await sfQuery<{ Id: string }>(
    base,
    token,
    `SELECT Id FROM Account WHERE IsPersonAccount = false AND External_ID_4D__c = '${e.replace(/'/g, "\\'")}' LIMIT 1`,
  );
  const id = rows[0]?.Id ?? null;
  cache.set(e, id);
  return id;
}

interface DeptAccountInfo {
  childExtId: string;
  parentExtId: string | null;
}

// Matches FORCED_CATEGORY_IDS in departmentFlow.ts — the set of IDs that are
// saved as top-level "dept_cat_" Accounts rather than "dept_" child Accounts.
// Only used as a fallback guess (pre-warm cache keys, resolveDeptAccount's
// cache-miss query) — the row-loop lookups themselves are prefix-agnostic,
// driven off the Accounts actually present in Salesforce.
const DEPT_CAT_IDS = new Set([
  "1",
  "2",
  "3",
  "4",
  "5",
  "6",
  "7",
  "8",
  "9",
  "10",
]);
function deptExtId(deptId: string): string {
  return DEPT_CAT_IDS.has(deptId) ? `dept_cat_${deptId}` : `dept_${deptId}`;
}

async function resolveDeptAccount(
  sfBase: string,
  token: string,
  deptId: string,
  cache: Map<string, DeptAccountInfo | null>,
): Promise<DeptAccountInfo | null> {
  if (cache.has(deptId)) return cache.get(deptId) ?? null;
  const extId = deptExtId(deptId);
  const rows = await sfQuery<{
    External_ID_4D__c: string;
    Parent?: { External_ID_4D__c: string };
  }>(
    sfBase,
    token,
    `SELECT External_ID_4D__c, Parent.External_ID_4D__c FROM Account WHERE IsPersonAccount = false AND External_ID_4D__c = '${extId.replace(/'/g, "\\'")}' LIMIT 1`,
  );
  if (rows.length === 0) {
    cache.set(deptId, null);
    return null;
  }
  const info: DeptAccountInfo = {
    childExtId: rows[0].External_ID_4D__c,
    parentExtId: rows[0].Parent?.External_ID_4D__c ?? null,
  };
  cache.set(deptId, info);
  return info;
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
  Course_Summary?: string;
  Weekday?: string;
  Course_Time?: string;
  Building?: string;
  Room?: string;
  Drop_By_Date?: string;
  Additional_Fee?: string;
  Textbooks?: string;
  Reg_Open_Date?: string;
  Enrollment_Status?: string;
  Limited_Enrollment?: string;
  Instructor_Notes?: string;
  Other_Costs?: string;
  Other_Costs_Description?: string;
  Web_Enrollments_Remaining?: string;
  Web_Registration_Closed?: string;
  Cancelled?: string;
  Exception_Text?: string;
  Credit_NoCredit_Required?: string;
  Do_Not_Show_On_Web?: string;
  Program?: string;
  History?: string;
  Primary_Instructor?: string;
  Map_Link?: string;
  Other_Costs_Description_2?: string;
  Other_Costs_2?: string;
  RecID?: string;
  Primary_Associate?: string;
  No_Textbooks?: string;
  Roster_Email?: string;
  Evaluation_Link?: string;
  Dropped_Percent?: string;
  Dropped_Special_Percent?: string;
  Global_Eval?: string;
  Return_Rate?: string;
  No_Wait_List?: string;
  No_Discounts?: string;
  Canvas_Publish?: string;
  Directions_To_Class?: string;
  Additional_Info?: string;
  Waiver_Type?: string;
  Closed_DTS?: string;
  Room_Viewer_Link?: string;
  Breakeven_Enrollment?: string;
  Enroll_On_Start_Date?: string;
  Evaluation_Document_ID?: string;
  Format?: string;
  Survey_ID?: string;
  Survey_Close_Date?: string;
  Initial_Eval_Email_Time?: string;
  Registration_Message?: string;
  Coordinator_ID?: string;
  Do_Not_Solicit?: string;
  Additional_Description?: string;
  Tuition?: string;
  Tuition_Rule_ID?: string;
  Grade_Restriction?: string;
  Catalog_Details?: string;
  Student_Role?: string;
  Course_Version?: string;
  Catalog_Footer_Note?: string;
  Recording?: string;
  Instructor_Preferences?: string;
  Zoom_URL?: string;
  Zoom_PW?: string;
  Staff_Notes?: string;
  Add_Fee_Label?: string;
  Hybrid?: string;
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
  unrecognisedQuarters: Map<string, number>;
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
    let idField = "id"; // actual header name for the id column (resolved after first row)
    let dataRowIndex = 0;
    const rows: RawCourseRow[] = [];
    let aborted = false;
    let skippedByQuarter = 0;
    const unrecognisedQuarters = new Map<string, number>();

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
          // Locate the id column — handle uppercase and any residual BOM
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

        // Normalise to record.id regardless of original casing
        if (idField !== "id" && record[idField] !== undefined) {
          record.id = record[idField];
        }

        if (!(record.id ?? "").trim()) {
          dataRowIndex++;
          return;
        }

        // Quarter filter — skip rows outside the valid range
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

export const courseImport = flow({
  name: "Course Import",
  stableKey: "c1d2e3f4-1a2b-4c3d-8e4f-aa11bb22cc33",
  description:
    "Streams the course TSV from Google Drive one window at a time and upserts " +
    "Learning, LearningCourse, Location, CourseOffering, CourseOfferingSchedule, " +
    "and CourseOfferingParticipant records to Salesforce via REST API.",

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

    logger.info(`[Course Import] Starting at row ${startRow}`);

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars[
      "Google Drive Connection"
    ] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Course File ID"] as unknown as string;
    const instructorFileId = configVars[
      "Course Instructor File ID"
    ] as unknown as string | undefined;
    const deptFileId = configVars["Course Department File ID"] as unknown as
      string | undefined;
    const submissionFileId = configVars[
      "Course Submission File ID"
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

    // ── Pre-loop caches ───────────────────────────────────────────────────────
    logger.info("[Course Import] Building lookup caches…");
    const programCache = await buildFieldCache(
      sfBase,
      sfToken,
      "SELECT Id, Name FROM LearningProgram",
      "Name",
    );
    logger.info(`[Course Import] Caches — programs=${programCache.size}`);

    // ── Course-Instructor junction ────────────────────────────────────────────
    let courseInstructorMap = new Map<string, CourseInstructorRow[]>();
    if (instructorFileId) {
      try {
        courseInstructorMap = await loadCourseInstructors(
          instructorFileId,
          gdToken,
        );
        logger.info(
          `[Course Import] Course-Instructor junction loaded — ${courseInstructorMap.size} courses`,
        );
      } catch (err) {
        logger.warn(
          `[Course Import] Could not load Course-Instructor file: ${String(err)} — falling back to name lookup`,
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
          `[Course Import] Course-Associate junction loaded — ${courseAssociateMap.size} courses`,
        );
      } catch (err) {
        logger.warn(
          `[Course Import] Could not load Course-Associate file: ${String(err)} — falling back to Primary_Associate field`,
        );
      }
    }

    // ── Course-Department junction ────────────────────────────────────────────
    let deptByBaseCourseMap = new Map<string, string>();
    let deptByCourseIdMap = new Map<string, string>();
    let rawDeptMap = new Map<string, CourseDepartmentRow[]>();
    if (deptFileId) {
      try {
        rawDeptMap = await loadCourseDepartments(deptFileId, gdToken);
        // Catalog-level map (baseCode → deptId): used for LearningCourse.ProviderId
        deptByBaseCourseMap = buildDeptByBaseCourseMap(rawDeptMap);
        // Per-offering map (Course_ID → deptId): used for CourseOffering.Department__c
        deptByCourseIdMap = buildDeptByCourseIdMap(rawDeptMap);
        logger.info(
          `[Course Import] Course-Department junction loaded — ${rawDeptMap.size} courses, ${deptByBaseCourseMap.size} unique base courses, ${deptByCourseIdMap.size} offering-level entries`,
        );
      } catch (err) {
        logger.warn(
          `[Course Import] Could not load Course-Department file: ${String(err)} — department fields will not be set`,
        );
      }
    }

    // ── Course-Submission data ────────────────────────────────────────────────
    let courseSubmissionMap = new Map<string, CourseSubmissionRow>();
    if (submissionFileId) {
      try {
        courseSubmissionMap = await loadCourseSubmissions(
          submissionFileId,
          gdToken,
        );
        logger.info(
          `[Course Import] Course-Submission data loaded — ${courseSubmissionMap.size} entries`,
        );
      } catch (err) {
        logger.warn(
          `[Course Import] Could not load Course-Submission file: ${String(err)} — submission fields will not be set`,
        );
      }
    }

    const personByNameCache = new Map<string, string | null>();
    const personByExtIdCache = new Map<string, string | null>();
    const deptAccountCache = new Map<string, string | null>();
    const deptWithParentCache = new Map<string, DeptAccountInfo | null>();
    const locationCache = new Map<string, string>();

    // ── Pre-load full dept hierarchy once ────────────────────────────────────
    // One SOQL at startup populates deptWithParentCache completely so that every
    // resolveDeptAccount() call during the row loop is an instant cache hit.
    try {
      const deptAccounts = await sfQuery<{
        External_ID_4D__c: string;
        Parent?: { External_ID_4D__c: string };
      }>(
        sfBase,
        sfToken,
        `SELECT External_ID_4D__c, Parent.External_ID_4D__c FROM Account WHERE IsPersonAccount = false AND External_ID_4D__c LIKE 'dept%'`,
      );
      for (const da of deptAccounts) {
        const extId = da.External_ID_4D__c;
        const rawId = extId.startsWith("dept_cat_")
          ? extId.slice("dept_cat_".length)
          : extId.startsWith("dept_")
            ? extId.slice("dept_".length)
            : null;
        if (!rawId) continue;
        deptWithParentCache.set(rawId, {
          childExtId: extId,
          parentExtId: da.Parent?.External_ID_4D__c ?? null,
        });
      }
      logger.info(
        `[Course Import] Dept hierarchy pre-loaded — ${deptWithParentCache.size} accounts`,
      );
    } catch (err) {
      logger.warn(
        `[Course Import] Could not pre-load dept hierarchy: ${String(err)} — will query on demand`,
      );
    }

    // ── Stream TSV window ─────────────────────────────────────────────────────
    const validQuarters = buildValidQuarters(ANCHOR_QUARTER, YEARS_BACK);
    logger.info(
      `[Course Import] Streaming rows ${startRow}–${startRow + MAX_ROWS - 1}… valid quarters: ${[...validQuarters].join(", ")}`,
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
      `[Course Import] Parsed ${rows.length} rows, skipped ${skippedByQuarter} (quarter filter) (hasMore=${hasMore})`,
    );
    if (unrecognisedQuarters.size > 0) {
      const detail = [...unrecognisedQuarters.entries()]
        .map(([q, n]) => `"${q}" ×${n}`)
        .join(", ");
      logger.warn(
        `[Course Import] WARNING — ${skippedByQuarter} rows skipped due to unrecognised/out-of-range Quarter values: ${detail}. Valid quarters are: ${[...validQuarters].join(", ")}`,
      );
    }

    // ── Pre-warm caches for this window ──────────────────────────────────────
    // Fires 3 parallel batch SOQLs to populate dept/instructor/associate caches
    // before the row loop, eliminating cold SOQL misses on every first-seen ID.
    if (rows.length > 0) {
      const allDeptExtIds = new Set<string>();
      for (const row of rows) {
        const bc = stripSectionSuffix(str(row.Code)) || str(row.Code);
        const sid = str(row.id);
        const pd = deptByBaseCourseMap.get(bc);
        if (pd) allDeptExtIds.add(deptExtId(pd));
        const od = deptByCourseIdMap.get(sid);
        if (od) allDeptExtIds.add(deptExtId(od));
        for (const dr of rawDeptMap.get(sid) ?? []) {
          if (dr.Department_ID?.trim())
            allDeptExtIds.add(deptExtId(dr.Department_ID.trim()));
        }
      }

      const allInstrIds = new Set<string>();
      const allAssocIds = new Set<string>();
      const allLocationNames = new Set<string>();
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
        const building = str(row.Building);
        if (building) {
          const locName = buildLocationName(building, str(row.Room));
          if (locName) allLocationNames.add(locName);
        }
      }

      await Promise.all([
        allDeptExtIds.size > 0
          ? sfQuery<{ Id: string; External_ID_4D__c: string }>(
              sfBase,
              sfToken,
              `SELECT Id, External_ID_4D__c FROM Account WHERE IsPersonAccount = false AND External_ID_4D__c IN (${[...allDeptExtIds].map((id) => `'${id.replace(/'/g, "\\'")}'`).join(",")})`,
            ).then((rs) =>
              rs.forEach((r) =>
                deptAccountCache.set(r.External_ID_4D__c, r.Id),
              ),
            )
          : Promise.resolve(),

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

        allLocationNames.size > 0
          ? sfQuery<{ Id: string; Name: string }>(
              sfBase,
              sfToken,
              `SELECT Id, Name FROM Location WHERE Name IN (${[...allLocationNames].map((n) => `'${n.replace(/'/g, "\\'")}'`).join(",")})`,
            ).then(async (rs) => {
              rs.forEach((r) => locationCache.set(r.Name, r.Id));
              const missing = [...allLocationNames].filter(
                (n) => !locationCache.has(n),
              );
              if (missing.length > 0)
                await Promise.all(
                  missing.map(async (name) => {
                    const id = await sfCreate(sfBase, sfToken, "Location", {
                      Name: name,
                    });
                    locationCache.set(name, id);
                  }),
                );
            })
          : Promise.resolve(),
      ]);

      logger.info(
        `[Course Import] Pre-warmed — depts:${allDeptExtIds.size} instrs:${allInstrIds.size} assocs:${allAssocIds.size} locs:${allLocationNames.size}`,
      );
    }

    // ── Counters ──────────────────────────────────────────────────────────────
    const counts = {
      learning: { ok: 0, err: 0 },
      learningCourse: { ok: 0, err: 0 },
      location: { ok: 0, skipped: 0, err: 0 },
      courseOffering: { ok: 0, err: 0 },
      schedule: { ok: 0, skipped: 0, err: 0 },
      copInstructor: { ok: 0, skipped: 0, err: 0 },
      copAssociate: { ok: 0, skipped: 0, err: 0 },
      termUpdate: { ok: 0, skipped: 0, err: 0 },
      courseDept: { ok: 0, skipped: 0, err: 0 },
    };

    const successRows: SuccessRow[] = [];
    const errorRows: ErrorRow[] = [];

    // Post-offering tasks collected during the row loop, fired all at once after.
    const postOfferingTasks: Array<() => Promise<void>> = [];

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

      // Resolve catalog-level department before Learning upsert so ProviderId is available
      let rowDeptAccountId: string | null = null;
      const primaryDeptId = deptByBaseCourseMap.get(baseCode) ?? null;
      if (primaryDeptId) {
        rowDeptAccountId = await resolveAccountByExtId(
          sfBase,
          sfToken,
          deptExtId(primaryDeptId),
          deptAccountCache,
        );
        if (!rowDeptAccountId) {
          logger.warn(
            `[Course Import] Row ${startRow + i} (${sourceId}) Dept "${primaryDeptId}" not found in Salesforce`,
          );
        }
      }

      // 1. Learning
      const learningPayload: Record<string, unknown> = {
        Name: (title || baseCode).slice(0, 255),
        Type: "LearningCourse",
        IsActive: true,
      };
      if (rowDeptAccountId) learningPayload.ProviderId = rowDeptAccountId;
      try {
        const r = await sfUpsert(
          sfBase,
          sfToken,
          "Learning",
          "External_ID_4D__c",
          baseCode,
          learningPayload,
        );
        counts.learning.ok++;
        ok("Learning", r.id, r.created, {
          External_ID_4D__c: baseCode,
          ...learningPayload,
        });
      } catch (err) {
        counts.learning.err++;
        const msg = sfErrMsg(err);
        logger.error(
          `[Course Import] Row ${startRow + i} (${sourceId}) Learning: ${msg}`,
        );
        fail("Learning", msg, {
          External_ID_4D__c: baseCode,
          ...learningPayload,
        });
        continue;
      }

      // 2. LearningCourse — upsert by External_ID_4D__c, parent Learning referenced by External ID
      const lc: Record<string, unknown> = {
        Name: (title || baseCode).slice(0, 255),
        CourseNumber: baseCode.replace(/\s+/g, ""),
      };
      const catalogNotes = normaliseCatalogNotes(row.Catalog_Notes);
      if (catalogNotes) lc.Catalog_Notes__c = catalogNotes;
      const description = normaliseCatalogNotes(row.Description);
      if (description) lc.Description = description.slice(0, 32000);
      const textbooks = normaliseCatalogNotes(row.Textbooks);
      if (textbooks) lc.Textbooks__c = textbooks.slice(0, 255);
      // Automation excluded — upsert directly by External_ID_4D__c, parent Learning referenced by External ID
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
          `[Course Import] Row ${startRow + i} (${sourceId}) LearningCourse: ${sfErrMsg(err)}`,
        );
        fail("LearningCourse", sfErrMsg(err), lc);
      }

      // 3. Location
      let locationId: string | null = null;
      if (str(row.Building)) {
        try {
          locationId = await resolveLocation(
            sfBase,
            sfToken,
            str(row.Building),
            str(row.Room),
            locationCache,
          );
          if (locationId) {
            counts.location.ok++;
            ok("Location", locationId, false, {
              Building: str(row.Building),
              Room: str(row.Room),
            });
          } else {
            counts.location.skipped++;
          }
        } catch (err) {
          counts.location.err++;
          const msg = sfErrMsg(err);
          logger.warn(
            `[Course Import] Row ${startRow + i} (${sourceId}) Location: ${msg}`,
          );
          fail("Location", msg, {
            Building: str(row.Building),
            Room: str(row.Room),
          });
        }
      } else {
        counts.location.skipped++;
      }

      // 4. Primary Instructor — resolve via Course-Instructor junction
      // junctionRows: all instructor records for this course, sorted so IsPrimary=true comes first
      const junctionRows = (courseInstructorMap.get(sourceId) ?? []).sort(
        (a, b) =>
          (parseBool(b.IsPrimary) ? 1 : 0) - (parseBool(a.IsPrimary) ? 1 : 0),
      );

      let primaryInstructorId: string | null = null;

      if (junctionRows.length > 0) {
        // Resolve primary instructor from junction (IsPrimary=true or first row)
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
                `[Course Import] Row ${startRow + i} (${sourceId}) Primary instructor not found: Instructor_ID="${instrExtId}"`,
              );
          } catch (err) {
            logger.warn(
              `[Course Import] Row ${startRow + i} (${sourceId}) Instructor lookup: ${sfErrMsg(err)}`,
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
              `[Course Import] Row ${startRow + i} (${sourceId}) Instructor not found by name: "${str(row.Primary_Instructor)}"`,
            );
        } catch (err) {
          logger.warn(
            `[Course Import] Row ${startRow + i} (${sourceId}) Instructor name lookup: ${sfErrMsg(err)}`,
          );
        }
      }

      // 5. LearningProgram — disabled until LearningId requirement is resolved
      // const learningProgramId: string | null = null;
      // const programName = str(row.Program);
      // if (programName) {
      //   learningProgramId = programCache.get(programName) ?? null;
      //   if (!learningProgramId) {
      //     try {
      //       learningProgramId = await sfCreate(sfBase, sfToken, "LearningProgram", { Name: programName });
      //       programCache.set(programName, learningProgramId);
      //     } catch (err) {
      //       logger.warn(`[Course Import] Row ${startRow + i} (${sourceId}) LearningProgram: ${sfErrMsg(err)}`);
      //     }
      //   }
      // }

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
              `[Course Import] Row ${startRow + i} (${sourceId}) Coordinator not found: Coordinator_ID="${coordExtId}"`,
            );
        } catch (err) {
          logger.warn(
            `[Course Import] Row ${startRow + i} (${sourceId}) Coordinator lookup: ${sfErrMsg(err)}`,
          );
        }
      }

      // 6. CourseOffering
      let courseOfferingId: string | null = null;
      const isCancelled = parseBool(row.Cancelled) === true;
      // Remap pre-2013 quarters to their 2013 equivalent (fa08 → fa13, wi11 → wi13, etc.)
      const effectiveQuarter = (() => {
        const m = /^([a-z]+)(\d+)$/i.exec(quarter);
        if (m && parseInt(m[2], 10) < 13) return `${m[1].toLowerCase()}13`;
        return quarter;
      })();
      const co: Record<string, unknown> = {
        Name: code || sourceId,
        ...(sectionSuffix ? { SectionNumber: sectionSuffix } : {}),
        Enrollment_Status__c: normaliseEnrollmentStatus(
          row.Enrollment_Status,
          isCancelled,
        ),
      };
      co.LearningCourse = { External_ID_4D__c: baseCode };
      if (effectiveQuarter)
        co.AcademicSession = { Abbreviation__c: effectiveQuarter };
      if (primaryInstructorId) co.PrimaryFacultyId = primaryInstructorId;
      if (coordId) co.Coordinator__c = coordId;
      try {
        // Per-offering department: direct join on sourceId = Course_Department.Course_ID
        // Architect: Course_Department.Course_ID matches CourseOffering.External_ID_4D__c
        // giving each offering its own department snapshot (not catalog-level average)
        const offeringDeptId = deptByCourseIdMap.get(sourceId) ?? null;
        if (offeringDeptId) {
          // Resolve against the actual Account in Salesforce (prefix-agnostic —
          // works whether this ID is a "dept_" child or a "dept_cat_" category)
          // instead of guessing the prefix from a hardcoded ID list. A miss just
          // omits the field rather than failing the whole CourseOffering upsert.
          const offeringDeptInfo = await resolveDeptAccount(
            sfBase,
            sfToken,
            offeringDeptId,
            deptWithParentCache,
          );
          if (offeringDeptInfo)
            co.Department__r = {
              External_ID_4D__c: offeringDeptInfo.childExtId,
            };
          else
            logger.warn(
              `[Course Import] Row ${startRow + i} (${sourceId}) CourseOffering.Department__r: dept ${offeringDeptId} not found — field omitted`,
            );
        }

        // if (learningProgramId) co.LearningProgramId = learningProgramId; // LearningProgram disabled

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
        setStr("Course_Summary__c", row.Course_Summary);
        setDate("Drop_By_Date__c", row.Drop_By_Date);
        setFloat("Additional_Fee__c", row.Additional_Fee);
        setFloat("Tuition_Amount__c", row.Tuition);
        const staffNotes = str(row.Staff_Notes);
        if (staffNotes) co.Staff_Notes__c = staffNotes;
        const instrNotes = str(row.Instructor_Notes);
        if (instrNotes) co.Instructor_Notes__c = instrNotes;
        setFloat("Other_Costs__c", row.Other_Costs);
        const otherCostsDesc = normaliseCatalogNotes(
          row.Other_Costs_Description,
        );
        if (otherCostsDesc) co.Other_Costs_Description__c = otherCostsDesc;
        setInt("Web_Enrollments_Remaining__c", row.Web_Enrollments_Remaining);
        setBool("Web_Registration_Closed__c", row.Web_Registration_Closed);
        setBool("Limited_Enrollment__c", row.Limited_Enrollment);
        const exceptionText = normaliseCatalogNotes(row.Exception_Text);
        if (exceptionText) co.Exception_Text__c = exceptionText;
        setBool("Credit_NoCredit_Required__c", row.Credit_NoCredit_Required);
        setBool("Do_Not_Show_On_Web__c", row.Do_Not_Show_On_Web);
        setStr("Map_Link__c", row.Map_Link);
        const otherCostsDesc2 = normaliseCatalogNotes(
          row.Other_Costs_Description_2,
        );
        if (otherCostsDesc2) co.Other_Costs_Description_2__c = otherCostsDesc2;
        setFloat("Other_Costs_2__c", row.Other_Costs_2);
        setBool("No_Textbooks__c", row.No_Textbooks);
        const rEmail = normaliseRosterEmail(row.Roster_Email);
        if (rEmail !== null) co.Roster_Email__c = rEmail;
        setStr("Evaluation_Link__c", row.Evaluation_Link);
        const droppedPct = parseFloatVal(row.Dropped_Percent);
        if (droppedPct !== null && droppedPct <= 100)
          co.Dropped_Percent__c = droppedPct;
        const droppedSpecialPct = parseFloatVal(row.Dropped_Special_Percent);
        if (droppedSpecialPct !== null && droppedSpecialPct <= 100)
          co.Dropped_Special_Percent__c = droppedSpecialPct;
        setFloat("Global_Eval__c", row.Global_Eval);
        setFloat("Return_Rate__c", row.Return_Rate);
        setBool("No_Discounts__c", row.No_Discounts);
        setBool("Canvas_Publish__c", row.Canvas_Publish);
        const directionsToClass = normaliseCatalogNotes(
          row.Directions_To_Class,
        );
        if (directionsToClass) co.Directions_To_Class__c = directionsToClass;
        const additionalInfo = normaliseCatalogNotes(row.Additional_Info);
        if (additionalInfo) co.Additional_Info__c = additionalInfo;
        const waiverType = normaliseWaiverType(row.Waiver_Type);
        if (waiverType) co.Waiver_Type__c = waiverType;
        const cdts = parseClosedDTS(row.Closed_DTS);
        if (cdts) co.Registration_Close_Date__c = cdts;
        const roomViewerLink = normaliseHttpsUrl(row.Room_Viewer_Link);
        if (roomViewerLink) co.Room_Viewer_Link__c = roomViewerLink;
        setInt("Breakeven_Enrollment__c", row.Breakeven_Enrollment);
        setInt("Enroll_On_Start_Date__c", row.Enroll_On_Start_Date);
        const evalDocId = normaliseEvalDocId(row.Evaluation_Document_ID);
        if (evalDocId) co.Evaluation_Document_ID__c = evalDocId;
        const fmt =
          normaliseFormat(row.Format, row.Hybrid) ??
          formatFromSuffix(sectionSuffix);
        if (fmt) co.Format__c = fmt;
        const surveyId = normaliseSurveyId(row.Survey_ID);
        if (surveyId) co.Survey_ID__c = surveyId;
        setDate("Survey_Close_Date__c", row.Survey_Close_Date);
        const evalTime = str(row.Initial_Eval_Email_Time);
        if (evalTime && evalTime !== "0:00:00" && evalTime !== "00:00:00")
          co.Initial_Eval_Email_Time__c = evalTime;
        const tuitionRuleRaw = str(row.Tuition_Rule_ID)
          .trim()
          .replace(/\.0$/, "");
        if (tuitionRuleRaw && tuitionRuleRaw !== "0") {
          co.Tuition_Rule__r = { External_ID_4D__c: tuitionRuleRaw };
        }
        const gradeRestriction = normaliseGradeRestriction(
          row.Grade_Restriction,
        );
        if (gradeRestriction) co.Grade_Restriction__c = gradeRestriction;
        const studentRole = normaliseStudentRole(row.Student_Role);
        if (studentRole) co.Student_Role__c = studentRole;
        setStr("Course_Version__c", row.Course_Version);
        // setStr("Catalog_Footer_Note__c", row.Catalog_Footer_Note); // not in mapping workbook
        setBool("Recording__c", row.Recording);
        const instrPrefs = normaliseInstructorPrefs(row.Instructor_Preferences);
        if (instrPrefs) co.Textbook_Instructor_Preferences__c = instrPrefs;
        const zoomUrl = normaliseHttpsUrl(row.Zoom_URL);
        if (zoomUrl) co.Zoom_URL__c = zoomUrl;
        const zoomPw = normaliseZoomPw(row.Zoom_PW);
        if (zoomPw) co.Zoom_Password__c = zoomPw;
        const addFeeLabel = normaliseAddFeeLabel(row.Add_Fee_Label);
        if (addFeeLabel) co.Add_Fee_Label__c = addFeeLabel;
        // setBool("No_Wait_List__c", row.No_Wait_List); // field not yet in org
        // setBool("Do_Not_Solicit__c", row.Do_Not_Solicit); // field not yet in org
        // setStr("Registration_Message__c", row.Registration_Message); // not in mapping workbook
        // setStr("Additional_Description__c", row.Additional_Description); // not in mapping workbook

        // Course_Submission fields — joined on Course_Code|Course_Quarter
        const submissionKey = `${code}|${quarter.toLowerCase()}`;
        const submission = courseSubmissionMap.get(submissionKey);
        if (submission) {
          setFloat("Occurrences__c", submission.Course_Occurrences);
          setFloat("Hours_Per_Occurrence__c", submission.Course_Hours_Per);
          setFloat("Total_Hours__c", submission.Course_Total_Hours);
          setFloat("Async_Hours__c", submission.Course_Async_Hours);
          const tuitionAdj = parseFloatVal(submission.Tuition_Adjustment);
          if (tuitionAdj !== null) co.Tuition_Adjustment__c = tuitionAdj;
          const tuitionAdjNote = str(submission.Tuition_Adjustment_Note).trim();
          if (tuitionAdjNote) co.Tuition_Adjustment_Note__c = tuitionAdjNote;
        }

        // Log required-field values so missing ones are visible in logs
        logger.info(
          `[Course Import] Row ${startRow + i} (${sourceId}) CourseOffering required fields:` +
            `\n  LearningCourse     = ${baseCode} (External_ID_4D__c)` +
            `\n  AcademicSession    = ${effectiveQuarter || "NULL"} (Abbreviation__c)` +
            `\n  MaxEnrollments__c  = ${co.MaxEnrollments__c ?? "NULL"} | EnrollmentCapacity = ${co.EnrollmentCapacity ?? "NULL"}` +
            `\n  PrimaryFacultyId   = ${co.PrimaryFacultyId ?? "NULL"}` +
            `\n  Tuition_Rule__r    = ${tuitionRuleRaw || "NULL"}`,
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
          `[Course Import] Row ${startRow + i} (${sourceId}) CourseOffering: ${msg}`,
        );
        fail("CourseOffering", msg, { External_ID_4D__c: sourceId, ...co });
        continue;
      }

      // 7-10. Post-offering operations — all independent once courseOfferingId is set.
      // Run Course_Department, Schedule, COP-Instructor, COP-Associate in parallel.
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
          // Task A: Course_Department__c junction — only IsPrimary=True rows (Jul 2 decision)
          (async () => {
            for (const deptRow of rawDeptMap.get(sourceId) ?? []) {
              if (parseBool(deptRow.IsPrimary) !== true) {
                counts.courseDept.skipped++;
                continue;
              }
              const deptId = deptRow.Department_ID?.trim();
              if (!deptId || deptId === "0") {
                counts.courseDept.skipped++;
                continue;
              }
              const deptInfo = await resolveDeptAccount(
                sfBase,
                sfToken,
                deptId,
                deptWithParentCache,
              );
              if (!deptInfo) {
                counts.courseDept.skipped++;
                logger.warn(
                  `[Course Import] Row ${startRow + i} (${sourceId}) Course_Department__c: dept_${deptId} not found — skipped`,
                );
                continue;
              }
              const cdExtId = deptRow.ID?.trim()
                ? `CDEPT-${deptRow.ID.trim()}`
                : null;
              if (!cdExtId) {
                counts.courseDept.skipped++;
                continue;
              }
              const junctionPayload: Record<string, unknown> = {
                Course_Offering__r: { External_ID_4D__c: sourceId },
                Child_Department__r: { External_ID_4D__c: deptInfo.childExtId },
                IsPrimary__c: parseBool(deptRow.IsPrimary) ?? false,
              };
              // Parent_Department__r only set when the dept actually has a
              // parent. Categories (e.g. dept_cat_4) are top-level and have
              // none — leave the field empty rather than self-referencing
              // Parent = Child, which was the old fallback behaviour.
              if (deptInfo.parentExtId)
                junctionPayload.Parent_Department__r = {
                  External_ID_4D__c: deptInfo.parentExtId,
                };
              try {
                const r = await sfUpsert(
                  sfBase,
                  sfToken,
                  "Course_Department__c",
                  "External_ID_4D__c",
                  cdExtId,
                  junctionPayload,
                );
                counts.courseDept.ok++;
                ok("Course_Department__c", r.id, r.created, {
                  External_ID_4D__c: cdExtId,
                  ...junctionPayload,
                });
              } catch (err) {
                counts.courseDept.err++;
                const msg = sfErrMsg(err);
                logger.warn(
                  `[Course Import] Row ${startRow + i} (${sourceId}) Course_Department__c dept_${deptId}: ${msg}`,
                );
                fail("Course_Department__c", msg, {
                  External_ID_4D__c: cdExtId,
                  ...junctionPayload,
                });
              }
            }
          })(),

          // Task B: CourseOfferingSchedule
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
                  if (locationId) sched.LocationId = locationId;
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
                  `[Course Import] Row ${startRow + i} (${sourceId}) Schedule: ${msg}`,
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

          // Task C: COP-Instructor
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
                  `[Course Import] Row ${startRow + i} (${sourceId}) COP-Instructor: ${msg}`,
                );
                fail("COP-Instructor", msg, instrExternal);
              }
            }
          })(),

          // Task D: COP-Associate
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
                // PDF update: ParticipantAffiliation now derives from Speaking_Date
                // instead of always "Associate" — null/placeholder ("00/00/00",
                // "00/00/0000") date = Course Support Specialist, real date = Guest Speaker.
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
                    // Match any prior-run Associate-family value so a re-run
                    // doesn't create a duplicate under the new affiliation label.
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
                      `[Course Import] Row ${startRow + i} (${sourceId}) COP-Associate: ${msg}`,
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
                    `[Course Import] Row ${startRow + i} (${sourceId}) COP-Associate: ${msg}`,
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
                  // No Speaking_Date exists for fallback (Primary_Associate-sourced)
                  // rows — no date is the same as a null date, so this path always
                  // resolves to Course Support Specialist.
                  const fallbackAffiliation = "Course Support Specialist";
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
                      `[Course Import] Row ${startRow + i} (${sourceId}) ${msg}`,
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
                    `[Course Import] Row ${startRow + i} (${sourceId}) COP-Associate: ${msg}`,
                  );
                  fail("COP-Associate", msg, {
                    CourseOfferingId: courseOfferingId,
                    Primary_Associate: assocRaw,
                    ParticipantAffiliation: "Course Support Specialist",
                  });
                }
              } else {
                counts.copAssociate.skipped++;
              }
            }
          })(),
        ]).then(() => undefined),
      );

      // 11. AcademicTerm RegistrationOpenDate — disabled: field not yet mapped in org
      // const regOpenDate = parseCourseDate(row.Reg_Open_Date);
      // if (regOpenDate && quarter) {
      //   const termId = termCache.get(effectiveQuarter) ?? null;
      //   if (termId) {
      //     try {
      //       await sfUpdate(sfBase, sfToken, "AcademicTerm", termId, {
      //         RegistrationOpenDate: regOpenDate,
      //       });
      //       counts.termUpdate.ok++;
      //       ok("AcademicTerm", termId, false);
      //     } catch (err) {
      //       counts.termUpdate.err++;
      //       const msg = sfErrMsg(err);
      //       logger.warn(
      //         `[Course Import] Row ${startRow + i} (${sourceId}) AcademicTerm update: ${msg}`,
      //       );
      //       fail("AcademicTerm", msg);
      //     }
      //   } else {
      //     const msg = `AcademicTerm not found for quarter "${quarter}"`;
      //     logger.warn(
      //       `[Course Import] Row ${startRow + i} (${sourceId}) ${msg}`,
      //     );
      //     counts.termUpdate.skipped++;
      //     fail("AcademicTerm", msg);
      //   }
      // } else {
      //   counts.termUpdate.skipped++;
      // }
      counts.termUpdate.skipped++;
    }

    // ── Batch post-offering operations — all rows' Dept/Schedule/COP fire together ──
    if (postOfferingTasks.length > 0) {
      logger.info(
        `[Course Import] Firing post-offering batch for ${postOfferingTasks.length} rows simultaneously`,
      );
      await Promise.all(postOfferingTasks.map((t) => t()));
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    const RESULT_OBJECTS = [
      "Learning",
      "LearningCourse",
      "Location",
      "CourseOffering",
      "CourseOfferingSchedule",
      "COP-Instructor",
      "COP-Associate",
      "AcademicTerm",
      "Course_Department__c",
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
          flowName: "Course Import",
          objects: objectData,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: courseSheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Course Import] Results sheet: ${sheet.url}`);
      } catch (err) {
        logger.warn(
          `[Course Import] Could not update results sheet: ${String(err)}`,
        );
      }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Course Import] Window summary (rows ${startRow}–${nextStartRow - 1}):` +
        `\n  Learning:             ${counts.learning.ok} ok, ${counts.learning.err} err` +
        `\n  LearningCourse:       ${counts.learningCourse.ok} ok, ${counts.learningCourse.err} err` +
        `\n  Location:             ${counts.location.ok} ok, ${counts.location.skipped} skip, ${counts.location.err} err` +
        `\n  CourseOffering:       ${counts.courseOffering.ok} ok, ${counts.courseOffering.err} err` +
        `\n  Schedule:             ${counts.schedule.ok} ok, ${counts.schedule.skipped} skip, ${counts.schedule.err} err` +
        `\n  COP-Instructor:       ${counts.copInstructor.ok} ok, ${counts.copInstructor.skipped} skip, ${counts.copInstructor.err} err` +
        `\n  COP-Associate:        ${counts.copAssociate.ok} ok, ${counts.copAssociate.skipped} skip, ${counts.copAssociate.err} err` +
        `\n  AcademicTerm RegOpen: ${counts.termUpdate.ok} ok, ${counts.termUpdate.skipped} skip, ${counts.termUpdate.err} err` +
        `\n  Course_Department__c: ${counts.courseDept.ok} ok, ${counts.courseDept.skipped} skip, ${counts.courseDept.err} err`,
    );

    // ── Recurse ───────────────────────────────────────────────────────────────
    if (hasMore && (!TEST_MODE || nextStartRow < TEST_MAX_ROWS)) {
      logger.info(
        `[Course Import] Invoking next iteration at startRow=${nextStartRow}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Course Import", {
        startRow: nextStartRow,
        courseSheetId: nextSheetId,
      });
    } else {
      logger.info("[Course Import] All rows processed — import complete.");
    }

    return { data: { startRow, rowsProcessed: rows.length, hasMore, counts } };
  },
});

export default [courseImport];
