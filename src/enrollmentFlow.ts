/**
 * Stanford CSP Migration – Enrollment Import flow.
 *
 * Streams the Enrollment TSV from Google Drive, deduplicates by
 * (Registration_ID + Student_ID + Course_ID), and bulk-upserts
 * CourseOfferingParticipant records via Bulk API 2.0. Once every COP batch
 * has fully completed, a second phase creates one CourseOfferingPtcpResult
 * child record for each COP with a populated Grade (LetterGrade = Grade__c,
 * ParticipantResultStatus = see mapParticipantResultStatus()) — reusing the
 * same in-memory deduped rows, no second file read. Must run after COP,
 * since a Result's lookup to its parent COP requires that COP to already
 * exist in Salesforce.
 *
 * ParticipantResultStatus: the mapping doc said to always send "Final", but
 * that field's real picklist only has Pass/Fail/Incomplete/Withdraw (locked
 * by the standard package — "Final" can't be added). Architect confirmed
 * (Sep 28) the Grade → status mapping: A+/A/A-/B+/B/B-/C+/C/C-/CR/NGR →
 * Pass; D+/D/D-/NC/NP → Fail. Any other grade value has no confirmed
 * mapping yet and is skipped (logged, not guessed).
 *
 * PREREQUISITES (must run before this flow):
 *   1. Student flow       — loads Person Account / Contact (Student_ID_4D__c)
 *   2. Course flow        — loads CourseOffering (External_ID_4D__c)
 *   3. Registration flow  — NOTE: does NOT load AcademicTermEnrollment (it
 *      only creates CardPaymentMethod/Order/PaymentGroup/Payment — see
 *      AcademicTermEnrollmentId note below). Still a prerequisite in case
 *      that changes.
 *
 * AcademicTermEnrollmentId is NOT SET on COP (see mapToEnrollmentCop).
 * Architect (Sep 28): AcademicTermEnrollment is not populated for
 * registrations going forward, so past data should not populate it either —
 * doing so could cause issues with the build. No flow creates it.
 *
 * External ID: "ENR-{Enrollment.ID}"  e.g. "ENR-200058"
 *   Prefixed to avoid collision with CINST-{n} / CASSOC-{n} instructor/associate COPs.
 *   Enrollment Waiver flow must look up COPs using this same "ENR-" prefix.
 *
 * DEDUP RULE (Jul 2 refinement; Status handling updated per architect, Sep 18):
 *   When a student drops, 4D creates 2–3 enrollment rows for the same
 *   Registration_ID + Student_ID + Course_ID. Only one COP is created in SF
 *   for that group. Terminal-status precedence (highest rank wins):
 *     Drop variants (10–7) > Cancel variants (6) > Drop (5) > UnEnrolled (4)
 *     > DupEnrollment (2) > Enrolled / Wait List (1)
 *     > New (0 — excluded entirely per Amy Jul 2: abandoned cart)
 *   EXCEPTION — Adjustment: per architect (Sep 18), "Adjustment" rows are
 *   NEVER folded into this group ranking. 4D creates a separate Adjustment
 *   row alongside the primary Enrolled/Dropped row when staff process a late
 *   refund/tuition change, and both must produce their own COP in Salesforce
 *   ("bring in both records"). See buildDedupMap().
 *
 * STATUS → ParticipationStatus (all resolved as of Sep 18 architect update):
 *   - Enrolled → "Enrolled"; Wait List → "Waitlisted"; UnEnrolled → "Dropped"
 *   - All Drop variants (Drop w/Refund, Drop No Refund, Drop Transferred,
 *     Drop - Pending, Drop, and 1-record junk variants) → "Dropped" (flattened
 *     — supersedes the earlier plan to preserve refund detail as separate
 *     picklist values; architect, Sep 18: "All dropped statuses will need to
 *     be set as 'Dropped'")
 *   - Course cancel, Cancelled - Refunded, Cancelled - Pending → "Cancelled"
 *     (architect, Sep 18 — confirmed "Course cancel" is included)
 *   - Adjustment → "Adjustment" (new status; architect, Sep 18)
 *   - DupEnrollment → "DupEnrollment" (new status; architect confirmed: keep
 *     these records as-is, do not exclude or remap to "Enrolled")
 *   - New → excluded entirely (abandoned cart, Amy Jul 2); blank/unknown → null
 *   ⚠ PREREQUISITE: Salesforce's ParticipationStatus picklist must have
 *   "Cancelled", "Adjustment", and "DupEnrollment" added as values before
 *   this flow runs — only "Enrolled", "Waitlisted", "Dropped", "Completed"
 *   exist today. Records will fail to upsert until these are added.
 *
 * OPEN ITEMS:
 *   - TA_Discount sign convention: RESOLVED — approved to store as negative
 *     (matches source format).
 *   - Audit fields (CreatedDate, LastModifiedDate): Requires "Set Audit Fields
 *     upon Record Creation" and "Create Audit Fields" permissions for the
 *     migration user — in progress (self-service, Salesforce admin task).
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse } from "papaparse";
import { str, toDate, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// ── Constants ─────────────────────────────────────────────────────────────────
const SF_API = "v60.0";
// Smaller than the 50,000 used elsewhere in this project (Registration,
// Transcript Request, Textbook) — deliberately, as extra safety margin after
// this flow hit the 1GB memory limit twice building/uploading a 50K-record
// CSV batch on top of everything else already in memory at that point.
// Sequential batches cost nothing extra either way (each is awaited before
// the next starts), so a smaller size only adds a couple more round-trips.
const BULK_BATCH_SIZE = 20_000;
const SF_OBJECT = "CourseOfferingParticipant";
const EXT_ID_FIELD = "External_ID_4D__c";
const TEST_MODE = false;  // set to false to process all records
const TEST_LIMIT = 1000;  // max records to upsert when TEST_MODE is true

// Course Flow only migrates CourseOffering records within this same 2-year
// window (ANCHOR_QUARTER = "wi25" everywhere else in this project). An
// enrollment for a course outside that window has no CourseOffering to link
// to — it was never in scope — so it's skipped here the same way, instead of
// being submitted and failing on a missing required CourseOfferingId.
const ANCHOR_QUARTER = "wi25";
const YEARS_BACK = 2;

// 4D quarter number format: {YYYY}{digit} — fa=1, wi=2, sp=3, su=4
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

// ── Raw row type ──────────────────────────────────────────────────────────────

// NOTE: intentionally holds ONLY the columns actually read anywhere in this
// file (Program, Source, Other_Costs, Created_By, Last_Modified_By,
// Grade_Printed_Date, Student_ID_Previous, Prior_Enrollment_ID,
// STAP_Report_Run_Date's sibling No_Discounts, and Instructor_ID are all
// "Do Not Map" and dropped). This is deliberate, not an oversight: with
// ~500K rows surviving dedup and held in memory simultaneously, every extra
// column here is ~500K extra string properties — see NEEDED_COLUMNS /
// buildDedupMap() below, which was hitting Prismatic's 1GB execution memory
// limit before this trim (and before switching off dynamic/dictionary-mode
// object construction).
interface RawEnrollmentRow {
  ID?: string;
  Registration_ID?: string;
  Student_ID?: string;
  Course_ID?: string;
  Grade_Option?: string;
  Grade?: string;
  Grade_Entered_Date?: string;
  Status?: string;
  Notes?: string;
  Tuition?: string;
  Fee?: string;
  Add_Drop?: string;
  Extension?: string;
  TA_Discount?: string;
  Created_Date?: string;
  Created_Time?: string;        // merged into CreatedDate
  Last_Modified_Date?: string;
  Last_Modified_Time?: string;  // merged into LastModifiedDate
  STAP_Applied?: string;
  Enrollment_Date?: string;
  STAP_Report_Run_Date?: string;
  Survey_Response_Date?: string;
  Survey_Response_Time?: string;
}

// Column names pulled out of each raw TSV row — must match RawEnrollmentRow's
// keys above exactly. Building each row via this fixed list (instead of
// looping over every TSV header, as before) also lets V8 use one consistent,
// memory-efficient object shape for all ~586K rows instead of a slower,
// heavier "dictionary mode" object per row.
const NEEDED_COLUMNS: (keyof RawEnrollmentRow)[] = [
  "ID",
  "Registration_ID",
  "Student_ID",
  "Course_ID",
  "Grade_Option",
  "Grade",
  "Grade_Entered_Date",
  "Status",
  "Notes",
  "Tuition",
  "Fee",
  "Add_Drop",
  "Extension",
  "TA_Discount",
  "Created_Date",
  "Created_Time",
  "Last_Modified_Date",
  "Last_Modified_Time",
  "STAP_Applied",
  "Enrollment_Date",
  "STAP_Report_Run_Date",
  "Survey_Response_Date",
  "Survey_Response_Time",
];

// ── Dedup helpers ─────────────────────────────────────────────────────────────

// Higher rank = more terminal status = wins when multiple rows share a dedup group.
// NOTE: "Adjustment" is NOT ranked here — those rows bypass grouping entirely
// and are always kept as their own separate COP (architect, Sep 18: "bring in
// both records"). See buildDedupMap().
function statusRank(status: string): number {
  const s = status.trim();
  if (
    s === "Drop w/Refund" ||
    s === "Drop w/ refund" ||
    s === "Drop w/refund" ||
    s === "Drop 1/2 refund" ||
    s === "Drop w/ r"
  )
    return 10;
  if (s === "Drop no refund" || s === "Drop No Refund") return 9;
  if (s === "Drop - Pending") return 8;
  if (s === "Drop Transferred") return 7;
  // Cancel variants — all map to ParticipationStatus "Cancelled" (architect, Sep 18)
  if (
    s === "Course cancel" ||
    s === "Cancelled - Refunded" ||
    s === "Cancelled - Pending"
  )
    return 6;
  if (s === "Drop") return 5;
  if (s === "UnEnrolled") return 4;
  // DupEnrollment kept as its own status per architect — participates in
  // normal dedup ranking like any other status.
  if (s === "DupEnrollment") return 2;
  if (s === "Enrolled" || s === "Wait List") return 1;
  return 0; // "New" (abandoned cart, excluded earlier), blank, unknown
}

// ── Value mapping helpers ─────────────────────────────────────────────────────

function mapParticipationStatus(raw: string | undefined): string | null {
  const s = str(raw).trim();
  if (s === "Enrolled") return "Enrolled";
  if (s === "Wait List") return "Waitlisted";
  if (s === "UnEnrolled") return "Dropped";
  // All Drop variants flatten to plain "Dropped" — architect, Sep 18: "All
  // dropped statuses will need to be set as 'Dropped'" (supersedes the
  // earlier plan to preserve refund detail as separate picklist values).
  if (
    s === "Drop w/Refund" ||
    s === "Drop w/ refund" ||
    s === "Drop w/refund" ||
    s === "Drop 1/2 refund" ||
    s === "Drop w/ r" ||
    s === "Drop no refund" ||
    s === "Drop No Refund" ||
    s === "Drop Transferred" ||
    s === "Drop - Pending" ||
    s === "Drop"
  )
    return "Dropped";
  // Cancel variants — architect, Sep 18: "All statuses with Cancelled in
  // them will need to be set as 'Cancelled'" (confirmed "Course cancel" is
  // included even though it doesn't literally contain the word "Cancelled").
  if (
    s === "Course cancel" ||
    s === "Cancelled - Refunded" ||
    s === "Cancelled - Pending"
  )
    return "Cancelled";
  // Architect, Sep 18: Adjustment becomes its own status and is never
  // excluded. The matching Enrolled/Dropped row from the same dedup group is
  // kept too — see buildDedupMap() for how both rows survive as separate COPs.
  if (s === "Adjustment") return "Adjustment";
  // Architect confirmed: keep DupEnrollment records as-is, as their own status.
  if (s === "DupEnrollment") return "DupEnrollment";
  return null; // blank or unrecognized — caller logs and skips
}

function mapGradeOption(raw: string | undefined): string | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  switch (s) {
    case "NGR":
      return "NGR";
    case "CR/NC":
      return "Credit/No Credit";
    case "CR/NCR":
      return "Credit/No Credit"; // junk cleanup
    case "Letter":
      return "Letter Grade";
    case "Audit":
    case "Audit'":
    case "Audit\\":
      return "NGR"; // Audit = NGR per Amy, Jul 2
    case "Inst S/NC":
      return "Inst S/NC";
    case "Inst CR/N":
      return "Inst S/NC"; // junk cleanup
    default:
      return undefined;
  }
}

// Grade junk cleanup per workbook (Jul 2 refinement).
// CourseOfferingPtcpResult records are created in a SEPARATE flow after COP load.
function normalizeGrade(raw: string | undefined): string | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  switch (s) {
    case "AUD":
      return "AU";
    case "CRA":
      return "CR";
    case "CR/":
      return "CR";
    case "904":
      return undefined;
    case "Ins":
      return undefined;
    case "Stu":
      return undefined;
    case "A-S":
      return "A-";
    case "SU":
      return "S";
    default:
      return s;
  }
}

// Grade → ParticipantResultStatus (CourseOfferingPtcpResult). Architect
// confirmed (Sep 28) — the field's real picklist only has Pass/Fail/
// Incomplete/Withdraw, not "Final" as the mapping doc originally said, and
// that value can't be added (locked by the standard package). Only the
// values actually present in our real 2-year data were confirmed; anything
// else returns undefined (record skipped) rather than guessing.
function mapParticipantResultStatus(grade: string): string | undefined {
  switch (grade) {
    case "A+":
    case "A":
    case "A-":
    case "B+":
    case "B":
    case "B-":
    case "C+":
    case "C":
    case "C-":
    case "CR":
    case "NGR":
      return "Pass";
    case "D+":
    case "D":
    case "D-":
    case "NC":
    case "NP":
      return "Fail";
    default:
      return undefined;
  }
}

function mapAddDrop(raw: string | undefined): string | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  const u = s.toUpperCase();
  if (u === "ADD") return "Add";
  if (u === "DROP") return "Drop";
  if (s === "AJST") return "Adjustment";
  return undefined;
}

// Strips trailing ".0" (4D Longint-to-Text export artifact).
function stripDotZero(v: string | undefined): string {
  return str(v).trim().replace(/\.0$/, "");
}

// Returns YYYY-MM-DD or "" — toDate() already returns "" for "00/00/00".
function enrollDate(raw: string | undefined): string {
  return toDate(str(raw).trim());
}

function combineDateTime(
  dateRaw: string | undefined,
  timeRaw: string | undefined,
): string | undefined {
  const d = enrollDate(dateRaw);
  if (!d) return undefined;
  const t = str(timeRaw).trim() || "00:00:00";
  return `${d}T${t}.000Z`;
}

function parseCurrency(raw: string | undefined): number | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  const n = parseFloat(s);
  return isNaN(n) ? undefined : n;
}

// ── Salesforce query helper ───────────────────────────────────────────────────

async function sfQueryAll<T>(
  base: string,
  token: string,
  soql: string,
): Promise<T[]> {
  const headers = { Authorization: `Bearer ${token}` };
  const all: T[] = [];
  let nextUrl: string | null = null;
  let done = false;

  const fetch = async (url: string | null) => {
    const { data } = await axios.get<{
      records: T[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${base}/services/data/${SF_API}/query`,
      url ? { headers } : { params: { q: soql }, headers },
    );
    all.push(...data.records);
    done = data.done;
    nextUrl = data.nextRecordsUrl ? `${base}${data.nextRecordsUrl}` : null;
  };

  await fetch(null);
  while (!done && nextUrl) await fetch(nextUrl);
  return all;
}

// ── Student / Person Account cache ────────────────────────────────────────────

interface StudentCacheEntry {
  contactId: string; // PersonContactId → ParticipantContactId
  accountId: string; // Id             → ParticipantAccountId
}

async function buildStudentCache(
  base: string,
  token: string,
): Promise<Map<string, StudentCacheEntry>> {
  const records = await sfQueryAll<{
    Id: string;
    PersonContactId: string;
    Student_ID_4D__c: string;
  }>(
    base,
    token,
    "SELECT Id, PersonContactId, Student_ID_4D__c FROM Account WHERE IsPersonAccount = true AND Student_ID_4D__c != null",
  );
  const map = new Map<string, StudentCacheEntry>();
  for (const r of records) {
    if (r.Student_ID_4D__c) {
      map.set(r.Student_ID_4D__c, {
        contactId: r.PersonContactId,
        accountId: r.Id,
      });
    }
  }
  return map;
}

// ── CourseOffering cache ──────────────────────────────────────────────────────

async function buildCourseOfferingCache(
  base: string,
  token: string,
): Promise<Map<string, string>> {
  const records = await sfQueryAll<{
    Id: string;
    External_ID_4D__c: string;
  }>(
    base,
    token,
    "SELECT Id, External_ID_4D__c FROM CourseOffering WHERE External_ID_4D__c != null",
  );
  const map = new Map<string, string>();
  for (const r of records) {
    if (r.External_ID_4D__c) map.set(r.External_ID_4D__c, r.Id);
  }
  return map;
}

// ── Streaming dedup builder ───────────────────────────────────────────────────
// Streams the entire Enrollment TSV once. For each group
// (Registration_ID + Student_ID + Course_ID) keeps only the row with the
// highest status rank (most terminal). Memory cost = O(unique_groups × row_size).

async function buildDedupMap(
  fileId: string,
  accessToken: string,
): Promise<{
  winners: Map<string, RawEnrollmentRow>;
  totalRows: number;
  excludedNew: number;
  adjustmentCount: number;
  skippedOutOfWindow: number;
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
    const winners = new Map<string, RawEnrollmentRow>();
    const winnerRank = new Map<string, number>();
    // Maps each NEEDED_COLUMNS name to its column index in the raw TSV, built
    // once the header row is seen. Columns we don't care about (Program,
    // Source, Created_By, etc.) simply never get an entry here and are never
    // read — see the RawEnrollmentRow comment above for why that matters.
    const columnIndex: Partial<Record<keyof RawEnrollmentRow, number>> = {};
    let headersSeen = false;
    let totalRows = 0;
    let excludedNew = 0;
    let adjustmentCount = 0;
    let skippedOutOfWindow = 0;
    let settled = false;
    const finish = (result: {
      winners: Map<string, RawEnrollmentRow>;
      totalRows: number;
      excludedNew: number;
      adjustmentCount: number;
      skippedOutOfWindow: number;
    }) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };

    papaParse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      skipEmptyLines: true,

      step: (
        result: Papa.ParseResult<string[]>,
        parser: { abort: () => void },
      ) => {
        const raw = result.data as unknown as string[];

        if (!headersSeen) {
          headersSeen = true;
          const headers = raw.map((h) =>
            h
              .replace(/^﻿/, "")
              .replace(/\r/g, "")
              .trim(),
          );
          for (const col of NEEDED_COLUMNS) {
            const idx = headers.indexOf(col);
            if (idx !== -1) columnIndex[col] = idx;
          }
          return;
        }

        const get = (col: keyof RawEnrollmentRow): string => {
          const idx = columnIndex[col];
          return idx === undefined ? "" : (raw[idx] ?? "").replace(/\r/g, "");
        };
        // Fixed property list on every object — same shape every time, so V8
        // can use one fast hidden class across all ~586K rows instead of
        // dictionary mode (see RawEnrollmentRow comment above).
        const record: RawEnrollmentRow = {
          ID: get("ID"),
          Registration_ID: get("Registration_ID"),
          Student_ID: get("Student_ID"),
          Course_ID: get("Course_ID"),
          Grade_Option: get("Grade_Option"),
          Grade: get("Grade"),
          Grade_Entered_Date: get("Grade_Entered_Date"),
          Status: get("Status"),
          Notes: get("Notes"),
          Tuition: get("Tuition"),
          Fee: get("Fee"),
          Add_Drop: get("Add_Drop"),
          Extension: get("Extension"),
          TA_Discount: get("TA_Discount"),
          Created_Date: get("Created_Date"),
          Created_Time: get("Created_Time"),
          Last_Modified_Date: get("Last_Modified_Date"),
          Last_Modified_Time: get("Last_Modified_Time"),
          STAP_Applied: get("STAP_Applied"),
          Enrollment_Date: get("Enrollment_Date"),
          STAP_Report_Run_Date: get("STAP_Report_Run_Date"),
          Survey_Response_Date: get("Survey_Response_Date"),
          Survey_Response_Time: get("Survey_Response_Time"),
        };

        if (!str(record.ID).trim()) return;
        totalRows++;

        // TEST_MODE: stop reading the file early instead of streaming and
        // deduping the full ~586K-row file, so a small test run doesn't risk
        // the same out-of-memory failure a full run would need more headroom
        // for. (TEST_LIMIT further down only trims the FINAL record list —
        // it can't help here, since by then the whole file has already been
        // read.) Not a substitute for fixing full-run memory — just enough
        // to safely test the new Status logic end-to-end.
        if (TEST_MODE && totalRows >= TEST_LIMIT) {
          parser.abort();
          finish({
            winners,
            totalRows,
            excludedNew,
            adjustmentCount,
            skippedOutOfWindow,
          });
          return;
        }

        const status = str(record.Status).trim();

        if (status === "New") {
          excludedNew++;
          return;
        }

        // Course Flow only migrates CourseOffering records within the
        // current 2-year window — an enrollment for a course outside it has
        // no CourseOffering to link to, so it can never become a COP.
        // Filtering here, DURING the streaming pass (not after dedup), keeps
        // peak memory bounded: on the real file, ~530K/586K rows are outside
        // this window, and letting all of them sit in the dedup map first —
        // only to discard 91% of them a moment later — is what pushed a full
        // run over Prismatic's 1GB memory limit.
        const courseId = str(record.Course_ID).trim();
        const coursePrefix = courseId.split("_")[0];
        if (!coursePrefix || !VALID_COURSE_ID_PREFIXES.has(coursePrefix)) {
          skippedOutOfWindow++;
          return;
        }

        // Architect, Sep 18: "bring in both records" for Adjustment — 4D
        // creates a separate Adjustment row alongside the primary
        // Enrolled/Dropped row for the same Registration_ID + Student_ID +
        // Course_ID group. Adjustment rows bypass the group-ranking below
        // entirely (keyed on their own unique Enrollment ID) so they always
        // survive as their own COP, regardless of what wins the group.
        if (status === "Adjustment") {
          adjustmentCount++;
          winners.set(`adj:${str(record.ID).trim()}`, record);
          return;
        }

        const rank = statusRank(status);
        const regId = stripDotZero(record.Registration_ID);
        const studId = stripDotZero(record.Student_ID);
        const groupKey = `${regId}|${studId}|${courseId}`;

        const existingRank = winnerRank.get(groupKey) ?? -1;
        if (rank > existingRank) {
          winners.set(groupKey, record);
          winnerRank.set(groupKey, rank);
        }
      },

      complete: () =>
        finish({
          winners,
          totalRows,
          excludedNew,
          adjustmentCount,
          skippedOutOfWindow,
        }),
      error: (err: Error) => {
        if (!settled) reject(err);
      },
    });
  });
}

// ── SF record mapper ──────────────────────────────────────────────────────────

type EnrollmentCopRecord = Record<string, unknown>;

function mapToEnrollmentCop(
  raw: RawEnrollmentRow,
  studentCache: Map<string, StudentCacheEntry>,
  coCache: Map<string, string>,
  logger: { warn: (m: string) => void },
): EnrollmentCopRecord | null {
  const id = str(raw.ID).trim();
  if (!id) return null;

  const status = str(raw.Status).trim();

  const participationStatus = mapParticipationStatus(status);
  if (!participationStatus) {
    logger.warn(
      `[Enrollment Import] Unknown status "${status}" for ENR-${id} — skipped`,
    );
    return null;
  }

  const record: EnrollmentCopRecord = {
    External_ID_4D__c: `ENR-${id}`,
    ParticipantAffiliation: "Student",
    ParticipationStatus: participationStatus,
  };

  // AcademicTermEnrollment — intentionally not set. Architect (Sep 28): it is
  // not populated for registrations going forward, so migrated data leaves it
  // blank too (populating past data could cause issues with the build). No
  // AcademicTermEnrollment records exist in Salesforce, so sending this lookup
  // would fail every row (INVALID_FIELD: foreign key external ID not found).
  // const regId = stripDotZero(raw.Registration_ID);
  // if (regId && regId !== "0") {
  //   record["AcademicTermEnrollment.External_ID_4D__c"] = regId;
  // }

  // Student contact + account
  const studentId = stripDotZero(raw.Student_ID);
  if (studentId && studentId !== "0") {
    const entry = studentCache.get(studentId);
    if (entry) {
      record.ParticipantContactId = entry.contactId;
      record.ParticipantAccountId = entry.accountId;
    } else {
      logger.warn(
        `[Enrollment Import] Student_ID "${studentId}" not found in SF — ENR-${id} will be missing contact/account`,
      );
    }
  }

  // CourseOffering
  const courseId = str(raw.Course_ID).trim();
  if (courseId) {
    const coId = coCache.get(courseId);
    if (coId) {
      record.CourseOfferingId = coId;
    } else {
      logger.warn(
        `[Enrollment Import] Course_ID "${courseId}" not found in SF — ENR-${id} will be missing offering`,
      );
    }
  }

  // Grade_Option
  const gradeOption = mapGradeOption(raw.Grade_Option);
  if (gradeOption) record.Grade_Option__c = gradeOption;

  // Grade (junk cleaned; CourseOfferingPtcpResult is a SEPARATE load step)
  const grade = normalizeGrade(raw.Grade);
  if (grade) record.Grade__c = grade;

  // Grade_Entered_Date — mapping doc says "Grade_Entered_Date__c", but the
  // real field in the org is named "IP_GradeEnteredDate__c" (confirmed via
  // live Object Manager, Sep 25 — job-level "Field name not found" error on
  // the documented name). It's also a DateTime field, not Date — confirmed
  // Sep 28 by "not a valid value for the type xsd:dateTime" when sending a
  // plain date — so a default time-of-day is appended, same pattern as
  // Enrollment_Date__c/RegistrationDateTime below.
  const gradeEnteredDate = enrollDate(raw.Grade_Entered_Date);
  if (gradeEnteredDate)
    record.IP_GradeEnteredDate__c = `${gradeEnteredDate}T00:00:00.000Z`;

  // Notes → Summary (strip _4DNL_ tokens)
  const notes = str(raw.Notes).replace(/_4DNL_/g, "\n").trim();
  if (notes) record.Summary = notes.slice(0, 32000);

  // Currency fields — zero values preserved where noted in workbook
  const tuition = parseCurrency(raw.Tuition);
  if (tuition !== undefined) record.Tuition__c = tuition;

  const fee = parseCurrency(raw.Fee);
  if (fee !== undefined) record.Fee__c = fee;

  const extension = parseCurrency(raw.Extension);
  if (extension !== undefined) record.Extension__c = extension;

  // TA_Discount — stored as negative per SA recommendation (all non-zero values are negative)
  const taDiscount = parseCurrency(raw.TA_Discount);
  if (taDiscount !== undefined && taDiscount !== 0)
    record.TA_Discount__c = taDiscount;

  // STAP_Applied — zero means STAP not applicable; convert to null
  const stapApplied = parseCurrency(raw.STAP_Applied);
  if (stapApplied !== undefined && stapApplied !== 0)
    record.STAP_Applied__c = stapApplied;

  // Add_Drop
  const addDrop = mapAddDrop(raw.Add_Drop);
  if (addDrop) record.Add_Drop__c = addDrop;

  // Enrollment_Date → Enrollment_Date__c + RegistrationDateTime (date + default 00:00:00)
  const enrollmentDate = enrollDate(raw.Enrollment_Date);
  if (enrollmentDate) {
    record.Enrollment_Date__c = enrollmentDate;
    record.RegistrationDateTime = `${enrollmentDate}T00:00:00.000Z`;
  }

  // STAP_Report_Run_Date — only where STAP_Applied > 0
  if (stapApplied !== undefined && stapApplied !== 0) {
    const stapDate = enrollDate(raw.STAP_Report_Run_Date);
    if (stapDate) record.STAP_Report_Run_Date__c = stapDate;
  }

  // Survey_Response_Date + Survey_Response_Time
  const surveyDate = enrollDate(raw.Survey_Response_Date);
  if (surveyDate) {
    record.Survey_Response_Date__c = surveyDate;
    const surveyTime = str(raw.Survey_Response_Time).trim();
    if (surveyTime && surveyTime !== "00:00:00") {
      record.Survey_Response_Time__c = surveyTime;
    }
  }

  // Audit fields — requires "Set Audit Fields upon Record Creation" org permission
  const createdDateTime = combineDateTime(raw.Created_Date, raw.Created_Time);
  if (createdDateTime) record.CreatedDate = createdDateTime;

  // LastModifiedDate: falls back to CreatedDate when Last_Modified_Date is 00/00/00
  const lastModDate = enrollDate(raw.Last_Modified_Date);
  const lastModDateTime = lastModDate
    ? combineDateTime(raw.Last_Modified_Date, raw.Last_Modified_Time)
    : createdDateTime;
  if (lastModDateTime) record.LastModifiedDate = lastModDateTime;

  return record;
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const enrollmentImport = flow({
  name: "Enrollment Import",
  stableKey: "en223344-5566-4c78-9900-aabbccddeeff",
  description:
    "Streams the Enrollment TSV from Google Drive, deduplicates by " +
    "(Registration_ID + Student_ID + Course_ID), and bulk-upserts " +
    "CourseOfferingParticipant records via Bulk API 2.0. " +
    "Must run after Student, Course, and Registration flows.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;
    logger.info("[Enrollment Import] Starting…");

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars[
      "Google Drive Connection"
    ] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Enrollment File ID"] as unknown as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Enrollment File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── Build lookup caches ───────────────────────────────────────────────────
    logger.info(
      "[Enrollment Import] Building Student and CourseOffering caches…",
    );
    const [studentCache, coCache] = await Promise.all([
      buildStudentCache(sfBase, sfToken),
      buildCourseOfferingCache(sfBase, sfToken),
    ]);
    logger.info(
      `[Enrollment Import] Caches — students=${studentCache.size}, courseOfferings=${coCache.size}`,
    );

    // ── Stream + dedup ────────────────────────────────────────────────────────
    logger.info(
      "[Enrollment Import] Streaming Enrollment TSV and building dedup map…",
    );
    const {
      winners,
      totalRows,
      excludedNew,
      adjustmentCount,
      skippedOutOfWindow,
    } = await buildDedupMap(fileId, gdToken);

    const uniqueGroups = winners.size;
    logger.info(
      `[Enrollment Import] Dedup complete — total=${totalRows}, ` +
        `uniqueGroups=${uniqueGroups}, excludedNew=${excludedNew}, ` +
        `adjustmentRecordsKeptSeparately=${adjustmentCount}, ` +
        `skippedOutOfWindow=${skippedOutOfWindow}`,
    );

    // ── Map to SF records ─────────────────────────────────────────────────────
    // The 2-year window filter already ran DURING buildDedupMap's streaming
    // pass above (not here) — that's what keeps peak memory bounded, since it
    // stops ~530K/586K out-of-scope rows from ever entering the dedup map in
    // the first place. `winners` at this point only holds in-window rows.
    const sfRecords: EnrollmentCopRecord[] = [];
    let skippedUnmappable = 0;

    for (const row of winners.values()) {
      const mapped = mapToEnrollmentCop(row, studentCache, coCache, logger);
      if (!mapped) {
        skippedUnmappable++;
        continue;
      }
      sfRecords.push(mapped);
    }
    // Done with the raw rows — drop the reference so V8 can reclaim this
    // memory before the (also memory-hungry) Bulk API upload below runs.
    winners.clear();

    if (TEST_MODE && sfRecords.length > TEST_LIMIT) {
      sfRecords.splice(TEST_LIMIT);
      logger.info(
        `[Enrollment Import] TEST MODE: limited to first ${TEST_LIMIT} records`,
      );
    }

    logger.info(
      `[Enrollment Import] ${sfRecords.length} records ready to upsert ` +
        `(${skippedUnmappable} unmappable)`,
    );

    if (sfRecords.length === 0) {
      logger.info("[Enrollment Import] No records to upsert — done.");
      return {
        data: { totalRows, uniqueGroups, sfRecords: 0 },
      };
    }

    // ── Bulk upsert in batches of BULK_BATCH_SIZE ─────────────────────────────
    const batches: EnrollmentCopRecord[][] = [];
    for (let i = 0; i < sfRecords.length; i += BULK_BATCH_SIZE) {
      batches.push(sfRecords.slice(i, i + BULK_BATCH_SIZE));
    }
    logger.info(
      `[Enrollment Import] Submitting ${batches.length} bulk job(s) ` +
        `(up to ${BULK_BATCH_SIZE} records each)…`,
    );

    let totalProcessed = 0;
    let totalFailed = 0;
    let successfulCsv = "";
    let failedCsv = "";

    for (let b = 0; b < batches.length; b++) {
      logger.info(
        `[Enrollment Import] Batch ${b + 1}/${batches.length} — ${batches[b].length} records`,
      );
      const result = await runBulkJob(
        sfBase,
        sfToken,
        SF_OBJECT,
        EXT_ID_FIELD,
        batches[b] as unknown as Record<string, unknown>[],
        logger,
        `[Enrollment Import Batch ${b + 1}]`,
      );
      totalProcessed += result.numberRecordsProcessed;
      totalFailed += result.numberRecordsFailed;
      if (result.successfulCsv)
        successfulCsv += (successfulCsv ? "\n" : "") + result.successfulCsv;
      if (result.failedCsv)
        failedCsv += (failedCsv ? "\n" : "") + result.failedCsv;
    }

    // ── Phase 2: CourseOfferingPtcpResult — graded enrollments only ──────────
    // Per mapping doc: a separate load step, run only after every COP batch
    // above has fully completed. A Result's lookup to its parent COP is
    // resolved via the same external-ID relationship pattern used everywhere
    // else in this codebase ("RelationshipName.ExternalIdField" as a CSV
    // column) — Salesforce requires the parent COP to already exist for that
    // to resolve, which is exactly why this can't run in parallel with, or
    // before, the COP batches above. Reuses the same "ENR-{id}" value as the
    // Result's own External_ID_4D__c (safe — external IDs are scoped per
    // object, and it's a clean 1:1 relationship: at most one Result per COP).
    const RESULT_OBJECT = "CourseOfferingPtcpResult";
    const resultRecords: Record<string, unknown>[] = [];
    let skippedUnmappableResultStatus = 0;
    for (const rec of sfRecords) {
      const grade = rec.Grade__c as string | undefined;
      if (!grade) continue;
      const resultStatus = mapParticipantResultStatus(grade);
      if (!resultStatus) {
        skippedUnmappableResultStatus++;
        logger.warn(
          `[Enrollment Import][Result] Grade "${grade}" has no ParticipantResultStatus mapping — Result skipped for ${rec.External_ID_4D__c}`,
        );
        continue;
      }
      const copExtId = rec.External_ID_4D__c as string;
      // LetterGrade (standard field) is capped at 2 characters — architect
      // confirmed (Sep 29): shrink "NGR" to "NG" rather than leave it blank
      // or resize the field (which isn't possible; it's a standard field).
      // Grade__c on COP keeps the full "NGR" value untouched — only this
      // Result field is shortened.
      const letterGrade = grade === "NGR" ? "NG" : grade;
      resultRecords.push({
        External_ID_4D__c: copExtId,
        "CourseOfferingParticipant.External_ID_4D__c": copExtId,
        LetterGrade: letterGrade,
        ParticipantResultStatus: resultStatus,
      });
    }
    if (skippedUnmappableResultStatus > 0) {
      logger.warn(
        `[Enrollment Import][Result] ${skippedUnmappableResultStatus} graded record(s) skipped — grade value not yet mapped to a ParticipantResultStatus.`,
      );
    }

    logger.info(
      `[Enrollment Import] Phase 2 — ${resultRecords.length} ` +
        `CourseOfferingPtcpResult record(s) to upsert (graded enrollments)…`,
    );

    let resultProcessed = 0;
    let resultFailed = 0;
    let resultSuccessfulCsv = "";
    let resultFailedCsv = "";

    if (resultRecords.length > 0) {
      const resultBatches: Record<string, unknown>[][] = [];
      for (let i = 0; i < resultRecords.length; i += BULK_BATCH_SIZE) {
        resultBatches.push(resultRecords.slice(i, i + BULK_BATCH_SIZE));
      }
      for (let b = 0; b < resultBatches.length; b++) {
        logger.info(
          `[Enrollment Import][Result] Batch ${b + 1}/${resultBatches.length} ` +
            `— ${resultBatches[b].length} records`,
        );
        const result = await runBulkJob(
          sfBase,
          sfToken,
          RESULT_OBJECT,
          EXT_ID_FIELD,
          resultBatches[b],
          logger,
          `[Enrollment Import][Result] Batch ${b + 1}`,
        );
        resultProcessed += result.numberRecordsProcessed;
        resultFailed += result.numberRecordsFailed;
        if (result.successfulCsv)
          resultSuccessfulCsv +=
            (resultSuccessfulCsv ? "\n" : "") + result.successfulCsv;
        if (result.failedCsv)
          resultFailedCsv += (resultFailedCsv ? "\n" : "") + result.failedCsv;
      }
    } else {
      logger.info(
        "[Enrollment Import][Result] No graded enrollments in this run — skipping.",
      );
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Enrollment Import",
        objects: [
          {
            objectName: SF_OBJECT,
            successfulCsv,
            failedCsv,
          },
          {
            objectName: RESULT_OBJECT,
            successfulCsv: resultSuccessfulCsv,
            failedCsv: resultFailedCsv,
          },
        ],
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Enrollment Import] Results sheet: ${sheet.url}`);
    } catch (err) {
      logger.warn(
        `[Enrollment Import] Could not write results sheet: ${String(err)}`,
      );
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Enrollment Import] Complete —` +
        `\n  Source rows:      ${totalRows}` +
        `\n  Excluded (New):   ${excludedNew}` +
        `\n  Dedup groups:     ${uniqueGroups}` +
        `\n  Adjustment (kept separately): ${adjustmentCount}` +
        `\n  Outside 2yr window: ${skippedOutOfWindow}` +
        `\n  Unmappable status: ${skippedUnmappable}` +
        `\n  COP submitted:    ${sfRecords.length}` +
        `\n  COP processed:    ${totalProcessed}` +
        `\n  COP failed:       ${totalFailed}` +
        `\n  Result submitted: ${resultRecords.length}` +
        `\n  Result processed: ${resultProcessed}` +
        `\n  Result failed:    ${resultFailed}`,
    );

    return {
      data: {
        totalRows,
        uniqueGroups,
        adjustmentCount,
        skippedOutOfWindow,
        skippedUnmappable,
        sfRecords: sfRecords.length,
        processed: totalProcessed,
        failed: totalFailed,
        resultRecords: resultRecords.length,
        resultProcessed,
        resultFailed,
      },
    };
  },
});

export default [enrollmentImport];
