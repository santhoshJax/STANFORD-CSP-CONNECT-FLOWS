/**
 * Stanford CSP Migration – Enrollment Import flow.
 *
 * Streams the Enrollment TSV from Google Drive, deduplicates by
 * (Registration_ID + Student_ID + Course_ID), and bulk-upserts
 * CourseOfferingParticipant records via Bulk API 2.0. Once every COP batch
 * has fully completed, a second phase creates one CourseOfferingPtcpResult
 * child record for each COP with a populated Grade (LetterGrade = Grade__c,
 * ParticipantResultStatus = "Final") — reusing the same in-memory deduped
 * rows, no second file read. Must run after COP, since a Result's lookup to
 * its parent COP requires that COP to already exist in Salesforce.
 *
 * PREREQUISITES (must run before this flow):
 *   1. Student flow       — loads Person Account / Contact (Student_ID_4D__c)
 *   2. Course flow        — loads CourseOffering (External_ID_4D__c)
 *   3. Registration flow  — loads AcademicTermEnrollment (External_ID_4D__c)
 *
 * External ID: "ENR-{Enrollment.ID}"  e.g. "ENR-200058"
 *   Prefixed to avoid collision with CINST-{n} / CASSOC-{n} instructor/associate COPs.
 *   Enrollment Waiver flow must look up COPs using this same "ENR-" prefix.
 *
 * DEDUP RULE (Jul 2 refinement):
 *   When a student drops, 4D creates 2–3 enrollment rows for the same
 *   Registration_ID + Student_ID + Course_ID. Only one COP is created in SF.
 *   Terminal-status precedence (highest rank wins):
 *     Drop w/ Refund variants (10) > Drop No Refund (9) > Drop - Pending (8)
 *     > Drop Transferred (7) > Cancel variants (6) > Drop (5) > UnEnrolled (4)
 *     > Adjustment (3) > DupEnrollment (2) > Enrolled / Wait List (1)
 *     > New (0 — excluded entirely per Amy Jul 2: abandoned cart)
 *
 * OPEN ITEMS (async follow-up required — affected records excluded until resolved):
 *   - "Adjustment" (6,456):         Late refund/tuition adjustment. Target status TBD (Amy).
 *   - "DupEnrollment" (3,173):      Exclude or migrate as "Enrolled"? TBD.
 *   - "Course cancel" (4,859):      Course-cancelled vs. student-dropped. Status TBD (Amy).
 *   - "Cancelled - Refunded" (3,430): Status TBD.
 *   - "Cancelled - Pending" (18):   Status TBD.
 *   - Legacy_Enrollment_Status__c:  Where to preserve drop/refund detail (Holly/Amy async).
 *   - TA_Discount sign convention:  Stored as negative per SA recommendation; confirm with Ahmet.
 *   - Audit fields (CreatedDate, LastModifiedDate): Requires "Set Audit Fields upon Record
 *     Creation" and "Create Audit Fields" permissions for the migration user.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse } from "papaparse";
import { str, toDate, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// ── Constants ─────────────────────────────────────────────────────────────────
const SF_API = "v60.0";
const BULK_BATCH_SIZE = 50_000;
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

interface RawEnrollmentRow {
  ID?: string;
  Registration_ID?: string;
  Student_ID?: string;
  Course_ID?: string;
  Program?: string;             // Do Not Map
  Grade_Option?: string;
  Grade?: string;
  Grade_Entered_Date?: string;
  Status?: string;
  Source?: string;              // Do Not Map
  Notes?: string;
  Tuition?: string;
  Fee?: string;
  Other_Costs?: string;         // Do Not Map
  Add_Drop?: string;
  Extension?: string;
  TA_Discount?: string;
  Created_Date?: string;
  Created_Time?: string;        // merged into CreatedDate
  Created_By?: string;          // Do Not Map
  Last_Modified_Date?: string;
  Last_Modified_Time?: string;  // merged into LastModifiedDate
  Last_Modified_By?: string;    // Do Not Map
  STAP_Applied?: string;
  Enrollment_Date?: string;
  Grade_Printed_Date?: string;  // Do Not Map
  Student_ID_Previous?: string; // Do Not Map
  Prior_Enrollment_ID?: string; // Do Not Map
  STAP_Report_Run_Date?: string;
  No_Discounts?: string;        // Do Not Map
  Survey_Response_Date?: string;
  Survey_Response_Time?: string;
  Instructor_ID?: string;       // Do Not Map
  [key: string]: string | undefined;
}

// ── Dedup helpers ─────────────────────────────────────────────────────────────

// Higher rank = more terminal status = wins when multiple rows share a dedup group.
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
  // Cancel variants — OPEN; ranked so they can win a group but are excluded in the mapper
  if (
    s === "Course cancel" ||
    s === "Cancelled - Refunded" ||
    s === "Cancelled - Pending"
  )
    return 6;
  if (s === "Drop") return 5;
  if (s === "UnEnrolled") return 4;
  // OPEN items — ranked but excluded in mapper until business decision
  if (s === "Adjustment") return 3;
  if (s === "DupEnrollment") return 2;
  if (s === "Enrolled" || s === "Wait List") return 1;
  return 0; // "New" (abandoned cart), blank, unknown — always excluded
}

// ── Value mapping helpers ─────────────────────────────────────────────────────

function mapParticipationStatus(raw: string | undefined): string | null {
  const s = str(raw).trim();
  if (s === "Enrolled") return "Enrolled";
  if (s === "Wait List") return "Waitlisted";
  if (s === "UnEnrolled") return "Dropped";
  if (
    s === "Drop w/Refund" ||
    s === "Drop w/ refund" ||
    s === "Drop w/refund" ||
    s === "Drop 1/2 refund" ||
    s === "Drop w/ r"
  )
    return "Drop w/ Refund";
  if (s === "Drop no refund" || s === "Drop No Refund") return "Drop No Refund";
  if (s === "Drop Transferred") return "Drop Transferred";
  if (s === "Drop - Pending") return "Drop - Pending";
  if (s === "Drop") return "Dropped";
  return null; // OPEN or unknown — caller logs and skips
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

const OPEN_STATUSES = new Set([
  "Adjustment",
  "DupEnrollment",
  "Course cancel",
  "Cancelled - Refunded",
  "Cancelled - Pending",
]);

async function buildDedupMap(
  fileId: string,
  accessToken: string,
): Promise<{
  winners: Map<string, RawEnrollmentRow>;
  totalRows: number;
  excludedNew: number;
  openStatusCounts: Map<string, number>;
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
    const openStatusCounts = new Map<string, number>();
    let headers: string[] = [];
    let totalRows = 0;
    let excludedNew = 0;

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
              h
                .replace(/^﻿/, "")
                .replace(/\r/g, "")
                .trim() || `__blank_${i}`,
          );
          return;
        }

        const record: RawEnrollmentRow = {};
        headers.forEach((h, i) => {
          if (!h.startsWith("__blank_")) {
            record[h] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        if (!str(record.ID).trim()) return;
        totalRows++;

        const status = str(record.Status).trim();

        if (status === "New") {
          excludedNew++;
          return;
        }

        if (OPEN_STATUSES.has(status)) {
          openStatusCounts.set(
            status,
            (openStatusCounts.get(status) ?? 0) + 1,
          );
        }

        const rank = statusRank(status);
        const regId = stripDotZero(record.Registration_ID);
        const studId = stripDotZero(record.Student_ID);
        const courseId = str(record.Course_ID).trim();
        const groupKey = `${regId}|${studId}|${courseId}`;

        const existingRank = winnerRank.get(groupKey) ?? -1;
        if (rank > existingRank) {
          winners.set(groupKey, record);
          winnerRank.set(groupKey, rank);
        }
      },

      complete: () =>
        resolve({ winners, totalRows, excludedNew, openStatusCounts }),
      error: (err: Error) => reject(err),
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

  if (OPEN_STATUSES.has(status)) return null; // excluded pending decisions

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

  // AcademicTermEnrollment — resolved by Salesforce during Bulk API job
  // via the external ID relationship reference in the CSV column header.
  const regId = stripDotZero(raw.Registration_ID);
  if (regId && regId !== "0") {
    record["AcademicTermEnrollment.External_ID_4D__c"] = regId;
  }

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

  // Grade_Entered_Date
  const gradeEnteredDate = enrollDate(raw.Grade_Entered_Date);
  if (gradeEnteredDate) record.Grade_Entered_Date__c = gradeEnteredDate;

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
    const { winners, totalRows, excludedNew, openStatusCounts } =
      await buildDedupMap(fileId, gdToken);

    logger.info(
      `[Enrollment Import] Dedup complete — total=${totalRows}, ` +
        `uniqueGroups=${winners.size}, excludedNew=${excludedNew}`,
    );

    if (openStatusCounts.size > 0) {
      const detail = [...openStatusCounts.entries()]
        .map(([s, n]) => `"${s}" ×${n}`)
        .join(", ");
      logger.warn(
        `[Enrollment Import] OPEN STATUS VALUES excluded pending mapping decisions: ${detail}. ` +
          "See flow header for resolution options.",
      );
    }

    // ── Map to SF records ─────────────────────────────────────────────────────
    const sfRecords: EnrollmentCopRecord[] = [];
    let skippedOpenStatus = 0;
    let skippedOutOfWindow = 0;
    let skippedUnmappable = 0;

    for (const row of winners.values()) {
      if (OPEN_STATUSES.has(str(row.Status).trim())) {
        skippedOpenStatus++;
        continue;
      }
      // Course Flow only migrates CourseOffering records within the current
      // 2-year window — an enrollment for a course outside it has nothing to
      // link to and would just fail on a missing required CourseOfferingId.
      const coursePrefix = str(row.Course_ID).trim().split("_")[0];
      if (!coursePrefix || !VALID_COURSE_ID_PREFIXES.has(coursePrefix)) {
        skippedOutOfWindow++;
        continue;
      }
      const mapped = mapToEnrollmentCop(row, studentCache, coCache, logger);
      if (!mapped) {
        skippedUnmappable++;
        continue;
      }
      sfRecords.push(mapped);
    }

    if (TEST_MODE && sfRecords.length > TEST_LIMIT) {
      sfRecords.splice(TEST_LIMIT);
      logger.info(
        `[Enrollment Import] TEST MODE: limited to first ${TEST_LIMIT} records`,
      );
    }

    logger.info(
      `[Enrollment Import] ${sfRecords.length} records ready to upsert ` +
        `(skipped ${skippedOpenStatus} open-status, ${skippedOutOfWindow} outside 2yr window, ${skippedUnmappable} unmappable)`,
    );

    if (sfRecords.length === 0) {
      logger.info("[Enrollment Import] No records to upsert — done.");
      return {
        data: { totalRows, uniqueGroups: winners.size, sfRecords: 0 },
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
    for (const rec of sfRecords) {
      const grade = rec.Grade__c as string | undefined;
      if (!grade) continue;
      const copExtId = rec.External_ID_4D__c as string;
      resultRecords.push({
        External_ID_4D__c: copExtId,
        "CourseOfferingParticipant.External_ID_4D__c": copExtId,
        LetterGrade: grade,
        ParticipantResultStatus: "Final",
      });
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
        `\n  Dedup groups:     ${winners.size}` +
        `\n  Open-status:      ${skippedOpenStatus}` +
        `\n  Outside 2yr window: ${skippedOutOfWindow}` +
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
        uniqueGroups: winners.size,
        skippedOutOfWindow,
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
