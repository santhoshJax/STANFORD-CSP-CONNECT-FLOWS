/**
 * Stanford CSP Migration – Certificate Course Import flow.
 *
 * Two-phase upsert from a single TSV:
 *
 *   Phase 1 — LearningAchievement (one per unique Course_Code):
 *     Upsert key : External_ID__c = Course_Code — confirmed via Object Manager
 *                  (Sep 2026); this object does NOT have External_ID_4D__c,
 *                  unlike every other object in this project. Do not "fix"
 *                  this to match the project convention — it would break the
 *                  upsert outright (field doesn't exist on this object).
 *     Name       : LearningCourse.Name + " Achievement"
 *                  e.g. "Comparative Literature Achievement"
 *     Learning_Course__c : LearningCourse SF Id (from courseCache) — confirmed
 *                  via Object Manager; this object has no Course_Code_4D__c
 *                  field at all, despite a mapping doc revision briefly
 *                  suggesting otherwise (Sep 2026) — that was a copy-paste
 *                  error from the LearningProgramPlanRequirement row above it
 *                  in the same sheet, which genuinely does have that field.
 *     RecordTypeId       : "Learning Course" RecordType (queried once at startup)
 *
 *   Phase 2 — LearningProgramPlanRequirement (one per source row):
 *     Upsert key : External_ID_4D__c = ID
 *     Name       : LearningCourse.Name (looked up via Course_Code)
 *     LearningProgramPlan.External_ID_4D__c   = Certificate_ID  ← external ID notation, no SOQL
 *     LearningAchievement.External_ID__c      = Course_Code     ← external ID notation, no query-back
 *     Course_Code_4D__c  : REMOVED — field deleted from LearningProgramPlanRqmt
 *                  in Salesforce (Sep 2026); sending it caused the whole
 *                  Bulk API job to fail with InvalidBatch before any record
 *                  was even attempted. Left commented out in the code below.
 *     Course_Type__c     : direct picklist
 *     SequenceNumber     : Course_Order parsed to integer
 *     Choose_Instructor__c : direct boolean
 *
 * Relationship strategy:
 *   - Certificate_ID links only to LearningProgramPlan via external ID
 *     notation (Sep 2026 mapping revision — no more direct
 *     Learning_Program__r reference: LearningProgramPlan already carries the
 *     link back to its parent LearningProgram, set by the Certificate
 *     Program flow, so a second direct link here would be redundant).
 *   - LearningAchievement linked by external ID notation after Phase 1
 *     upsert — no query-back needed.
 *   - Requirement Name and achievement Name/lookup require LearningCourse.Name,
 *     so a single SOQL cache (CourseNumber → {Id, Name}) is still needed.
 *
 * Prerequisites (must run before this flow):
 *   - Certificate Program flow — creates Learning, LearningProgram, and
 *     LearningProgramPlan (all three share the same
 *     External_ID_4D__c = Certificate_Program.ID)
 *   - Course flow              — creates LearningCourse records
 */

import { flow } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import {
  str,
  toBool,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
} from "./utils";
import {
  createPerObjectResultsSheet,
  appendSkippedRecords,
} from "./reportResults";
import { stripSectionSuffix } from "./courseUtils";

const SF_API = "v60.0";

const TEST_MODE = false; // set to false to process all records
const TEST_LIMIT = 10; // max rows to process when TEST_MODE is true

// Scope: only load courses whose Certificate_ID is MIN_CERTIFICATE_ID or
// greater — matches the 2-year data agreement (architect-confirmed mapping
// doc, Sep 2026).
const MIN_CERTIFICATE_ID = 17;

// ── Raw TSV row shape ─────────────────────────────────────────────────────────

interface CertCourseRaw {
  ID?: string;
  Certificate_ID?: string;
  Course_Code?: string;
  Course_Type?: string;
  Course_Order?: string;
  Choose_Instructor?: string;
}

// ── Salesforce record shapes ──────────────────────────────────────────────────

interface LearningAchievementSf {
  External_ID__c: string; // Course_Code — upsert key (NOT External_ID_4D__c on this object)
  Name: string; // LearningCourse.Name + " Achievement"
  Learning_Course__c: string; // LearningCourse SF Id
  RecordTypeId?: string; // "Learning Course" RecordType
}

interface LearningProgramPlanRequirementSf {
  External_ID_4D__c: string;
  Name?: string; // LearningCourse.Name
  "LearningProgramPlan.External_ID_4D__c"?: string; // Certificate_ID (ext ID notation)
  "LearningAchievement.External_ID__c"?: string; // Course_Code   (ext ID notation)
  // Course_Code_4D__c?: string; // REMOVED — field deleted from LearningProgramPlanRqmt in Salesforce (Sep 2026)
  Course_Type__c?: string;
  SequenceNumber?: number;
  Choose_Instructor__c?: boolean;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function readHeaders(token: string) {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}

async function sfQuery<T>(
  base: string,
  token: string,
  soql: string,
): Promise<T[]> {
  const { data } = await axios.get<{
    records: T[];
    done: boolean;
    nextRecordsUrl?: string;
  }>(`${base}/services/data/${SF_API}/query`, {
    params: { q: soql },
    headers: readHeaders(token),
  });
  return data.records;
}

// ── Fetch TSV from Google Drive ───────────────────────────────────────────────

async function fetchTsv(
  fileId: string,
  accessToken: string,
): Promise<CertCourseRaw[]> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: CertCourseRaw[] = [];
    Papa.parse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: true,
      skipEmptyLines: true,
      quoteChar: "\0",
      step: (result: Papa.ParseResult<CertCourseRaw>) => {
        rows.push(result.data as unknown as CertCourseRaw);
      },
      complete: () => resolve(rows),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const certificateCourseImport = flow({
  name: "Certificate Course",
  stableKey: "cc9d8e7f-6a5b-4c3d-2e1f-0a9b8c7d6e5f",
  description:
    "Reads the Certificate_Course TSV from Google Drive and upserts " +
    "LearningAchievement (one per unique Course_Code) then " +
    "LearningProgramPlanRequirement records to Salesforce via Bulk API 2.0.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;

    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = (configVars as Record<string, unknown>)[
      "Certificate Course File ID"
    ] as string;
    const failedFolderId = (configVars as Record<string, unknown>)[
      "Failed Records Folder ID"
    ] as string | undefined;

    if (!fileId)
      throw new Error("Certificate Course File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── 1. Pre-load LearningCourse cache (CourseNumber → {id, name}) ─────────
    // Need both Id (for Course_Code_4D__c + LearningAchievement.Learning_Course__c)
    // and Name (for requirement Name + achievement Name).
    // NOT "Code" — confirmed via a real query (Sep 2026) that LearningCourse has
    // no such field; CourseNumber is the real one (matches courseFlow.ts, which
    // already writes CourseNumber = baseCode with all whitespace stripped, e.g.
    // "OWC 101" → "OWC101" — every real CourseNumber value sampled is exactly
    // this shape: letters immediately followed by digits, no spaces).
    logger.info("[CertCourse] Loading LearningCourse cache…");
    const courseRows = await sfQuery<{
      Id: string;
      Name: string;
      CourseNumber: string;
    }>(
      sfBase,
      sfToken,
      "SELECT Id, Name, CourseNumber FROM LearningCourse WHERE CourseNumber != null",
    );
    const courseCache = new Map<string, { id: string; name: string }>();
    for (const c of courseRows)
      courseCache.set(c.CourseNumber.trim(), { id: c.Id, name: c.Name });
    logger.info(
      `[CertCourse] LearningCourse cache loaded — ${courseCache.size} entries`,
    );

    // ── 2. Pre-load LearningAchievement RecordType Id ─────────────────────────
    // "Learning Course" is the default RecordType per workbook.
    logger.info("[CertCourse] Loading LearningAchievement RecordType…");
    let achievementRecordTypeId: string | undefined;
    try {
      const rtRows = await sfQuery<{ Id: string }>(
        sfBase,
        sfToken,
        "SELECT Id FROM RecordType WHERE SObjectType = 'LearningAchievement' AND Name = 'Learning Course' LIMIT 1",
      );
      achievementRecordTypeId = rtRows[0]?.Id;
      if (achievementRecordTypeId) {
        logger.info(
          `[CertCourse] LearningAchievement RecordType Id: ${achievementRecordTypeId}`,
        );
      } else {
        logger.warn(
          "[CertCourse] LearningAchievement RecordType 'Learning Course' not found — RecordTypeId will be omitted.",
        );
      }
    } catch (err) {
      logger.warn(
        `[CertCourse] Could not query RecordType: ${String(err)} — RecordTypeId will be omitted.`,
      );
    }

    // ── 3. Fetch & parse TSV ──────────────────────────────────────────────────
    logger.info(`[CertCourse] Fetching file ${fileId}…`);
    let rawRows = await fetchTsv(fileId, gdToken);
    logger.info(`[CertCourse] Parsed ${rawRows.length} rows.`);

    // Applied before TEST_MODE slicing so a test run actually exercises
    // in-scope rows instead of whatever happens to be first in the file.
    const beforeFilter = rawRows.length;
    rawRows = rawRows.filter((raw) => {
      const certIdNum = parseInt(str(raw.Certificate_ID).trim(), 10);
      return isNaN(certIdNum) || certIdNum >= MIN_CERTIFICATE_ID;
    });
    const filteredOut = beforeFilter - rawRows.length;
    if (filteredOut > 0)
      logger.info(
        `[CertCourse] Filtered ${filteredOut} rows with Certificate_ID < ${MIN_CERTIFICATE_ID} (outside 2-year data agreement).`,
      );

    if (TEST_MODE && rawRows.length > TEST_LIMIT) {
      logger.info(
        `[CertCourse] TEST_MODE enabled — limiting to first ${TEST_LIMIT} of ${rawRows.length} in-scope rows.`,
      );
      rawRows = rawRows.slice(0, TEST_LIMIT);
    }

    if (rawRows.length === 0) {
      logger.info("[CertCourse] No data rows found.");
      return {
        data: { achievements: 0, requirements: 0, skipped: 0, filteredOut },
      };
    }

    // ── 4. Build SF records ───────────────────────────────────────────────────
    // LearningAchievement: deduped by Course_Code — one per unique course.
    const achievementMap = new Map<string, LearningAchievementSf>();
    const requirements: LearningProgramPlanRequirementSf[] = [];
    let skipped = 0;
    let noCourseMatch = 0;
    // "LearningProgramPlanRequirement - Skipped" tab: one row per skipped
    // source row (ID, Certificate_ID, Course_Code, Reason).
    const skippedRequirementRows: string[][] = [];
    // "LearningAchievement - Skipped" tab: one row per DISTINCT course code
    // that was referenced but never matched — deduped the same way
    // achievementMap itself is deduped.
    const skippedAchievementRows: string[][] = [];
    const loggedMissingCourseCodes = new Set<string>();

    for (const raw of rawRows) {
      const externalId = str(raw.ID).trim();
      if (!externalId) {
        skipped++;
        logger.warn("[CertCourse] Skipping row with blank ID.");
        continue;
      }

      // Normalize to match CourseNumber's stored shape, e.g. "OWC 101 N@" →
      // "OWC101" (architect confirmed, Sep 2026):
      //   1. Strip "@" — confirmed junk, safe to discard.
      //   2. Strip the trailing section letter ("N") via the same
      //      stripSectionSuffix() courseFlow.ts already uses for this exact
      //      pattern — it saves that letter separately on
      //      CourseOffering.SectionNumber there, but this flow only needs to
      //      match the base course, not preserve the section.
      //   3. Strip remaining whitespace, matching how courseFlow.ts writes
      //      CourseNumber itself ("OWC 101" → "OWC101").
      const courseCodeRaw = str(raw.Course_Code).trim();
      const courseCodeNoAt = courseCodeRaw.replace(/@/g, "").trim();
      const courseCodeBase =
        stripSectionSuffix(courseCodeNoAt) || courseCodeNoAt;
      const courseCode = courseCodeBase.replace(/\s+/g, "");
      const certId = str(raw.Certificate_ID).trim();
      const course = courseCode ? courseCache.get(courseCode) : undefined;

      // Name and LearningAchievementId are both required on
      // LearningProgramPlanRqmt (confirmed via live test insert, Sep 2026) —
      // neither can be filled without a matched course, so a row with no
      // match can never succeed. Skip it entirely (both the requirement AND,
      // if this course was never matched before, a note on the achievement
      // side) rather than submit a doomed record — same pattern already used
      // in studentCertificateFlow.ts for rows with no Student match.
      if (!course) {
        noCourseMatch++;
        const reason = courseCode
          ? `Course_Code "${courseCodeRaw}" (normalized "${courseCode}") not found in LearningCourse`
          : "Blank Course_Code (Elective placeholder — no specific course given)";
        logger.warn(
          `[CertCourse] ID=${externalId}: ${reason} — Name/LearningAchievementId required but unavailable, skipping requirement row entirely.`,
        );
        skippedRequirementRows.push([
          externalId,
          certId,
          courseCodeRaw,
          reason,
        ]);
        if (courseCode && !loggedMissingCourseCodes.has(courseCode)) {
          loggedMissingCourseCodes.add(courseCode);
          skippedAchievementRows.push([
            courseCode,
            "Not found in LearningCourse — course may fall outside Course flow's 2-year load window, or doesn't exist",
          ]);
        }
        continue;
      }

      // ── LearningAchievement (deduped) ──────────────────────────────────────
      if (!achievementMap.has(courseCode)) {
        const achievement: LearningAchievementSf = {
          External_ID__c: courseCode, // NOT External_ID_4D__c — confirmed via Object Manager
          Name: `${course.name} Achievement`,
          Learning_Course__c: course.id,
        };
        if (achievementRecordTypeId)
          achievement.RecordTypeId = achievementRecordTypeId;
        achievementMap.set(courseCode, achievement);
      }

      // ── LearningProgramPlanRequirement ────────────────────────────────────
      const req: LearningProgramPlanRequirementSf = {
        External_ID_4D__c: externalId,
        Name: course.name, // workbook: "Course Name found by Course_Code"
        "LearningAchievement.External_ID__c": courseCode, // ext ID notation, upserted in Phase 1
      };

      // Certificate_ID links to LearningProgramPlan via external ID notation.
      // No direct Learning_Program__r reference — LearningProgramPlan already
      // carries that link (set by the Certificate Program flow), so a second
      // direct link here would be redundant (architect confirmation, Sep 2026).
      if (certId) req["LearningProgramPlan.External_ID_4D__c"] = certId;

      // Course_Code_4D__c = LearningCourse SF Id — REMOVED — field deleted
      // from LearningProgramPlanRqmt in Salesforce (Sep 2026).
      // req.Course_Code_4D__c = course.id;

      const courseType = str(raw.Course_Type).trim();
      if (courseType) req.Course_Type__c = courseType;

      const courseOrder = str(raw.Course_Order).trim();
      if (courseOrder) {
        const n = parseInt(courseOrder, 10);
        if (!isNaN(n)) req.SequenceNumber = n;
      }

      if (str(raw.Choose_Instructor).trim())
        req.Choose_Instructor__c = toBool(raw.Choose_Instructor);

      requirements.push(req);
    }

    if (skipped > 0) logger.warn(`[CertCourse] Skipped ${skipped} rows.`);
    if (noCourseMatch > 0)
      logger.warn(
        `[CertCourse] ${noCourseMatch} rows had no course match — requirement skipped entirely (see "LearningProgramPlanRequirement - Skipped" tab).`,
      );

    const achievements = [...achievementMap.values()];

    if (achievements.length === 0 && requirements.length === 0) {
      logger.info("[CertCourse] No valid records to upsert.");
      return {
        data: {
          achievements: 0,
          requirements: 0,
          skipped,
          filteredOut,
          noCourseMatch,
        },
      };
    }

    // ── 5. Phase 1: Upsert LearningAchievement ────────────────────────────────
    let achProcessed = 0,
      achFailed = 0;
    let achSuccessfulCsv = "",
      achFailedCsv = "";

    if (achievements.length > 0) {
      logger.info(
        `[CertCourse] Phase 1 — Upserting ${achievements.length} LearningAchievement records…`,
      );
      const achResult = await runBulkJob(
        sfBase,
        sfToken,
        "LearningAchievement",
        "External_ID__c", // NOT External_ID_4D__c — confirmed via Object Manager
        achievements as unknown as Record<string, unknown>[],
        logger,
        "[CertCourse][LearningAchievement]",
      );
      achProcessed = achResult.numberRecordsProcessed;
      achFailed = achResult.numberRecordsFailed;
      achSuccessfulCsv = achResult.successfulCsv;
      achFailedCsv = achResult.failedCsv;
      logger.info(
        `[CertCourse] LearningAchievement — processed=${achProcessed}, failed=${achFailed}`,
      );
    }

    // ── 6. Phase 2: Upsert LearningProgramPlanRequirement ────────────────────
    let reqProcessed = 0,
      reqFailed = 0;
    let reqSuccessfulCsv = "",
      reqFailedCsv = "";

    if (requirements.length > 0) {
      logger.info(
        `[CertCourse] Phase 2 — Upserting ${requirements.length} LearningProgramPlanRequirement records…`,
      );
      const reqResult = await runBulkJob(
        sfBase,
        sfToken,
        "LearningProgramPlanRqmt", // real API name — confirmed via Object Manager (Sep 2026); "Requirement" is abbreviated "Rqmt" in the actual object, unlike the full-word label
        "External_ID_4D__c",
        requirements as unknown as Record<string, unknown>[],
        logger,
        "[CertCourse][LearningProgramPlanRequirement]",
      );
      reqProcessed = reqResult.numberRecordsProcessed;
      reqFailed = reqResult.numberRecordsFailed;
      reqSuccessfulCsv = reqResult.successfulCsv;
      reqFailedCsv = reqResult.failedCsv;
      logger.info(
        `[CertCourse] LearningProgramPlanRequirement — processed=${reqProcessed}, failed=${reqFailed}`,
      );
    }

    // ── 7. Results sheet ──────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Certificate Course",
        objects: [
          {
            objectName: "LearningAchievement",
            successfulCsv: achSuccessfulCsv,
            failedCsv: achFailedCsv,
          },
          {
            objectName: "LearningProgramPlanRequirement",
            successfulCsv: reqSuccessfulCsv,
            failedCsv: reqFailedCsv,
          },
        ],
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[CertCourse] Results sheet: ${sheet.url}`);

      if (skippedAchievementRows.length > 0) {
        await appendSkippedRecords(
          sheet.spreadsheetId,
          gdToken,
          ["Course_Code", "Reason"],
          skippedAchievementRows,
          "LearningAchievement - Skipped",
        );
      }
      if (skippedRequirementRows.length > 0) {
        await appendSkippedRecords(
          sheet.spreadsheetId,
          gdToken,
          ["ID", "Certificate_ID", "Course_Code", "Reason"],
          skippedRequirementRows,
          "LearningProgramPlanRequirement - Skipped",
        );
      }
    } catch (err: unknown) {
      logger.warn(
        `[CertCourse] Could not update results sheet: ${String(err)}`,
      );
    }

    logger.info(
      `[CertCourse] Import complete —` +
        `\n  LearningAchievement submitted:            ${achievements.length}` +
        `\n  LearningAchievement processed:            ${achProcessed}` +
        `\n  LearningAchievement failed:               ${achFailed}` +
        `\n  LearningProgramPlanRequirement submitted: ${requirements.length}` +
        `\n  LearningProgramPlanRequirement processed: ${reqProcessed}` +
        `\n  LearningProgramPlanRequirement failed:    ${reqFailed}` +
        `\n  Rows skipped (blank ID):                  ${skipped}` +
        `\n  Rows skipped (no course match):           ${noCourseMatch}` +
        `\n  Rows filtered (Certificate_ID < ${MIN_CERTIFICATE_ID}):      ${filteredOut}`,
    );

    return {
      data: {
        achievements: achievements.length,
        achievementsProcessed: achProcessed,
        achievementsFailed: achFailed,
        requirements: requirements.length,
        requirementsProcessed: reqProcessed,
        requirementsFailed: reqFailed,
        skipped,
        noCourseMatch,
        filteredOut,
      },
    };
  },
});

export default [certificateCourseImport];
