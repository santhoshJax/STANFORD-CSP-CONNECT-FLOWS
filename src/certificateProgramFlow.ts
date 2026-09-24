/**
 * Stanford CSP Migration – Certificate Program Import flow.
 *
 * Reads the Certificate_Program TSV from Google Drive and upserts, in order
 * (each phase's parent must exist before the next phase links to it):
 *   0. Learning             — one per TSV row; upsert key: External_ID_4D__c = ID.
 *                             Type hardcoded "LearningProgram" (no space — matches
 *                             the API-name convention already used in courseFlow.ts's
 *                             Learning.Type = "LearningCourse", not the doc's literal
 *                             "Learning Program" wording). IsActive is the *inverse*
 *                             of source Inactive (source stores it backwards per
 *                             architect confirmation, Sep 2026).
 *   1. LearningProgram       — one per TSV row; upsert key: External_ID_4D__c = ID.
 *                             Learning = { External_ID_4D__c } — Master-Detail
 *                             (Unique) to the Learning record from phase 0;
 *                             confirmed Master-Detail via org schema, not a plain
 *                             Lookup as the mapping doc's "Lookup" Map Type implied,
 *                             which is why phase 0 must fully complete first.
 *   2. LearningProgramPlan   — one per LearningProgram (companion record required
 *                             by Education Cloud data model); same External_ID_4D__c,
 *                             Name = LearningProgram.Name + " Plan" (doc omits a Name
 *                             mapping for this object; architect confirmed keeping
 *                             this existing convention, Sep 2026).
 *
 * Fields NOT mapped (SA Decision – Do Not Map):
 *   Handbook_Document_ID — IDs don't match Textbook file; Amy will add manually
 *   LearningProgram.Inactive → IsActive — not mapped per spec (Learning.IsActive
 *   IS mapped, inverted, per phase 0 above — the two objects are handled differently)
 *
 * NOTE — Learning is also written by courseFlow.ts (keyed by course base code,
 * e.g. "OWC 101"), sharing this same External_ID_4D__c field on the same object.
 * No prefix applied here: course base codes are always letter-prefixed while
 * Certificate_Program IDs are bare small integers, so the two key spaces don't
 * overlap in practice. Revisit if that assumption ever changes.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import {
  str,
  toBool,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
} from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// Scope: only load certificates with an Id of MIN_CERTIFICATE_ID or greater —
// matches the 2-year data agreement (architect-confirmed mapping doc, Sep 2026).
const MIN_CERTIFICATE_ID = 17;

// ── Raw TSV row shape ─────────────────────────────────────────────────────────

interface CertProgramRaw {
  ID?: string;
  Name?: string;
  Cohort_Number?: string;
  Start_Quarter?: string;
  Deadline_Quarter?: string;
  Handbook_Document_ID?: string; // Do Not Map
  Minimum_Grade?: string;
  Min_Avg_Grade?: string;
  Inactive?: string; // Do Not Map
  Comments?: string;
}

// ── Salesforce record shapes ──────────────────────────────────────────────────

interface LearningSf {
  External_ID_4D__c: string;
  Name: string;
  Description?: string;
  IsActive: boolean;
  Type: string;
}

interface LearningProgramSf {
  External_ID_4D__c: string;
  Name: string;
  Cohort_Number__c?: string;
  // Lookup(Academic Term) — confirmed via Object Manager (Sep 2026), NOT
  // AcademicSession. AcademicTerm.Abbreviation__c is itself a real External
  // ID field, so these are set via external-ID relationship notation instead
  // of a pre-fetched Id cache (same pattern as studentCertificateFlow.ts).
  "Start_Quarter__r.Abbreviation__c"?: string;
  "Deadline_Quarter__r.Abbreviation__c"?: string;
  Minimum_Grade__c?: string;
  Min_Avg_Grade__c?: string;
  Description?: string;
  "Learning.External_ID_4D__c": string; // Master-Detail (Unique) — same ID as phase 0
}

interface LearningProgramPlanSf {
  External_ID_4D__c: string; // same ID as parent LearningProgram
  Name: string; // LearningProgram.Name + " Plan"
  "LearningProgram.External_ID_4D__c": string; // relationship to parent
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function stripLineBreakTokens(v: string | undefined): string | undefined {
  const raw = str(v).trim();
  if (!raw) return undefined;
  return raw.replace(/_4DNL_/g, "\n").trim() || undefined;
}

// ── Fetch TSV from Google Drive ───────────────────────────────────────────────

async function fetchTsv(
  fileId: string,
  accessToken: string,
): Promise<CertProgramRaw[]> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: CertProgramRaw[] = [];
    Papa.parse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: true,
      skipEmptyLines: true,
      quoteChar: "\0",
      step: (result: Papa.ParseResult<CertProgramRaw>) => {
        rows.push(result.data as unknown as CertProgramRaw);
      },
      complete: () => resolve(rows),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const certificateProgramImport = flow({
  name: "Certificate Program",
  stableKey: "c1d2e3f4-a5b6-4c7d-8e9f-0a1b2c3d4e5f",
  description:
    "Reads the Certificate_Program TSV from Google Drive and upserts " +
    "Learning, LearningProgram, and LearningProgramPlan records to " +
    "Salesforce via Bulk API 2.0.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;

    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = (configVars as Record<string, unknown>)[
      "Certificate Program File ID"
    ] as string;
    const failedFolderId = (configVars as Record<string, unknown>)[
      "Failed Records Folder ID"
    ] as string | undefined;

    if (!fileId)
      throw new Error("Certificate Program File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── 2. Fetch & parse TSV ──────────────────────────────────────────────────
    logger.info(`[CertProgram] Fetching file ${fileId}…`);
    const rawRows = await fetchTsv(fileId, gdToken);

    if (rawRows.length === 0) {
      logger.info("[CertProgram] No data rows found.");
      return { data: { learning: 0, programs: 0, plans: 0, skipped: 0 } };
    }
    logger.info(`[CertProgram] Parsed ${rawRows.length} rows.`);

    // ── 3. Build SF records ───────────────────────────────────────────────────
    const learningRecords: LearningSf[] = [];
    const records: LearningProgramSf[] = [];
    const planRecords: LearningProgramPlanSf[] = [];
    let skipped = 0;
    let filteredOut = 0;

    for (const raw of rawRows) {
      const externalId = str(raw.ID).trim();
      if (!externalId) {
        skipped++;
        logger.warn("[CertProgram] Skipping row with blank ID.");
        continue;
      }

      const idNum = parseInt(externalId, 10);
      if (!isNaN(idNum) && idNum < MIN_CERTIFICATE_ID) {
        filteredOut++;
        continue;
      }

      const name = str(raw.Name).trim();
      if (!name) {
        skipped++;
        logger.warn(`[CertProgram] Skipping ID=${externalId}: blank Name.`);
        continue;
      }

      // Phase 0 — Learning. Source Inactive is stored backwards (architect
      // confirmed, Sep 2026): Inactive="FALSE" means the program IS active.
      const description = stripLineBreakTokens(raw.Comments);
      const learning: LearningSf = {
        External_ID_4D__c: externalId,
        Name: name,
        IsActive: !toBool(raw.Inactive),
        Type: "LearningProgram", // no space — matches courseFlow.ts's Type: "LearningCourse" convention
      };
      if (description) learning.Description = description;
      learningRecords.push(learning);

      const record: LearningProgramSf = {
        External_ID_4D__c: externalId,
        Name: name,
        "Learning.External_ID_4D__c": externalId,
      };

      const cohortNumber = str(raw.Cohort_Number).trim();
      if (cohortNumber) record.Cohort_Number__c = cohortNumber;

      // Start_Quarter__c/Deadline_Quarter__c are Lookup(Academic Term), not
      // AcademicSession (confirmed via Object Manager, Sep 2026).
      // AcademicTerm.Abbreviation__c is itself a real External ID field, so
      // these are set via external-ID relationship notation — no pre-fetch
      // cache needed (same pattern as studentCertificateFlow.ts).
      const startQuarter = str(raw.Start_Quarter).trim().toLowerCase();
      if (startQuarter)
        record["Start_Quarter__r.Abbreviation__c"] = startQuarter;

      const deadlineQuarter = str(raw.Deadline_Quarter).trim().toLowerCase();
      if (deadlineQuarter)
        record["Deadline_Quarter__r.Abbreviation__c"] = deadlineQuarter;

      const minGrade = str(raw.Minimum_Grade).trim();
      if (minGrade) record.Minimum_Grade__c = minGrade;

      const minAvgGrade = str(raw.Min_Avg_Grade).trim();
      if (minAvgGrade) record.Min_Avg_Grade__c = minAvgGrade;

      if (description) record.Description = description; // same value computed for Learning, phase 0

      records.push(record);

      // LearningProgramPlan — companion record, one per LearningProgram
      planRecords.push({
        External_ID_4D__c: externalId,
        Name: `${name} Plan`,
        "LearningProgram.External_ID_4D__c": externalId,
      });
    }

    if (skipped > 0) {
      logger.warn(`[CertProgram] Skipped ${skipped} rows.`);
    }
    if (filteredOut > 0) {
      logger.info(
        `[CertProgram] Filtered ${filteredOut} rows with Id < ${MIN_CERTIFICATE_ID} (outside 2-year data agreement).`,
      );
    }

    if (records.length === 0) {
      logger.info("[CertProgram] No valid records to upsert.");
      return {
        data: { learning: 0, programs: 0, plans: 0, skipped, filteredOut },
      };
    }

    // ── 4a. Upsert Learning ────────────────────────────────────────────────────
    // Must fully complete before LearningProgram — LearningProgram.Learning is a
    // Master-Detail (Unique) relationship, not a plain Lookup, so LearningProgram
    // cannot be created without its Learning parent already existing.
    logger.info(
      `[CertProgram] Upserting ${learningRecords.length} Learning records…`,
    );
    const learningResult = await runBulkJob(
      sfBase,
      sfToken,
      "Learning",
      "External_ID_4D__c",
      learningRecords as unknown as Record<string, unknown>[],
      logger,
      "[CertProgram][Learning]",
    );
    logger.info(
      `[CertProgram] Learning — processed=${learningResult.numberRecordsProcessed}, failed=${learningResult.numberRecordsFailed}`,
    );

    // ── 4b. Upsert LearningProgram ────────────────────────────────────────────
    logger.info(
      `[CertProgram] Upserting ${records.length} LearningProgram records…`,
    );
    const programResult = await runBulkJob(
      sfBase,
      sfToken,
      "LearningProgram",
      "External_ID_4D__c",
      records as unknown as Record<string, unknown>[],
      logger,
      "[CertProgram][LearningProgram]",
    );
    logger.info(
      `[CertProgram] LearningProgram — processed=${programResult.numberRecordsProcessed}, failed=${programResult.numberRecordsFailed}`,
    );

    // ── 4c. Upsert LearningProgramPlan ────────────────────────────────────────
    // Parents must exist before plans are linked — upsert after LearningProgram job.
    logger.info(
      `[CertProgram] Upserting ${planRecords.length} LearningProgramPlan records…`,
    );
    const planResult = await runBulkJob(
      sfBase,
      sfToken,
      "LearningProgramPlan",
      "External_ID_4D__c",
      planRecords as unknown as Record<string, unknown>[],
      logger,
      "[CertProgram][LearningProgramPlan]",
    );
    logger.info(
      `[CertProgram] LearningProgramPlan — processed=${planResult.numberRecordsProcessed}, failed=${planResult.numberRecordsFailed}`,
    );

    // ── 5. Results sheet ──────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Certificate Program",
        objects: [
          {
            objectName: "Learning",
            successfulCsv: learningResult.successfulCsv,
            failedCsv: learningResult.failedCsv,
          },
          {
            objectName: "LearningProgram",
            successfulCsv: programResult.successfulCsv,
            failedCsv: programResult.failedCsv,
          },
          {
            objectName: "LearningProgramPlan",
            successfulCsv: planResult.successfulCsv,
            failedCsv: planResult.failedCsv,
          },
        ],
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[CertProgram] Results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(
        `[CertProgram] Could not update results sheet: ${String(err)}`,
      );
    }

    logger.info(
      `[CertProgram] Import complete — learning=${learningRecords.length}, programs=${records.length}, plans=${planRecords.length}, skipped=${skipped}, filteredOut=${filteredOut}.`,
    );
    return {
      data: {
        learning: learningRecords.length,
        programs: records.length,
        plans: planRecords.length,
        skipped,
        filteredOut,
      },
    };
  },
});

export default [certificateProgramImport];
