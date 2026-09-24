/**
 * Stanford CSP Migration – Student Certificate Import flow.
 *
 * Reads the Student_Certificate TSV from Google Drive and upserts two
 * independent records per row (no relationship between them — both are
 * separately derived from the same source row, per the mapping workbook):
 *
 *   1. LearnerProgram — one per row; upsert key: External_ID_4D__c = ID
 *        LearnerAccountId / LearnerContactId : from the Student cache
 *          (same pattern as enrollmentFlow.ts's buildStudentCache — these
 *          are standard lookup fields expecting a real Id, not external-ID
 *          notation, so the match happens via one pre-fetched SOQL query).
 *        LearningProgramPlan.External_ID_4D__c = Certificate_ID (external-ID
 *          notation — LearningProgramPlan already exists with this same
 *          External_ID_4D__c value, created by the Certificate Program flow).
 *        Status = raw Status, direct map (confirmed Sep 2026: LearnerProgram's
 *          picklist was expanded to include every value PersonAcademicCredential
 *          uses, so no translation table needed between the two objects).
 *        Description = Comments, with _4DNL_ stripped (same convention as
 *          every other free-text field in this project).
 *
 *   2. PersonAcademicCredential — one per row; upsert key: External_ID_4D__c
 *        = ID, same value as the LearnerProgram record above (confirmed
 *          Sep 2026 — deliberately shared, same "companion record" pattern
 *          used for LearningProgramPlan sharing LearningProgram's ID).
 *        LearnerContactId : from the same Student cache.
 *        Learning_Program__r.External_ID_4D__c = Certificate_ID (external-ID
 *          notation — links directly to LearningProgram, NOT through
 *          LearningProgramPlan; confirmed intentional asymmetry, Sep 2026 —
 *          this object's own field is Learning_Program__c, not a Plan lookup).
 *        Deadline_Quarter__r.Abbreviation__c = Deadline_Quarter, external-ID
 *          notation. AcademicTerm.Abbreviation__c is a real External ID field
 *          (Sep 2026) storing the exact same short-code format as the source
 *          ("fa15", "wi17") — no cache or translation needed, unlike
 *          AcademicSession in the Certificate Program flow.
 *        Issued__c / Issue_Date__c / Status__c : direct map.
 *        Issue_Method__c : direct map — confirmed Sep 2026 this is actually
 *          a Text Area(255), NOT a Picklist as the mapping doc claims, so no
 *          value validation is applied, just passthrough.
 *
 * Scope: only rows whose Certificate_ID is 17 or greater — matches the
 * 2-year data agreement (architect-confirmed mapping doc, Sep 2026).
 * Supersedes an earlier Deadline_Quarter-based window guess used before the
 * doc spelled out the concrete Id threshold.
 *
 * Prerequisites (must run before this flow):
 *   - Student flow             — loads Person Account / Contact (Student_ID_4D__c)
 *   - Certificate Program flow — loads LearningProgram + LearningProgramPlan
 *   - AcademicTerm records must already exist for the relevant quarters
 *     (confirmed Sep 2026 as a pre-existing standard object, not migrated by
 *     any flow in this project)
 *
 * Fields NOT mapped (Do Not Map per workbook): Student_ID_Previous,
 * Created_Date, Created_Time, Created_By, Last_Modified_Date,
 * Last_Modified_Time, Last_Modified_By.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import {
  str,
  toBool,
  toDate,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
} from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

const SF_API = "v60.0";

// Scope: only load student certificates whose Certificate_ID is
// MIN_CERTIFICATE_ID or greater — matches the 2-year data agreement
// (architect-confirmed mapping doc, Sep 2026).
const MIN_CERTIFICATE_ID = 17;

// ── Raw TSV row shape ─────────────────────────────────────────────────────────

interface StudentCertRaw {
  ID?: string;
  Student_ID?: string;
  Certificate_ID?: string;
  Status?: string;
  Comments?: string;
  Issued?: string;
  Issue_Date?: string;
  Issue_Method?: string;
  Deadline_Quarter?: string;
  // Do Not Map: Student_ID_Previous, Created_*, Last_Modified_*
}

// ── Salesforce record shapes ──────────────────────────────────────────────────

interface LearnerProgramSf {
  External_ID_4D__c: string;
  LearnerAccountId?: string;
  // Required — confirmed via live test insert (Sep 2026): REQUIRED_FIELD_MISSING
  // named this alongside LearningProgramPlanId when both were omitted. A row
  // with no Student match or no Certificate_ID is skipped entirely rather
  // than submitted (see the row loop below).
  LearnerContactId: string;
  "LearningProgramPlan.External_ID_4D__c": string;
  Status?: string;
  Description?: string;
  // Required — confirmed via live test insert (Sep 2026); NOT Auto Number as
  // originally assumed. Format confirmed via existing records in this same
  // org (a demo built by another team): "{Student Name} - {LearningProgram.Name}".
  Name: string;
}

interface PersonAcademicCredentialSf {
  External_ID_4D__c: string;
  // Required — same confirmation as LearnerProgram.LearnerContactId above.
  LearnerContactId: string;
  "Learning_Program__r.External_ID_4D__c"?: string;
  "Deadline_Quarter__r.Abbreviation__c"?: string;
  Issued__c?: boolean;
  Issue_Date__c?: string;
  Issue_Method__c?: string;
  Status__c?: string;
  // Required — hardcoded "Certificate" (confirmed Sep 2026): every row this
  // flow processes represents a CSP certificate, so this is a fixed
  // constant, not derived from source data — same pattern as
  // Payment.Type = "Capture" in registrationFlow.ts.
  CredentialType: string;
  // Required — no mapping doc guidance exists for this field (confirmed
  // Sep 2026). Format confirmed via existing records in this same org (a
  // demo built by another team): "OWC Certificate in {LearningProgram.Name}",
  // falling back to the raw Certificate_ID if the program isn't matched.
  CredentialName: string;
  // Required — no mapping doc guidance exists for this field (confirmed
  // Sep 2026). Best-available default: same value as Issue_Date, since
  // that's the only date this source file provides. A row with no
  // parseable Issue_Date has no valid value to offer here, so
  // PersonAcademicCredential is skipped for that row (LearnerProgram is
  // still created independently — this field isn't required there).
  AchievedDate: string;
  // Required — confirmed via live test insert (Sep 2026); NOT Auto Number as
  // originally assumed. Confirmed identical to CredentialName in existing
  // same-org records — always set to the same value as CredentialName above.
  Name: string;
}

// ── Student cache (same pattern as enrollmentFlow.ts) ─────────────────────────

interface StudentCacheEntry {
  contactId: string;
  accountId: string;
  name: string;
}

async function sfQueryAll<T>(
  base: string,
  token: string,
  soql: string,
): Promise<T[]> {
  const headers = { Authorization: `Bearer ${token}` };
  const all: T[] = [];
  let nextUrl: string | null = null;
  let done = false;

  const fetchPage = async (url: string | null) => {
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

  await fetchPage(null);
  while (!done && nextUrl) await fetchPage(nextUrl);
  return all;
}

async function buildStudentCache(
  base: string,
  token: string,
): Promise<Map<string, StudentCacheEntry>> {
  const records = await sfQueryAll<{
    Id: string;
    Name: string;
    PersonContactId: string;
    Student_ID_4D__c: string;
  }>(
    base,
    token,
    "SELECT Id, Name, PersonContactId, Student_ID_4D__c FROM Account WHERE IsPersonAccount = true AND Student_ID_4D__c != null",
  );
  const map = new Map<string, StudentCacheEntry>();
  for (const r of records) {
    if (r.Student_ID_4D__c) {
      map.set(r.Student_ID_4D__c, {
        contactId: r.PersonContactId,
        accountId: r.Id,
        name: r.Name,
      });
    }
  }
  return map;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function stripLineBreakTokens(v: string | undefined): string | undefined {
  const raw = str(v).trim();
  if (!raw) return undefined;
  return raw.replace(/_4DNL_/g, "\n").trim() || undefined;
}

// Strips trailing ".0" (4D Longint-to-Text export artifact).
function stripDotZero(v: string | undefined): string {
  return str(v).trim().replace(/\.0$/, "");
}

// ── Fetch TSV from Google Drive ───────────────────────────────────────────────

async function fetchTsv(
  fileId: string,
  accessToken: string,
): Promise<StudentCertRaw[]> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: StudentCertRaw[] = [];
    Papa.parse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: true,
      skipEmptyLines: true,
      quoteChar: "\0",
      step: (result: Papa.ParseResult<StudentCertRaw>) => {
        rows.push(result.data as unknown as StudentCertRaw);
      },
      complete: () => resolve(rows),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const studentCertificateImport = flow({
  name: "Student Certificate",
  stableKey: "sc9d8e7f-1a2b-4c3d-5e6f-7a8b9c0d1e2f",
  description:
    "Reads the Student_Certificate TSV from Google Drive and upserts " +
    "LearnerProgram and PersonAcademicCredential records to Salesforce " +
    "via Bulk API 2.0. Scope: Certificate_ID 17 or greater (2-year data " +
    "agreement).",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;
    logger.info("[StudentCert] Starting…");

    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = (configVars as Record<string, unknown>)[
      "Student Certificate File ID"
    ] as string;
    const failedFolderId = (configVars as Record<string, unknown>)[
      "Failed Records Folder ID"
    ] as string | undefined;

    if (!fileId)
      throw new Error("Student Certificate File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── 1. Build Student + LearningProgram caches, fetch TSV — in parallel ────
    // LearningProgram cache only feeds CredentialName's fallback-to-real-name
    // (see row loop) — the actual Certificate_ID link itself is resolved via
    // external-ID notation on ingest, no cache needed for that part.
    logger.info(
      "[StudentCert] Building Student cache, LearningProgram cache, and fetching Student_Certificate TSV…",
    );
    const [studentCache, programNameCache, rawRows] = await Promise.all([
      buildStudentCache(sfBase, sfToken),
      sfQueryAll<{ External_ID_4D__c: string; Name: string }>(
        sfBase,
        sfToken,
        "SELECT External_ID_4D__c, Name FROM LearningProgram WHERE External_ID_4D__c != null",
      ).then((rows) => {
        const map = new Map<string, string>();
        for (const r of rows) map.set(r.External_ID_4D__c.trim(), r.Name);
        return map;
      }),
      fetchTsv(fileId, gdToken),
    ]);
    logger.info(
      `[StudentCert] Student cache — ${studentCache.size} entries; ` +
        `LearningProgram cache — ${programNameCache.size} entries`,
    );

    if (rawRows.length === 0) {
      logger.info("[StudentCert] No data rows found.");
      return {
        data: {
          learnerPrograms: 0,
          credentials: 0,
          filteredOut: 0,
          skipped: 0,
        },
      };
    }
    logger.info(`[StudentCert] Parsed ${rawRows.length} rows.`);

    // ── 2. Build SF records ────────────────────────────────────────────────────
    const learnerPrograms: LearnerProgramSf[] = [];
    const credentials: PersonAcademicCredentialSf[] = [];
    let skipped = 0;
    let filteredOut = 0;
    let noStudentMatch = 0;
    let noCertId = 0;
    let noAchievedDate = 0;

    for (const raw of rawRows) {
      const externalId = str(raw.ID).trim();
      if (!externalId) {
        skipped++;
        logger.warn("[StudentCert] Skipping row with blank ID.");
        continue;
      }

      // Supersedes the earlier Deadline_Quarter-based window guess.
      const certIdNum = parseInt(str(raw.Certificate_ID).trim(), 10);
      if (!isNaN(certIdNum) && certIdNum < MIN_CERTIFICATE_ID) {
        filteredOut++;
        continue;
      }

      const deadlineQuarterRaw = str(raw.Deadline_Quarter).trim().toLowerCase();

      // LearnerContactId is required on BOTH target objects (confirmed via
      // live test insert) — a row with no Student match can't produce a
      // valid record for either, so skip it entirely rather than submit a
      // doomed row.
      const studentId = stripDotZero(raw.Student_ID);
      const student = studentId ? studentCache.get(studentId) : undefined;
      if (!student) {
        noStudentMatch++;
        logger.warn(
          `[StudentCert] ID=${externalId}: Student_ID "${studentId}" not found in SF — LearnerContactId is required on both target objects, skipping row entirely.`,
        );
        continue;
      }

      // LearningProgramPlanId is required on LearnerProgram (confirmed via
      // live test insert) — a row with no Certificate_ID can't produce a
      // valid LearnerProgram, and has nothing meaningful to link on
      // PersonAcademicCredential either, so skip it entirely.
      const certId = str(raw.Certificate_ID).trim();
      if (!certId) {
        noCertId++;
        logger.warn(
          `[StudentCert] ID=${externalId}: blank Certificate_ID — LearningProgramPlanId is required on LearnerProgram, skipping row entirely.`,
        );
        continue;
      }

      const status = str(raw.Status).trim();
      const description = stripLineBreakTokens(raw.Comments);

      const programName = programNameCache.get(certId);
      if (!programName) {
        logger.warn(
          `[StudentCert] ID=${externalId}: Certificate_ID "${certId}" not found in LearningProgram cache — Name/CredentialName will fall back to the raw Certificate_ID.`,
        );
      }
      const credentialName = programName
        ? `OWC Certificate in ${programName}`
        : certId;

      // ── LearnerProgram ──────────────────────────────────────────────────────
      const lp: LearnerProgramSf = {
        External_ID_4D__c: externalId,
        LearnerContactId: student.contactId,
        "LearningProgramPlan.External_ID_4D__c": certId,
        Name: `${student.name} - ${programName ?? certId}`,
      };
      lp.LearnerAccountId = student.accountId;
      if (status) lp.Status = status;
      if (description) lp.Description = description;
      learnerPrograms.push(lp);

      // ── PersonAcademicCredential — same External_ID_4D__c as LearnerProgram ──
      // AchievedDate is required and has no other source in this file besides
      // Issue_Date — a row with no parseable Issue_Date has no valid value to
      // offer, so the credential is skipped for that row (LearnerProgram
      // above is unaffected — this field isn't required there).
      const issueDate = toDate(raw.Issue_Date);
      if (!issueDate) {
        noAchievedDate++;
        logger.warn(
          `[StudentCert] ID=${externalId}: no parseable Issue_Date — AchievedDate is required on PersonAcademicCredential, skipping credential for this row.`,
        );
      } else {
        const pac: PersonAcademicCredentialSf = {
          External_ID_4D__c: externalId,
          LearnerContactId: student.contactId,
          CredentialType: "Certificate",
          CredentialName: credentialName,
          AchievedDate: issueDate,
          Name: credentialName,
        };
        pac["Learning_Program__r.External_ID_4D__c"] = certId;
        if (deadlineQuarterRaw)
          pac["Deadline_Quarter__r.Abbreviation__c"] = deadlineQuarterRaw;
        if (str(raw.Issued).trim()) pac.Issued__c = toBool(raw.Issued);
        pac.Issue_Date__c = issueDate;
        const issueMethod = str(raw.Issue_Method).trim();
        if (issueMethod) pac.Issue_Method__c = issueMethod; // free text, not a picklist
        if (status) pac.Status__c = status;
        credentials.push(pac);
      }
    }

    if (skipped > 0)
      logger.warn(`[StudentCert] Skipped ${skipped} rows (blank ID).`);
    if (filteredOut > 0)
      logger.info(
        `[StudentCert] Filtered ${filteredOut} rows with Certificate_ID < ${MIN_CERTIFICATE_ID} (outside 2-year data agreement).`,
      );
    if (noStudentMatch > 0)
      logger.warn(
        `[StudentCert] ${noStudentMatch} rows had no matching Student — skipped entirely.`,
      );
    if (noCertId > 0)
      logger.warn(
        `[StudentCert] ${noCertId} rows had a blank Certificate_ID — skipped entirely.`,
      );
    if (noAchievedDate > 0)
      logger.warn(
        `[StudentCert] ${noAchievedDate} rows had no parseable Issue_Date — PersonAcademicCredential skipped (LearnerProgram still created).`,
      );

    if (learnerPrograms.length === 0) {
      logger.info("[StudentCert] No valid records to upsert.");
      return {
        data: {
          learnerPrograms: 0,
          credentials: 0,
          filteredOut,
          skipped,
          noStudentMatch,
          noCertId,
          noAchievedDate,
        },
      };
    }

    // ── 3. Upsert LearnerProgram ───────────────────────────────────────────────
    logger.info(
      `[StudentCert] Upserting ${learnerPrograms.length} LearnerProgram records…`,
    );
    const lpResult = await runBulkJob(
      sfBase,
      sfToken,
      "LearnerProgram",
      "External_ID_4D__c",
      learnerPrograms as unknown as Record<string, unknown>[],
      logger,
      "[StudentCert][LearnerProgram]",
    );
    logger.info(
      `[StudentCert] LearnerProgram — processed=${lpResult.numberRecordsProcessed}, failed=${lpResult.numberRecordsFailed}`,
    );

    // ── 4. Upsert PersonAcademicCredential ─────────────────────────────────────
    // Independent of LearnerProgram — no relationship between the two objects
    // in this mapping, so no ordering dependency; runs after purely to keep
    // results-sheet reporting sequential and easy to read. Can legitimately
    // be empty even when learnerPrograms isn't (every row missing a
    // parseable Issue_Date), so guarded the same way every multi-object flow
    // in this project guards an optional phase.
    let pacResult = {
      jobId: "",
      state: "Skipped",
      numberRecordsProcessed: 0,
      numberRecordsFailed: 0,
      successfulCsv: "",
      failedCsv: "",
      failedExternalIds: new Set<string>(),
    };
    if (credentials.length > 0) {
      logger.info(
        `[StudentCert] Upserting ${credentials.length} PersonAcademicCredential records…`,
      );
      pacResult = await runBulkJob(
        sfBase,
        sfToken,
        "PersonAcademicCredential",
        "External_ID_4D__c",
        credentials as unknown as Record<string, unknown>[],
        logger,
        "[StudentCert][PersonAcademicCredential]",
      );
      logger.info(
        `[StudentCert] PersonAcademicCredential — processed=${pacResult.numberRecordsProcessed}, failed=${pacResult.numberRecordsFailed}`,
      );
    } else {
      logger.info(
        "[StudentCert] No PersonAcademicCredential records to upsert — skipping.",
      );
    }

    // ── 5. Results sheet ────────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Student Certificate",
        objects: [
          {
            objectName: "LearnerProgram",
            successfulCsv: lpResult.successfulCsv,
            failedCsv: lpResult.failedCsv,
          },
          {
            objectName: "PersonAcademicCredential",
            successfulCsv: pacResult.successfulCsv,
            failedCsv: pacResult.failedCsv,
          },
        ],
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[StudentCert] Results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(
        `[StudentCert] Could not update results sheet: ${String(err)}`,
      );
    }

    logger.info(
      `[StudentCert] Import complete —` +
        `\n  Source rows:                    ${rawRows.length}` +
        `\n  Filtered (outside 2yr window):   ${filteredOut}` +
        `\n  Skipped (blank ID):              ${skipped}` +
        `\n  No Student match (row skipped):  ${noStudentMatch}` +
        `\n  No Certificate_ID (row skipped): ${noCertId}` +
        `\n  No Issue_Date (credential only skipped): ${noAchievedDate}` +
        `\n  LearnerProgram submitted:        ${learnerPrograms.length}` +
        `\n  LearnerProgram processed:        ${lpResult.numberRecordsProcessed}` +
        `\n  LearnerProgram failed:           ${lpResult.numberRecordsFailed}` +
        `\n  PersonAcademicCredential submitted: ${credentials.length}` +
        `\n  PersonAcademicCredential processed: ${pacResult.numberRecordsProcessed}` +
        `\n  PersonAcademicCredential failed:    ${pacResult.numberRecordsFailed}`,
    );

    return {
      data: {
        learnerPrograms: learnerPrograms.length,
        learnerProgramsProcessed: lpResult.numberRecordsProcessed,
        learnerProgramsFailed: lpResult.numberRecordsFailed,
        credentials: credentials.length,
        credentialsProcessed: pacResult.numberRecordsProcessed,
        credentialsFailed: pacResult.numberRecordsFailed,
        filteredOut,
        skipped,
        noStudentMatch,
        noCertId,
        noAchievedDate,
      },
    };
  },
});

export default [studentCertificateImport];
