/**
 * Stanford CSP Migration – Associate Import flow.
 *
 * Streams the associate TSV from Google Drive in MAX_ROWS-row windows.
 * Maps rows to Salesforce Person Account records and bulk-upserts via
 * Bulk API 2.0. Recurses via context.invokeFlow until the full file
 * has been processed.
 *
 * Pre-match logic:
 *   Before upserting, queries SF by email to find existing Person Accounts.
 *   Match = email match AND (name forward OR name swapped).
 *   Matched records are upserted on the highest-priority typed ID already set
 *   on the existing record (Student > Instructor), adding Associate_ID_4D__c
 *   to the record. Associate is always the lowest priority — under the flow
 *   execution order Student → Instructor → Associate, both other typed IDs
 *   may already exist by the time this flow runs. Unmatched records (and
 *   records matched to neither a Student nor an Instructor) insert/update as
 *   the associate's own whole record on Associate_ID_4D__c.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import {
  str,
  hasValue,
  toBool,
  toDate,
  normalizeCountryName,
  normalizeStateName,
  normalizeUniversityId,
  cleanEmail,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
  resolveAccountIdsByField,
  queryAccountsByEmail,
  queryExistingExternalIds,
  nameMatches,
  pickFields,
  resolveCountryAndState,
  isBlankStatePlaceholder,
} from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 3000; // enough to cover the full ~2-3k associate file in one window

const CORRECTION_FIELDS: string[] = [
  "PersonMailingStreet",
  "PersonMailingCity",
  "PersonMailingState",
  "PersonMailingPostalCode",
  "PersonMailingCountry",
  "University_ID__pc",
  "PersonEmail",
  "FirstName",
];

// These address fields are authoritative from the correction sheet even when the
// cell is blank — every record on the sheet was flagged for an address problem,
// so a blank Country/State/PostalCode means "clear this," not "no correction, use
// the original file's value." Falling back to the original value here just
// re-introduces the same bad data the correction was meant to fix. Street/City
// keep the opposite rule: a blank cell means "no override."
const ALWAYS_APPLY_FIELDS = new Set([
  "PersonMailingCountry",
  "PersonMailingState",
  "PersonMailingPostalCode",
]);

const CORRECTION_TAB = "Associate";

// ── Raw TSV record ─────────────────────────────────────────────────────────────

interface RawAssociateRecord {
  ID?: string;
  Last_Name?: string;
  First_Name?: string;
  Middle_Name?: string;
  Name_Suffix?: string;
  Title?: string;
  Department?: string;
  Username?: string;
  SUNet_ID?: string;
  Email_Address?: string;
  Home_Phone?: string;
  Work_Phone?: string;
  Mobile_Phone?: string;
  Address_Line1?: string;
  Address_Line2?: string;
  City?: string;
  State?: string;
  Zip?: string;
  Country?: string;
  Stanford_Mail_Code?: string;
  Student_ID?: string;
  Active?: string;
  Do_Not_Contact?: string;
  Notes?: string;
  University_ID?: string;
  Legal_Name?: string;
  First_Quarter?: string;
  Most_Recent_Quarter?: string;
  Out_Of_State_Fee?: string;
  Course_Solicitations?: string;
  Alert_Flag?: string;
  Email_Address_For_Students?: string;
  Total_Courses?: string;
  // Benefit_Status?: string; // retired field
  Salary_Category?: string;
  Harassment_Training_Complete?: string;
  [key: string]: string | undefined;
}

// ── Salesforce Person Account shape ───────────────────────────────────────────

interface SalesforceAssociateAccount {
  Associate_ID_4D__c: string;
  Associate_ID_4D__pc: string;
  LastName: string;
  FirstName?: string;
  MiddleName?: string;
  Suffix?: string;
  PersonTitle?: string;
  PersonDepartment?: string;
  Username_4D__pc?: string;
  SUNet_Id__pc?: string;
  PersonEmail?: string;
  PersonHomePhone?: string;
  Phone?: string;
  PersonMobilePhone?: string;
  PersonMailingStreet?: string;
  PersonMailingCity?: string;
  PersonMailingState?: string;
  PersonMailingPostalCode?: string;
  PersonMailingCountry?: string;
  // Stanford_Mail_Code__pc?: string; // Do Not Map — Holly confirmed 9/2 this
  // does not need to be migrated (mapping doc, Julia's update).
  // Cross-reference only (per mapping doc) — the associate's OWN "if they
  // also have a separate Student record" note from the source TSV. This
  // upserts to Account (Person Account), where a Contact custom field
  // Student_ID__c mirrors as Student_ID__pc — deliberately a different name
  // from Student_ID_4D__c/__pc (the real Student upsert key used below), so
  // the two can never collide when both are set on the same payload.
  Student_ID__pc?: number;
  Active__pc?: boolean;
  Do_Not_Contact__pc?: boolean;
  Description?: string;
  University_ID__pc?: string;
  // Legal_Name__pc?: string; // moved to PersonEmployment
  First_Quarter__pc?: string;
  Most_Recent_Quarter__pc?: string;
  // Out_Of_State_Fee__pc?: boolean; // moved to PersonEmployment
  Course_Solicitations__pc?: boolean;
  Alert_Flag__pc?: boolean;
  Email_Address_For_Students__pc?: string;
  Total_Courses__pc?: number;
  // Benefit_Status__pc?: string; // retired field
  Salary_Category__pc?: string;
  Harassment_Training_Complete__pc?: string;
}

// Fields that don't exist on the Student/Instructor schema at all — safe to
// layer on top of an existing Student or Instructor record without touching
// anything that record already owns. Per architect spec (Aug 2026 enhancement).
const MERGE_OVERLAY_FIELDS: (keyof SalesforceAssociateAccount)[] = [
  "PersonTitle",
  "PersonDepartment",
  "Active__pc",
  "Do_Not_Contact__pc",
  "PersonHomePhone",
  "Phone",
  "PersonMobilePhone",
  // "Student_ID__pc", // Per architect (Aug 2026): when a matching Student is
  // found by name+email, do NOT overlay this associate row's own
  // self-reported Student ID onto that matched record — leave it untouched.
  // Only set it on the "no match found" / brand-new-record path, which
  // already happens via the normal full field mapping outside this list.
  "Suffix",
  "SUNet_Id__pc",
  "Username_4D__pc",
];

// ── PersonEmployment shape ────────────────────────────────────────────────────

interface PersonEmploymentRecord {
  External_ID_4D__c: string;
  Name: string;
  RelatedPersonId?: string;
  AccountId?: string;
  Salary_Category__c?: string;
  Legal_Name__c?: string;
  Out_Of_State_Fee__c?: boolean;
  // Benefit_Status__c?: string;           // retired field
  // Total_Courses__c?: number;            // stays on Contact for associates per mapping doc
  // Field name confirmed against instructorFlow.ts's working PersonEmployment
  // write — "Harassment_Training_Complete__c" doesn't exist on PersonEmployment
  // and caused a job-level InvalidBatch failure (Sep 2026).
  Harassment_Training__c?: string;
}

interface StreamResult {
  accounts: Partial<SalesforceAssociateAccount>[];
  personEmployments: Partial<PersonEmploymentRecord>[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
  allCorrectionRecordsFound: boolean;
}

// ── Field mapping ──────────────────────────────────────────────────────────────

function mapToAccount(
  raw: RawAssociateRecord,
): Partial<SalesforceAssociateAccount> {
  const id = str(raw.ID).trim();
  const record: Partial<SalesforceAssociateAccount> = {
    Associate_ID_4D__c: id,
    Associate_ID_4D__pc: id,
    LastName: str(raw.Last_Name),
  };

  const setStr = (
    key: keyof SalesforceAssociateAccount,
    v: string | undefined,
  ) => {
    const val = str(v);
    if (val) (record as Record<string, unknown>)[key] = val;
  };

  setStr("FirstName", raw.First_Name);
  setStr("MiddleName", raw.Middle_Name);
  setStr("Suffix", raw.Name_Suffix);

  // Salesforce's standard Title field has an 80-character platform limit
  // (same as Instructor) — not 128.
  const title = str(raw.Title).slice(0, 80);
  if (title) record.PersonTitle = title;

  setStr("PersonDepartment", raw.Department);
  setStr("Username_4D__pc", raw.Username);
  setStr("SUNet_Id__pc", raw.SUNet_ID);
  const email = cleanEmail(raw.Email_Address);
  if (email) record.PersonEmail = email;
  setStr("PersonHomePhone", raw.Home_Phone);
  setStr("Phone", raw.Work_Phone);
  setStr("PersonMobilePhone", raw.Mobile_Phone);

  record.PersonMailingStreet = [str(raw.Address_Line1), str(raw.Address_Line2)]
    .filter(Boolean)
    .join("\n");
  record.PersonMailingCity = str(raw.City);
  {
    const { country, state } = resolveCountryAndState(raw.Country, raw.State);
    record.PersonMailingCountry = country;
    record.PersonMailingState = state;
  }
  // Blank out only when country resolved to a known non-US value. If
  // country is blank we can't tell whether the address is US or not, so
  // keep the zip as-is (same rule as State above, and as Student/Instructor).
  record.PersonMailingPostalCode =
    record.PersonMailingCountry === "United States" ||
    !record.PersonMailingCountry
      ? str(raw.Zip)
      : "";

  // Stanford_Mail_Code: Do Not Map — Holly confirmed 9/2 this does not need
  // to be migrated (mapping doc, Julia's update).
  const studentId = parseInt(str(raw.Student_ID), 10);
  if (!isNaN(studentId) && studentId !== 0) record.Student_ID__pc = studentId;

  if (hasValue(raw.Active)) record.Active__pc = toBool(raw.Active);
  if (hasValue(raw.Do_Not_Contact))
    record.Do_Not_Contact__pc = toBool(raw.Do_Not_Contact);

  const notes = str(raw.Notes).replace(/_4DNL_/g, "\n");
  if (notes) record.Description = notes;

  // Same 8-digit rule correction mode already applies (normalizeUniversityId):
  // a 7-digit value gets a leading zero; anything that still isn't exactly
  // 8 digits afterward is dropped rather than sent to a field with an
  // 8-digit validation rule.
  const universityId = normalizeUniversityId(raw.University_ID);
  if (universityId) record.University_ID__pc = universityId;
  // setStr("Legal_Name__pc", raw.Legal_Name); // moved to PersonEmployment

  const firstQ = str(raw.First_Quarter).replace(/\.0$/, "");
  if (firstQ) record.First_Quarter__pc = firstQ;
  const mostRecentQ = str(raw.Most_Recent_Quarter).replace(/\.0$/, "");
  if (mostRecentQ) record.Most_Recent_Quarter__pc = mostRecentQ;

  // if (hasValue(raw.Out_Of_State_Fee)) // moved to PersonEmployment
  //   record.Out_Of_State_Fee__pc = toBool(raw.Out_Of_State_Fee);
  if (hasValue(raw.Course_Solicitations))
    record.Course_Solicitations__pc = toBool(raw.Course_Solicitations);
  if (hasValue(raw.Alert_Flag)) record.Alert_Flag__pc = toBool(raw.Alert_Flag);

  const totalCourses = parseInt(str(raw.Total_Courses), 10);
  if (!isNaN(totalCourses) && totalCourses !== 0)
    record.Total_Courses__pc = totalCourses;

  return record;
}

// Per mapping doc (mirrors Instructor's Harassment_Training_Complete handling):
// source contains junk placeholder dates ("00/00/00", "0000-00-00", etc.)
// that must be filtered to null instead of parsed/passed through as-is.
function isJunkDatePlaceholder(v: string | undefined): boolean {
  return /^0+([./-]0+){2}$/.test(str(v));
}

// ── Field mapping — PersonEmployment ──────────────────────────────────────────

// PersonEmployment creation for an Associate row (Aug 2026 revision — Salary_Category
// filter removed per architect):
//   1. Checked FIRST, in onExecution (step 7a, "Skip Associate PersonEmployment
//      where an Instructor-sourced PersonEmployment already exists for the
//      same person"): does this same person already have an instructor_{id}
//      PersonEmployment record? If yes, no Associate PersonEmployment is
//      created at all — regardless of what this row's own data looks like.
//   2. Only if #1 finds no existing Instructor PersonEmployment: a row here
//      becomes a PE candidate as long as it has a valid ID — Salary_Category
//      is no longer a gate (previously required; that filter was dropped).
function mapToPersonEmployment(
  raw: RawAssociateRecord,
): Partial<PersonEmploymentRecord> | null {
  const id = str(raw.ID);
  if (!id) return null; // only gate left — a valid row ID

  const name =
    [str(raw.First_Name), str(raw.Last_Name)].filter(Boolean).join(" ") || id;

  const record: Partial<PersonEmploymentRecord> = {
    External_ID_4D__c: `associate_${id}`,
    Name: name,
  };

  const setStr = (key: keyof PersonEmploymentRecord, v: string | undefined) => {
    const val = str(v);
    if (val) (record as Record<string, unknown>)[key] = val;
  };

  // Restricted picklist — same fix as Instructor: source values already
  // match the picklist verbatim except for two known mismatches, the
  // "Contigent"/"Contingent" typo and "volunteer" (source lowercase,
  // picklist stores "Volunteer" with a capital V).
  const salaryCategoryRaw = str(raw.Salary_Category);
  const salaryCategory = /^Contin?gent$/i.test(salaryCategoryRaw)
    ? "Cont"
    : /^volunteer$/i.test(salaryCategoryRaw)
      ? "Volunteer"
      : salaryCategoryRaw;
  if (salaryCategory) record.Salary_Category__c = salaryCategory;
  setStr("Legal_Name__c", raw.Legal_Name);
  if (hasValue(raw.Out_Of_State_Fee))
    (record as Record<string, unknown>).Out_Of_State_Fee__c = toBool(raw.Out_Of_State_Fee);
  // setStr("Benefit_Status__c", raw.Benefit_Status); // retired field
  // Total_Courses stays on Contact only for associates (not PE) per mapping doc

  const harassmentDate = isJunkDatePlaceholder(raw.Harassment_Training_Complete)
    ? ""
    : toDate(raw.Harassment_Training_Complete);
  if (harassmentDate) record.Harassment_Training__c = harassmentDate;

  return record;
}

// ── Pre-match helper ───────────────────────────────────────────────────────────

// ── Correction sheet loader ────────────────────────────────────────────────────

async function loadCorrectionSheet(
  spreadsheetId: string,
  accessToken: string,
  correctionFields: string[],
  tabName: string,
): Promise<Map<string, Record<string, string>>> {
  const map = new Map<string, Record<string, string>>();
  const { data } = await axios.get<{ values?: string[][] }>(
    `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(spreadsheetId)}/values/${encodeURIComponent(tabName)}!A:Z`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );
  const rows = data.values ?? [];
  if (rows.length < 2) return map;
  const headers = rows[0].map((h) => h.trim());
  const idIdx = headers.indexOf("ID");
  if (idIdx === -1) return map;
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const id = (row[idIdx] ?? "").trim();
    if (!id) continue;
    const corrections: Record<string, string> = {};
    for (const field of correctionFields) {
      const ci = headers.indexOf(field);
      if (ci === -1) continue;
      const val = (row[ci] ?? "").trim();
      // Country/State/PostalCode are captured even when blank — see
      // ALWAYS_APPLY_FIELDS. Everything else keeps the "blank = no override" rule.
      if (val !== "" || ALWAYS_APPLY_FIELDS.has(field)) {
        corrections[field] = val;
      }
    }
    map.set(id, corrections);
  }
  return map;
}

// ── Google Drive streaming + TSV parsing ───────────────────────────────────────

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  correctionMap: Map<string, Record<string, string>>,
  correctionFields: string[],
  correctionEnabled: boolean,
): Promise<StreamResult> {
  const reqHeaders: Record<string, string> = {
    Authorization: `Bearer ${accessToken}`,
  };
  if (byteOffset > 0) {
    reqHeaders.Range = `bytes=${byteOffset}-`;
  }

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
    const accounts: Partial<SalesforceAssociateAccount>[] = [];
    const personEmployments: Partial<PersonEmploymentRecord>[] = [];
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

        if (accounts.length >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        if (row.length !== parsedHeaders.length) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const record: RawAssociateRecord = {};
        parsedHeaders.forEach((header, i) => {
          record[header] = row[i] ?? "";
        });

        if (!record.ID || !/^[1-9]\d*$/.test(record.ID.trim())) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (correctionEnabled && !correctionMap.has(record.ID.trim())) {
          lastCompletedCursor = rowEndByte;
          return;
        }

        const account = mapToAccount(record);

        const corrEntry = correctionMap.get(record.ID.trim());
        if (corrEntry) {
          const acc = account as Record<string, unknown>;
          // Hong Kong short-circuit — see resolveCountryAndState in utils.ts
          // for why: this org has no top-level "Hong Kong" country.
          if (
            corrEntry.PersonMailingCountry !== undefined &&
            ["HK", "HONG KONG"].includes(
              corrEntry.PersonMailingCountry.trim().toUpperCase(),
            )
          ) {
            acc.PersonMailingCountry = "China";
            acc.PersonMailingState = "Hong Kong";
          } else {
            if (corrEntry.PersonMailingCountry !== undefined) {
              acc.PersonMailingCountry = normalizeCountryName(
                corrEntry.PersonMailingCountry,
              );
            }
            if (corrEntry.PersonMailingState !== undefined) {
              const countryVal = String(acc.PersonMailingCountry ?? "");
              let stateVal =
                countryVal === "United States"
                  ? normalizeStateName(corrEntry.PersonMailingState)
                  : !countryVal
                    ? corrEntry.PersonMailingState
                    : "";
              if (isBlankStatePlaceholder(stateVal)) stateVal = "";
              acc.PersonMailingState = stateVal;
            }
          }
          // Same country-conditional rule as the main flow: blank out only
          // when country resolved to a known non-US value.
          if (corrEntry.PersonMailingPostalCode !== undefined) {
            const countryVal = String(acc.PersonMailingCountry ?? "");
            acc.PersonMailingPostalCode =
              countryVal === "United States" || !countryVal
                ? corrEntry.PersonMailingPostalCode
                : "";
          }
          {
            // Validate regardless of whether the correction sheet supplied an
            // override for this row — otherwise a blank cell here lets the
            // original (possibly invalid) TSV value pass through unchecked.
            // Correction mode only — normal import is unaffected. 7-digit
            // values are zero-padded to 8; any other invalid value is blanked.
            const val =
              corrEntry.University_ID__pc ?? String(acc.University_ID__pc ?? "");
            acc.University_ID__pc = normalizeUniversityId(val);
          }
          if (corrEntry.PersonEmail !== undefined) {
            // cleanEmail() already returns "" for anything that isn't a
            // valid single email address — a malformed sheet value is sent
            // as blank rather than left untouched. Only runs when the sheet
            // cell is non-blank; a blank cell means "no override."
            acc.PersonEmail = cleanEmail(corrEntry.PersonEmail);
          }
          for (const field of correctionFields) {
            if (
              field === "PersonMailingCountry" ||
              field === "PersonMailingState" ||
              field === "PersonMailingPostalCode" ||
              field === "University_ID__pc" ||
              field === "PersonEmail"
            )
              continue;
            if (corrEntry[field] !== undefined) {
              acc[field] = corrEntry[field];
            }
          }
        }

        const extId = record.ID.trim();
        if (!firstId) firstId = extId;
        lastId = extId;
        accounts.push(account);

        if (
          correctionEnabled &&
          correctionMap.size > 0 &&
          accounts.length >= correctionMap.size
        ) {
          aborted = true;
          parser.abort();
        }

        const employment = mapToPersonEmployment(record);
        if (employment) personEmployments.push(employment);

        lastCompletedCursor = rowEndByte;
      },

      complete: () =>
        resolve({
          accounts,
          personEmployments,
          hasMore:
            aborted &&
            !(
              correctionEnabled &&
              correctionMap.size > 0 &&
              accounts.length >= correctionMap.size
            ),
          nextByteOffset: lastCompletedCursor,
          parsedHeaders,
          firstId,
          lastId,
          skippedCount,
          allCorrectionRecordsFound:
            correctionEnabled &&
            correctionMap.size > 0 &&
            accounts.length >= correctionMap.size,
        }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const associateImport = flow({
  name: "Associate Import",
  stableKey: "f6a7b8c9-6666-4f9f-3c4d-ffee11334455",
  description:
    "Streams the associate TSV from Google Drive one window at a time, maps rows to " +
    "Salesforce Person Account records, and upserts via Bulk API 2.0. Pre-matches by " +
    "email + name against existing SF records to avoid duplicates. Recurses until " +
    "the full file has been processed.",

  onTrigger: async (_context, payload) => {
    return { payload };
  },

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    // ── 1. Read cursor from trigger payload ────────────────────────────────────
    const triggerBody = (
      params.onTrigger.results as unknown as
        | { body?: { data?: unknown } }
        | undefined
    )?.body?.data as Record<string, unknown> | undefined;
    const byteOffset =
      typeof triggerBody?.byteOffset === "number" ? triggerBody.byteOffset : 0;
    const knownHeaders = Array.isArray(triggerBody?.headers)
      ? (triggerBody.headers as string[])
      : [];
    const accountSheetId =
      typeof triggerBody?.accountSheetId === "string"
        ? triggerBody.accountSheetId
        : undefined;
    const peSheetId =
      typeof triggerBody?.peSheetId === "string"
        ? triggerBody.peSheetId
        : undefined;

    logger.info(`[Associate Import] Starting at byte offset ${byteOffset}`);

    // ── 2. Resolve connections and file ID ─────────────────────────────────────
    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = configVars["Associate File ID"] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;
    const correctionEnabled =
      String(configVars["Associate Correction Enabled"]) === "true";

    if (!fileId) throw new Error("Associate File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 2b. Load correction sheet if enabled ───────────────────────────────────
    const correctionSheetId = correctionEnabled
      ? String(
          (configVars as Record<string, unknown>)["Correction Sheet ID"] ?? "",
        ).trim()
      : "";

    const correctionMap = new Map<string, Record<string, string>>();
    if (correctionEnabled && correctionSheetId) {
      try {
        const loaded = await loadCorrectionSheet(
          correctionSheetId,
          gdToken,
          CORRECTION_FIELDS,
          CORRECTION_TAB,
        );
        logger.info(
          `[Associate Import] Correction sheet loaded — ${loaded.size} rows, fields: [${CORRECTION_FIELDS.join(", ")}]`,
        );
        for (const [k, v] of loaded) correctionMap.set(k, v);
      } catch (err) {
        const axiosErr = err as {
          response?: { status: number; data: unknown };
        };
        const detail = axiosErr.response
          ? `HTTP ${axiosErr.response.status} – ${JSON.stringify(axiosErr.response.data)}`
          : String(err);
        logger.warn(
          `[Associate Import] Could not load correction sheet: ${detail}`,
        );
      }
    }

    // ── 3. Stream & parse TSV window ───────────────────────────────────────────
    logger.info(
      `[Associate Import] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
    );
    const {
      accounts,
      personEmployments,
      hasMore,
      nextByteOffset,
      parsedHeaders,
      firstId,
      lastId,
      skippedCount,
      allCorrectionRecordsFound,
    } = await streamAndParseTsv(
      fileId,
      gdToken,
      byteOffset,
      MAX_ROWS,
      knownHeaders,
      correctionMap,
      CORRECTION_FIELDS,
      correctionEnabled,
    );
    logger.info(
      `[Associate Import] Parsed ${accounts.length} accounts, ${personEmployments.length} employment records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    // ── 4. Pre-match: query SF by email to find existing Person Accounts ───────
    const emails = accounts
      .map((a) => (a.PersonEmail ?? "").toLowerCase())
      .filter(Boolean);

    const uniqueEmails = [...new Set(emails)];
    logger.info(
      `[Associate Import] Pre-match query for ${uniqueEmails.length} unique emails…`,
    );
    const emailMatchMap = await queryAccountsByEmail(
      sfInstanceUrl,
      sfToken,
      uniqueEmails,
    );
    const totalMatchedAccounts = [...emailMatchMap.values()].reduce(
      (sum, list) => sum + list.length,
      0,
    );
    logger.info(
      `[Associate Import] SF returned ${totalMatchedAccounts} existing account(s) ` +
        `across ${emailMatchMap.size} email(s).`,
    );
    for (const [addr, list] of emailMatchMap) {
      if (list.length > 1) {
        logger.warn(
          `[Associate Import] Email collision: "${addr}" matches ${list.length} ` +
            `different Salesforce accounts (${list.map((r) => `${r.FirstName} ${r.LastName}`).join(", ")}) — ` +
            `disambiguating by name per row.`,
        );
      }
    }

    // ── 5. Split accounts into groups based on pre-match result ───────────────
    // Group A: matched, Student_ID_4D__c exists   → upsert on Student_ID_4D__c
    // Group B: matched, Instructor_ID_4D__c exists → upsert on Instructor_ID_4D__c
    // Group C: no match OR no other typed ID        → upsert on Associate_ID_4D__c
    const groupA: Record<string, unknown>[] = []; // key = Student_ID_4D__c
    const groupB: Record<string, unknown>[] = []; // key = Instructor_ID_4D__c
    const groupC: Record<string, unknown>[] = []; // key = Associate_ID_4D__c

    // Tracks, per Associate_ID_4D__c, the Instructor_ID_4D__c of the person
    // they matched to (if any) — used below to decide whether this
    // associate's PersonEmployment record should be skipped because an
    // Instructor-sourced one already exists for the same person.
    const matchedInstructorIdByAssociateId = new Map<string, string>();

    for (const account of accounts) {
      const email = (account.PersonEmail ?? "").toLowerCase();
      // PersonEmail isn't guaranteed unique — two unrelated people can share
      // one. Search every SF row returned for this email and pick the one
      // whose name actually matches, rather than assuming there's only one.
      const candidates = email ? (emailMatchMap.get(email) ?? []) : [];
      const sfRecord = candidates.find((c) =>
        nameMatches(
          c.FirstName,
          c.LastName,
          account.FirstName ?? "",
          account.LastName ?? "",
        ),
      );

      if (sfRecord) {
        // Record the matched Instructor ID (if any) regardless of which
        // group this account lands in — a person can carry Student_ID_4D__c
        // AND Instructor_ID_4D__c at once, and the PersonEmployment skip
        // check below needs to know about the Instructor side either way.
        if (sfRecord.Instructor_ID_4D__c) {
          matchedInstructorIdByAssociateId.set(
            account.Associate_ID_4D__c as string,
            sfRecord.Instructor_ID_4D__c,
          );
        }

        // Fields that don't exist on the Student/Instructor schema at all —
        // layered on top of the matched record in addition to the new ID.
        // Everything else on that record (name, email, address, notes,
        // alert flag, quarter dates) is left untouched. Per architect spec.
        //
        // Instructor's presence — regardless of whether Student is also
        // present — is what shuts the overlay off entirely: Instructor
        // already owns this same 11-field overlay, so Associate must not
        // apply it a second time (and must not risk overwriting Instructor's
        // values with its own). Associate only contributes the overlay when
        // it finds a Student match with NO Instructor in the picture.
        const hasInstructorMatch = Boolean(sfRecord.Instructor_ID_4D__c);
        const overlay = hasInstructorMatch
          ? {}
          : pickFields(account, MERGE_OVERLAY_FIELDS);

        // University ID backfill (Sep 2026 architect enhancement): Student
        // and Instructor already get first crack at this in their own flows
        // (which run before Associate) — by now sfRecord.University_ID__pc
        // reflects whatever they resolved. Only fill it in from Associate's
        // own data when it's still blank after both; never overwrite an
        // existing value. Applies whether the match is to a Student, an
        // Instructor-only record, or both. Normalized the same way
        // correction mode does (7-digit → 8-digit zero-pad; anything else
        // invalid is dropped rather than copied).
        const normalizedAssociateUniversityId = normalizeUniversityId(
          account.University_ID__pc,
        );
        const universityIdFill =
          !sfRecord.University_ID__pc && normalizedAssociateUniversityId
            ? { University_ID__pc: normalizedAssociateUniversityId }
            : {};

        if (sfRecord.Student_ID_4D__c) {
          // Matched to existing Student — add Associate_ID_4D__c, plus the
          // overlay fields UNLESS an Instructor match also exists (in which
          // case Instructor already owns the overlay — see above).
          groupA.push({
            Student_ID_4D__c: sfRecord.Student_ID_4D__c,
            Student_ID_4D__pc: sfRecord.Student_ID_4D__c,
            Associate_ID_4D__c: account.Associate_ID_4D__c,
            Associate_ID_4D__pc: account.Associate_ID_4D__pc,
            ...overlay,
            ...universityIdFill,
          } as Record<string, unknown>);
        } else if (sfRecord.Instructor_ID_4D__c) {
          // Matched to existing Instructor, no Student — add only
          // Associate_ID_4D__c as a tag; never apply the overlay here,
          // since Instructor already owns those fields. University ID fill
          // still applies independently of the overlay suppression above.
          groupB.push({
            Instructor_ID_4D__c: sfRecord.Instructor_ID_4D__c,
            Instructor_ID_4D__pc: sfRecord.Instructor_ID_4D__c,
            Associate_ID_4D__c: account.Associate_ID_4D__c,
            Associate_ID_4D__pc: account.Associate_ID_4D__pc,
            ...universityIdFill,
          } as Record<string, unknown>);
        } else {
          // Matched to existing associate OR no typed ID yet — this is the
          // associate's own record, so the full mapped associate record is
          // correct here.
          groupC.push({ ...account } as Record<string, unknown>);
        }
      } else {
        groupC.push({ ...account } as Record<string, unknown>);
      }
    }

    logger.info(
      `[Associate Import] Split: groupA(matchedToStudent)=${groupA.length}, ` +
        `groupB(matchedToInstructor)=${groupB.length}, groupC(associateOrNew)=${groupC.length}`,
    );

    // ── 6. Upsert to Salesforce — three bulk jobs ──────────────────────────────
    let nextAccountSheetId = accountSheetId;
    const failedAssociateIds = new Set<string>();

    const runGroupJob = async (
      group: Record<string, unknown>[],
      upsertKey: string,
      label: string,
      allKeys: string[],
    ) => {
      if (group.length === 0) return;
      const jobResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "Account",
        upsertKey,
        group,
        logger,
        `[Associate Import][${label}]`,
      );
      // Track failed Associate_ID_4D__c values for PE filtering
      for (const rec of group) {
        const assocId = rec.Associate_ID_4D__c as string | undefined;
        if (assocId) {
          const keyVal = rec[upsertKey] as string | undefined;
          if (keyVal && jobResult.failedExternalIds.has(keyVal)) {
            failedAssociateIds.add(assocId);
          }
        }
      }
      // Rebuild records in the same fixed key order so all groups share the
      // same column structure when appended to the results sheet.
      const orderedGroup = group.map((r) => {
        const ordered: Record<string, unknown> = {};
        for (const key of allKeys) {
          ordered[key] = r[key] ?? "";
        }
        return ordered;
      });
      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Associate Import",
          objectName: "Account",
          contacts: orderedGroup,
          externalIdField: upsertKey,
          failedExternalIds: jobResult.failedExternalIds,
          successfulCsv: jobResult.successfulCsv,
          failedCsv: jobResult.failedCsv,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: nextAccountSheetId,
        });
        nextAccountSheetId = sheet.spreadsheetId;
        logger.info(`[Associate Import][${label}] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(
          `[Associate Import][${label}] Could not update results sheet: ${String(err)}`,
        );
      }
    };

    if (accounts.length > 0) {
      const allGroupKeys = Array.from(
        new Set(
          [...groupA, ...groupB, ...groupC].flatMap((r) => Object.keys(r)),
        ),
      );
      await runGroupJob(
        groupA,
        "Student_ID_4D__c",
        "MatchedToStudent",
        allGroupKeys,
      );
      await runGroupJob(
        groupB,
        "Instructor_ID_4D__c",
        "MatchedToInstructor",
        allGroupKeys,
      );
      await runGroupJob(
        groupC,
        "Associate_ID_4D__c",
        "AssociateOrNew",
        allGroupKeys,
      );
    } else {
      logger.info(
        "[Associate Import] Window contained no records; skipping upload.",
      );
    }

    // ── 7. Resolve SF Account IDs, then upsert PersonEmployment ──────────────
    if (personEmployments.length > 0) {
      const successfulEmployments = personEmployments.filter((e) => {
        const assocId = e.External_ID_4D__c?.replace(/^associate_/, "") ?? "";
        return !failedAssociateIds.has(assocId);
      });

      if (successfulEmployments.length < personEmployments.length) {
        logger.warn(
          `[Associate Import] Skipping PersonEmployment for ${personEmployments.length - successfulEmployments.length} record(s) whose Account upsert failed.`,
        );
      }

      if (successfulEmployments.length === 0) {
        logger.warn(
          "[Associate Import] All accounts failed upsert — skipping PersonEmployment entirely.",
        );
      } else {
        // ── 7a. THE gate for Associate PersonEmployment (Salary_Category is no
        // longer checked — see mapToPersonEmployment above): skip creating one
        // wherever this same person already has an Instructor-sourced
        // PersonEmployment record; otherwise create it regardless of what
        // this associate row's own fields look like.
        const candidateInstructorIds = [
          ...new Set(
            successfulEmployments
              .map((e) => {
                const assocId =
                  e.External_ID_4D__c?.replace(/^associate_/, "") ?? "";
                return matchedInstructorIdByAssociateId.get(assocId);
              })
              .filter((v): v is string => Boolean(v)),
          ),
        ];

        let employmentsToLoad = successfulEmployments;
        if (candidateInstructorIds.length > 0) {
          const instructorPeExtIds = candidateInstructorIds.map(
            (id) => `instructor_${id}`,
          );
          const existingInstructorPe = await queryExistingExternalIds(
            sfInstanceUrl,
            sfToken,
            "PersonEmployment",
            "External_ID_4D__c",
            instructorPeExtIds,
          );
          logger.info(
            `[Associate Import] ${existingInstructorPe.size} of ${instructorPeExtIds.length} matched-Instructor PersonEmployment record(s) already exist.`,
          );

          employmentsToLoad = successfulEmployments.filter((e) => {
            const assocId =
              e.External_ID_4D__c?.replace(/^associate_/, "") ?? "";
            const instrId = matchedInstructorIdByAssociateId.get(assocId);
            if (!instrId) return true; // no matched instructor — keep
            return !existingInstructorPe.has(`instructor_${instrId}`);
          });

          const skippedForInstructor =
            successfulEmployments.length - employmentsToLoad.length;
          if (skippedForInstructor > 0) {
            logger.info(
              `[Associate Import] Skipped ${skippedForInstructor} Associate PersonEmployment record(s) — an Instructor PersonEmployment already exists for the same person.`,
            );
          }
        }

        if (employmentsToLoad.length === 0) {
          logger.info(
            "[Associate Import] No Associate PersonEmployment records remain after Instructor-dedup check — skipping upsert.",
          );
        } else {
          const assocIds = employmentsToLoad
            .map((e) => e.External_ID_4D__c?.replace(/^associate_/, "") ?? "")
            .filter(Boolean);

          logger.info(
            `[Associate Import] Resolving SF Account IDs for ${assocIds.length} associate IDs…`,
          );
          const accountIdMap = await resolveAccountIdsByField(
            sfInstanceUrl,
            sfToken,
            "Associate_ID_4D__c",
            assocIds,
          );
          logger.info(
            `[Associate Import] Resolved ${accountIdMap.size} Account IDs.`,
          );

          const employmentsWithIds = employmentsToLoad.map((e) => {
            const assocId =
              e.External_ID_4D__c?.replace(/^associate_/, "") ?? "";
            const sfId = accountIdMap.get(assocId);
            return { ...e, RelatedPersonId: sfId, AccountId: sfId };
          });

          const empJobResult = await runBulkJob(
            sfInstanceUrl,
            sfToken,
            "PersonEmployment",
            "External_ID_4D__c",
            employmentsWithIds as Record<string, unknown>[],
            logger,
            "[Associate Import][PersonEmployment]",
          );

          try {
            const sheet = await createResultsSheetFromContacts({
              flowName: "Associate Import",
              objectName: "PersonEmployment",
              contacts: employmentsWithIds as Record<string, unknown>[],
              externalIdField: "External_ID_4D__c",
              failedExternalIds: empJobResult.failedExternalIds,
              successfulCsv: empJobResult.successfulCsv,
              failedCsv: empJobResult.failedCsv,
              accessToken: gdToken,
              folderId: failedFolderId,
              spreadsheetId: peSheetId,
            });
            nextAccountSheetId = sheet.spreadsheetId;
            logger.info(
              `[Associate Import] PersonEmployment results sheet: ${sheet.url}`,
            );
          } catch (err: unknown) {
            logger.warn(
              `[Associate Import] Could not update PersonEmployment results sheet: ${String(err)}`,
            );
          }
        }
      }
    } else {
      logger.info(
        "[Associate Import] No employment records in window; skipping PersonEmployment upsert.",
      );
    }

    // ── 8. Recurse if more rows remain ─────────────────────────────────────────
    if (allCorrectionRecordsFound) {
      logger.info(
        `[Associate Import] All ${correctionMap.size} correction records found — stopping early.`,
      );
    }
    if (hasMore) {
      logger.info(
        `[Associate Import] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );

      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Associate Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        accountSheetId: nextAccountSheetId,
        peSheetId,
      });
    } else {
      logger.info("[Associate Import] All rows processed — import complete.");
    }

    return {
      data: {
        byteOffset,
        accountsProcessed: accounts.length,
        employmentsProcessed: personEmployments.length,
        hasMore,
      },
    };
  },
});

export default [associateImport];
