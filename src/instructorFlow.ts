/**
 * Stanford CSP Migration – Instructor Import flow.
 *
 * Streams an instructor TSV from Google Drive in MAX_ROWS-row windows using HTTP
 * Range headers (cursor = byteOffset). Each execution produces two sets of records:
 *   1. Person Account fields  → upserted to Account object
 *   2. Person Employment fields → upserted to PersonEmployment object,
 *      linked to the Person Account via RelatedPersonId resolved after Account upsert.
 *
 * Pre-match logic:
 *   Before upserting, queries SF by email to find existing Person Accounts.
 *   Match = email match AND (name forward OR name swapped).
 *   If the matched record already has Student_ID_4D__c, this instructor's
 *   data is layered on as a fixed overlay field set (see MERGE_OVERLAY_FIELDS)
 *   plus Instructor_ID_4D__c, leaving the rest of the Student record intact.
 *   Otherwise the full instructor record is upserted on Instructor_ID_4D__c
 *   (covers: no match, matched to an existing instructor-only record, or —
 *   under the current flow execution order Student → Instructor → Associate —
 *   matched to an Associate record, which can't yet exist when this flow
 *   runs). Instructor always takes priority over Associate regardless of
 *   Student presence, per architect spec — Associate is never checked here.
 *
 * Recurses via context.invokeFlow until the full file is processed.
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
  nameMatches,
  pickFields,
  resolveCountryAndState,
  isBlankStatePlaceholder,
} from "./utils";
import {
  createResultsSheetFromContacts,
  appendSkippedRecords,
} from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 3000; // enough to cover the full ~2-3k instructor file in one window

// Set to true to overlay corrected values from the correction sheet onto TSV records.
// Set back to false for normal runs.
// Salesforce API field names to override from the correction sheet when correction mode is on.
const CORRECTION_FIELDS: string[] = [
  "PersonMailingStreet",
  "PersonMailingCity",
  "PersonMailingState",
  "PersonMailingPostalCode",
  "PersonMailingCountry",
  "SUNet_Id__pc",
  "University_ID__pc",
];

// These fields are authoritative from the correction sheet even when the cell
// is blank — every record on the sheet was flagged for a problem with it, so a
// blank Country/State/PostalCode/SUNet ID means "clear this," not "no
// correction, use the original file's value." Falling back to the original
// value here just re-introduces the same bad data the correction was meant to
// fix. Street/City keep the opposite rule: a blank cell means "no override."
const ALWAYS_APPLY_FIELDS = new Set([
  "PersonMailingCountry",
  "PersonMailingState",
  "PersonMailingPostalCode",
  "SUNet_Id__pc",
]);

const CORRECTION_TAB = "Instructor";

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

// ── Raw TSV record ─────────────────────────────────────────────────────────────

interface RawInstructorRecord {
  ID?: string;
  Last_Name?: string;
  First_Name?: string;
  Middle_Name?: string;
  Name_Suffix?: string;
  Title?: string;
  Department?: string;
  Bio?: string;
  Notes?: string;
  SUNet_ID?: string;
  Email_Address?: string;
  Username?: string;
  Active?: string;
  Do_Not_Contact?: string;
  Home_Phone?: string;
  Work_Phone?: string;
  Mobile_Phone?: string;
  Address_Line1?: string;
  Address_Line2?: string;
  City?: string;
  State?: string;
  Zip?: string;
  Country?: string;
  Student_ID?: string;
  Stanford_Mail_Code?: string;
  // Benefit_Status?: string; // retired field
  Out_Of_State_Fee?: string;
  Alert_Flag?: string;
  University_ID?: string;
  Legal_Name?: string;
  First_Quarter?: string;
  Most_Recent_Quarter?: string;
  AC_Pay?: string;
  Email_Address_For_Students?: string;
  Harassment_Training_Complete?: string;
  Total_Courses?: string;
  Coordinator_Status?: string;
  Salary_Category?: string;
  // Online_Teaching_Training?: string; // not in mapping doc — unmapped for now
  // Zoom_Training?: string;            // not in mapping doc — unmapped for now
  // Canvas_Training?: string;          // not in mapping doc — unmapped for now
  [key: string]: string | undefined;
}

// ── Salesforce Person Account shape ───────────────────────────────────────────

interface SalesforceInstructorAccount {
  Instructor_ID_4D__c: string;
  Instructor_ID_4D__pc: string;
  LastName: string;
  FirstName?: string;
  MiddleName?: string;
  Suffix?: string;
  PersonTitle?: string;
  PersonDepartment?: string;
  Description?: string;
  SUNet_Id__pc?: string;
  PersonEmail?: string;
  Username_4D__pc?: string;
  Active__pc?: boolean | string;
  Do_Not_Contact__pc?: boolean | string;
  PersonHomePhone?: string;
  Phone?: string;
  PersonMobilePhone?: string;
  PersonMailingStreet?: string;
  PersonMailingCity?: string;
  PersonMailingState?: string;
  PersonMailingPostalCode?: string;
  PersonMailingCountry?: string;
  // Cross-reference only (per mapping doc) — the instructor's OWN "if they
  // also have a separate Student record" note from the source TSV. This
  // upserts to Account (Person Account), where a Contact custom field
  // Student_ID__c mirrors as Student_ID__pc — deliberately a different name
  // from Student_ID_4D__c/__pc (the real Student upsert key used when
  // merging into an existing matched Student record below), so the two can
  // never collide when both are set on the same payload.
  Student_ID__pc?: number | string; // Number field in Salesforce
  Stanford_Mail_Code__pc?: string;
  // Out_Of_State_Fee__pc?: boolean | string; // moved to PersonEmployment
  Alert_Flag__pc?: boolean | string;
  University_ID__pc?: string;
  // Legal_Name__pc?: string; // moved to PersonEmployment
  First_Quarter__pc?: string;
  Most_Recent_Quarter__pc?: string;
}

// Fields that don't exist on the Student schema at all — safe to layer on top
// of an existing Student record without touching anything that record
// already owns. Per architect spec (Aug 2026 enhancement).
const MERGE_OVERLAY_FIELDS: (keyof SalesforceInstructorAccount)[] = [
  "PersonTitle",
  "PersonDepartment",
  "Active__pc",
  "Do_Not_Contact__pc",
  "PersonHomePhone",
  "Phone",
  "PersonMobilePhone",
  // "Student_ID__pc", // Per architect (Aug 2026): when a matching Student is
  // found by name+email, do NOT overlay this instructor row's own
  // self-reported Student ID onto that matched record — leave it untouched.
  // Only set it on the "no match found" / brand-new-record path, which
  // already happens via the normal full field mapping outside this list.
  "Suffix",
  "SUNet_Id__pc",
  "Username_4D__pc",
];

// ── PersonEmployment shape ─────────────────────────────────────────────────────

interface PersonEmploymentRecord {
  External_ID_4D__c: string;
  Name: string;
  RelatedPersonId?: string;
  AccountId?: string;
  Instructor_Bio__c?: string;
  Legal_Name__c?: string;
  Out_Of_State_Fee__c?: boolean;
  // Benefit_Status__c?: string; // retired field
  AC_Pay__c?: boolean;
  Email_for_Students__c?: string;
  Harassment_Training__c?: string;
  Total_Courses__c?: number;
  Coordinator_Status__c?: string;
  Salary_Category__c?: string;
  // Online_Teaching_Training__c?: string; // not in mapping doc — unmapped for now
  // Zoom_Training__c?: string;            // not in mapping doc — unmapped for now
  // Canvas_Training__c?: string;          // not in mapping doc — unmapped for now
}

// ── Stream result ──────────────────────────────────────────────────────────────

/** A source row dropped before it ever became an account — never reaches Salesforce,
 * so it would otherwise disappear with no success/failure trace. See "Skipped" tab. */
interface SkippedRow {
  ID: string;
  Reason: string;
  Raw_Row: string;
}

interface InstructorStreamResult {
  accounts: Partial<SalesforceInstructorAccount>[];
  employments: Partial<PersonEmploymentRecord>[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
  skippedRows: SkippedRow[];
  allCorrectionRecordsFound: boolean;
}

// ── Field mapping — Account ────────────────────────────────────────────────────

function mapInstructorToAccount(
  raw: RawInstructorRecord,
): Partial<SalesforceInstructorAccount> {
  const id = str(raw.ID).trim();
  const record: Partial<SalesforceInstructorAccount> = {
    Instructor_ID_4D__c: id,
    Instructor_ID_4D__pc: id,
    LastName: str(raw.Last_Name),
  };

  const setStr = (
    key: keyof SalesforceInstructorAccount,
    v: string | undefined,
  ) => {
    (record as Record<string, unknown>)[key] = str(v);
  };

  const setBool = (
    key: keyof SalesforceInstructorAccount,
    v: string | undefined,
  ) => {
    (record as Record<string, unknown>)[key] = hasValue(v) ? toBool(v) : "";
  };

  setStr("FirstName", raw.First_Name);
  setStr("MiddleName", raw.Middle_Name);
  setStr("Suffix", raw.Name_Suffix);
  const title = str(raw.Title).slice(0, 80);
  if (title) record.PersonTitle = title;
  setStr("PersonDepartment", raw.Department);

  record.Description = str(raw.Notes)
    .replace(/_4DNL_/g, "\n")
    .trim();

  setStr("SUNet_Id__pc", raw.SUNet_ID);
  record.PersonEmail = cleanEmail(raw.Email_Address) || "";
  setStr("Username_4D__pc", raw.Username);
  setBool("Active__pc", raw.Active);
  setBool("Do_Not_Contact__pc", raw.Do_Not_Contact);

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
  // Same rule as State above: blank out only when country resolved to a
  // known non-US value. If country is blank we can't tell whether the
  // address is US or not, so keep the zip as-is.
  record.PersonMailingPostalCode =
    record.PersonMailingCountry === "United States" ||
    !record.PersonMailingCountry
      ? str(raw.Zip)
      : "";

  // Student_ID__pc (Account-side mirror of Contact field Student_ID__c) is
  // a Number field — keep the positive-integer sanity check (rejects
  // blank/junk source values) and emit a number.
  const studentId = parseInt(str(raw.Student_ID), 10);
  (record as Record<string, unknown>).Student_ID__pc =
    !isNaN(studentId) && studentId > 0 ? studentId : "";
  // setBool("Out_Of_State_Fee__pc", raw.Out_Of_State_Fee); // moved to PersonEmployment
  setBool("Alert_Flag__pc", raw.Alert_Flag);
  setStr("University_ID__pc", raw.University_ID);
  // setStr("Legal_Name__pc", raw.Legal_Name); // moved to PersonEmployment
  setStr("First_Quarter__pc", raw.First_Quarter);
  setStr("Most_Recent_Quarter__pc", raw.Most_Recent_Quarter);

  return record;
}

// Per mapping doc: Harassment_Training_Complete contains junk placeholder
// dates ("00/00/00", "0000-00-00", etc.) that must be filtered to null
// instead of being parsed/passed through as a literal date string.
function isJunkDatePlaceholder(v: string | undefined): boolean {
  return /^0+([./-]0+){2}$/.test(str(v));
}

// ── Field mapping — PersonEmployment ──────────────────────────────────────────

function mapToPersonEmployment(
  raw: RawInstructorRecord,
): Partial<PersonEmploymentRecord> | null {
  const id = str(raw.ID);
  if (!id) return null;

  const hasEmploymentData =
    hasValue(raw.Bio) ||
    // hasValue(raw.Benefit_Status) || // retired field
    hasValue(raw.AC_Pay) ||
    hasValue(raw.Email_Address_For_Students) ||
    hasValue(raw.Harassment_Training_Complete) ||
    hasValue(raw.Total_Courses) ||
    hasValue(raw.Coordinator_Status) ||
    hasValue(raw.Salary_Category) ||
    hasValue(raw.Out_Of_State_Fee) ||
    hasValue(raw.Legal_Name);

  if (!hasEmploymentData) return null;

  const name =
    [str(raw.First_Name), str(raw.Last_Name)].filter(Boolean).join(" ") || id;

  const record: Partial<PersonEmploymentRecord> = {
    External_ID_4D__c: `instructor_${id}`,
    Name: name,
  };

  const setStr = (key: keyof PersonEmploymentRecord, v: string | undefined) => {
    const val = str(v);
    if (val) (record as Record<string, unknown>)[key] = val;
  };

  const setBool = (
    key: keyof PersonEmploymentRecord,
    v: string | undefined,
  ) => {
    if (hasValue(v)) (record as Record<string, unknown>)[key] = toBool(v);
  };

  // Restricted picklist fields: always set the key, using "#N/A" (Bulk API
  // null marker) when blank so a stale value on the existing record actually
  // gets cleared on re-import — unlike setStr above, which omits the key
  // entirely on blank and would leave old data in place.
  const setPicklist = (
    key: keyof PersonEmploymentRecord,
    v: string | undefined,
  ) => {
    const val = str(v);
    (record as Record<string, unknown>)[key] = val || "#N/A";
  };

  const bio = str(raw.Bio)
    .replace(/_4DNL_/g, "\n")
    .trim();
  if (bio) record.Instructor_Bio__c = bio;
  setStr("Legal_Name__c", raw.Legal_Name);
  setBool("Out_Of_State_Fee__c", raw.Out_Of_State_Fee);
  // setStr("Benefit_Status__c", raw.Benefit_Status); // retired field
  setBool("AC_Pay__c", raw.AC_Pay);
  // Salesforce Email field type — run through the same cleanup as the
  // primary email (handles "Name: email" format, trailing periods,
  // multiple comma-separated addresses, malformed values → blank).
  const emailForStudents = cleanEmail(raw.Email_Address_For_Students);
  if (emailForStudents) record.Email_for_Students__c = emailForStudents;
  const harassmentDate = isJunkDatePlaceholder(raw.Harassment_Training_Complete)
    ? ""
    : toDate(raw.Harassment_Training_Complete);
  if (harassmentDate) record.Harassment_Training__c = harassmentDate;
  const totalCourses = parseInt(str(raw.Total_Courses), 10);
  if (!isNaN(totalCourses)) record.Total_Courses__c = totalCourses;
  setPicklist("Coordinator_Status__c", raw.Coordinator_Status);
  // Restricted picklist — source values already match the picklist verbatim
  // (Cont, RBE, journal transfer, emeritus prof, instructor vendor) except
  // for two known mismatches: the "Contigent"/"Contingent" typo (source
  // doesn't match any valid value) and "volunteer" (source is lowercase,
  // picklist stores "Volunteer" with a capital V).
  const salaryCategoryRaw = str(raw.Salary_Category);
  const salaryCategory = /^Contin?gent$/i.test(salaryCategoryRaw)
    ? "Cont"
    : /^volunteer$/i.test(salaryCategoryRaw)
      ? "Volunteer"
      : salaryCategoryRaw;
  if (salaryCategory) record.Salary_Category__c = salaryCategory;

  return record;
}

// ── Google Drive streaming + TSV parsing ───────────────────────────────────────

async function streamAndParseInstructorTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  correctionMap: Map<string, Record<string, string>>,
  correctionFields: string[],
  correctionEnabled: boolean,
): Promise<InstructorStreamResult> {
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
    const accounts: Partial<SalesforceInstructorAccount>[] = [];
    const employments: Partial<PersonEmploymentRecord>[] = [];
    let aborted = false;
    let firstId = "";
    let lastId = "";
    let skippedCount = 0;
    const skippedRows: SkippedRow[] = [];
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
          skippedRows.push({
            ID: "",
            Reason: `Column count mismatch: row has ${row.length} field(s), expected ${parsedHeaders.length}`,
            Raw_Row: row.join(" | "),
          });
          lastCompletedCursor = rowEndByte;
          return;
        }

        const record: RawInstructorRecord = {};
        parsedHeaders.forEach((header, i) => {
          record[header] = row[i] ?? "";
        });

        if (!record.ID || !/^[1-9]\d*$/.test(record.ID.trim())) {
          skippedCount++;
          skippedRows.push({
            ID: str(record.ID),
            Reason: "Missing or invalid ID (must be a positive integer)",
            Raw_Row: row.join(" | "),
          });
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (correctionEnabled && !correctionMap.has(record.ID.trim())) {
          lastCompletedCursor = rowEndByte;
          return;
        }

        const account = mapInstructorToAccount(record);

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
          // SUNet_Id__pc: unique field, no format validation, but the sheet is
          // authoritative even when blank (ALWAYS_APPLY_FIELDS) — a blank cell
          // is captured by loadCorrectionSheet, so this loop applies it as an
          // explicit blank rather than skipping and keeping the original TSV
          // value.
          for (const field of correctionFields) {
            if (
              field === "PersonMailingCountry" ||
              field === "PersonMailingState" ||
              field === "PersonMailingPostalCode" ||
              field === "University_ID__pc"
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

        const employment = mapToPersonEmployment(record);
        if (employment) employments.push(employment);

        lastCompletedCursor = rowEndByte;

        if (
          correctionEnabled &&
          correctionMap.size > 0 &&
          accounts.length >= correctionMap.size
        ) {
          aborted = true;
          parser.abort();
        }
      },

      complete: () =>
        resolve({
          accounts,
          employments,
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
          skippedRows,
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

export const instructorImport = flow({
  name: "Instructor Import",
  stableKey: "f7a8b9c0-6666-4f0a-3b4c-ff0011223344",
  description:
    "Streams the instructor TSV from Google Drive one window at a time, maps rows to " +
    "Salesforce Person Account and PersonEmployment records, and upserts both via " +
    "Bulk API 2.0. Pre-matches by email + name against existing SF records to avoid " +
    "duplicates. Recurses until the full file has been processed.",

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
    const empSheetId =
      typeof triggerBody?.empSheetId === "string"
        ? triggerBody.empSheetId
        : undefined;

    logger.info(`[Instructor Import] Starting at byte offset ${byteOffset}`);

    // ── 2. Resolve connections and file ID ─────────────────────────────────────
    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = configVars["Instructor File ID"] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;
    const correctionEnabled =
      String(configVars["Instructor Correction Enabled"]) === "true";

    if (!fileId) throw new Error("Instructor File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 2a. Correction sheet config ────────────────────────────────────────────
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
          `[Instructor Import] Correction sheet loaded — ${loaded.size} rows, fields: [${CORRECTION_FIELDS.join(", ")}]`,
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
          `[Instructor Import] Could not load correction sheet: ${detail}`,
        );
      }
    }

    // ── 3. Stream & parse the TSV window from Google Drive ─────────────────────
    logger.info(
      `[Instructor Import] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
    );
    const {
      accounts,
      employments,
      hasMore,
      nextByteOffset,
      parsedHeaders,
      firstId,
      lastId,
      skippedCount,
      skippedRows,
      allCorrectionRecordsFound,
    } = await streamAndParseInstructorTsv(
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
      `[Instructor Import] Parsed ${accounts.length} accounts, ${employments.length} employment records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    // ── 4. Pre-match: query SF by email to find existing Person Accounts ───────
    const emails = accounts
      .map((a) => (a.PersonEmail ?? "").toLowerCase())
      .filter(Boolean);

    const uniqueEmails = [...new Set(emails)];
    logger.info(
      `[Instructor Import] Pre-match query for ${uniqueEmails.length} unique emails…`,
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
      `[Instructor Import] SF returned ${totalMatchedAccounts} existing account(s) ` +
        `across ${emailMatchMap.size} email(s).`,
    );
    for (const [addr, list] of emailMatchMap) {
      if (list.length > 1) {
        logger.warn(
          `[Instructor Import] Email collision: "${addr}" matches ${list.length} ` +
            `different Salesforce accounts (${list.map((r) => `${r.FirstName} ${r.LastName}`).join(", ")}) — ` +
            `disambiguating by name per row.`,
        );
      }
    }

    // ── 5. Split accounts into groups based on pre-match result ───────────────
    // Group A: matched to existing Student → upsert on Student_ID_4D__c
    // Group C: no Student match (new, matched to existing Instructor, or —
    //   under the current flow execution order Student → Instructor →
    //   Associate — matched to an existing Associate, which can't yet exist
    //   when Instructor runs) → upsert on Instructor_ID_4D__c, full record.
    // Associate is deliberately never checked here: Instructor always wins
    // over Associate regardless of Student presence (per architect spec),
    // and Instructor runs before Associate, so there is nothing to defer to.
    const groupA: Record<string, unknown>[] = []; // key = Student_ID_4D__c
    const groupC: Record<string, unknown>[] = []; // key = Instructor_ID_4D__c

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
        if (sfRecord.Student_ID_4D__c) {
          // Matched to existing Student — add Instructor_ID_4D__c plus the
          // fixed overlay fields to that record; do not overwrite anything
          // else. Per architect spec.
          const overlay = pickFields(account, MERGE_OVERLAY_FIELDS);
          // University ID backfill (Sep 2026 architect enhancement): only
          // fill it in when the Student side doesn't already have one —
          // never overwrite an existing value with the instructor's.
          // Normalized the same way correction mode does (7-digit → 8-digit
          // zero-pad; anything else invalid is dropped rather than copied).
          const normalizedInstructorUniversityId = normalizeUniversityId(
            account.University_ID__pc,
          );
          const universityIdFill =
            !sfRecord.University_ID__pc && normalizedInstructorUniversityId
              ? { University_ID__pc: normalizedInstructorUniversityId }
              : {};
          groupA.push({
            Student_ID_4D__c: sfRecord.Student_ID_4D__c,
            Student_ID_4D__pc: sfRecord.Student_ID_4D__c,
            Instructor_ID_4D__c: account.Instructor_ID_4D__c,
            Instructor_ID_4D__pc: account.Instructor_ID_4D__pc,
            ...overlay,
            ...universityIdFill,
          } as Record<string, unknown>);
        } else {
          // No Student match — this is the instructor's own whole record
          // (new person, matched to an existing instructor-only record, or
          // any other match with no Student on it).
          groupC.push({ ...account } as Record<string, unknown>);
        }
      } else {
        // No match — new record, upsert on Instructor_ID_4D__c
        groupC.push({ ...account } as Record<string, unknown>);
      }
    }

    logger.info(
      `[Instructor Import] Split: groupA(matchedToStudent)=${groupA.length}, ` +
        `groupC(instructorOrNew)=${groupC.length}`,
    );

    // ── 6. Upsert to Salesforce — up to two bulk jobs ─────────────────────────
    let nextAccountSheetId = accountSheetId;
    const failedInstructorIds = new Set<string>();

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
        `[Instructor Import][${label}]`,
      );
      // Track failed Instructor_ID_4D__c values for PE filtering
      for (const rec of group) {
        const instrId = rec.Instructor_ID_4D__c as string | undefined;
        if (instrId) {
          const keyVal = rec[upsertKey] as string | undefined;
          if (keyVal && jobResult.failedExternalIds.has(keyVal)) {
            failedInstructorIds.add(instrId);
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
          flowName: "Instructor Import",
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
        logger.info(
          `[Instructor Import][${label}] Results sheet: ${sheet.url}`,
        );
      } catch (err: unknown) {
        logger.warn(
          `[Instructor Import][${label}] Could not update results sheet: ${String(err)}`,
        );
      }
    };

    if (accounts.length > 0) {
      const allGroupKeys = Array.from(
        new Set([...groupA, ...groupC].flatMap((r) => Object.keys(r))),
      );
      await runGroupJob(
        groupA,
        "Student_ID_4D__c",
        "MatchedToStudent",
        allGroupKeys,
      );
      await runGroupJob(
        groupC,
        "Instructor_ID_4D__c",
        "InstructorOrNew",
        allGroupKeys,
      );
    } else {
      logger.info(
        "[Instructor Import] No account records in window; skipping Account upsert.",
      );
    }

    // Rows dropped during parsing (bad/missing ID, column mismatch) never became
    // an account, so they never appear in the Success/Errors/Not Processed tabs
    // above — write them to their own tab so nothing disappears silently.
    if (skippedRows.length > 0) {
      if (nextAccountSheetId) {
        try {
          await appendSkippedRecords(
            nextAccountSheetId,
            gdToken,
            ["ID", "Reason", "Raw_Row"],
            skippedRows.map((r) => [r.ID, r.Reason, r.Raw_Row]),
          );
          logger.warn(
            `[Instructor Import] ${skippedRows.length} row(s) skipped during parsing — see "Skipped" tab.`,
          );
        } catch (err: unknown) {
          logger.warn(
            `[Instructor Import] Could not write Skipped tab: ${String(err)}`,
          );
        }
      } else {
        logger.warn(
          `[Instructor Import] ${skippedRows.length} row(s) skipped during parsing, but no ` +
            `results spreadsheet exists yet this window to attach a Skipped tab to: ` +
            JSON.stringify(skippedRows),
        );
      }
    }

    // ── 7. Resolve SF Account IDs, then upsert PersonEmployment ───────────────
    if (employments.length > 0) {
      const successfulEmployments = employments.filter((e) => {
        const instrId = e.External_ID_4D__c?.replace(/^instructor_/, "") ?? "";
        return !failedInstructorIds.has(instrId);
      });

      if (successfulEmployments.length < employments.length) {
        logger.warn(
          `[Instructor Import] Skipping PersonEmployment for ${
            employments.length - successfulEmployments.length
          } record(s) whose Account upsert failed.`,
        );
      }

      if (successfulEmployments.length === 0) {
        logger.warn(
          "[Instructor Import] All accounts failed upsert — skipping PersonEmployment entirely.",
        );
      } else {
        const instrIds = successfulEmployments
          .map((e) => e.External_ID_4D__c?.replace(/^instructor_/, "") ?? "")
          .filter(Boolean);

        logger.info(
          `[Instructor Import] Resolving SF Account IDs for ${instrIds.length} instructor IDs…`,
        );
        const accountIdMap = await resolveAccountIdsByField(
          sfInstanceUrl,
          sfToken,
          "Instructor_ID_4D__c",
          instrIds,
        );
        logger.info(
          `[Instructor Import] Resolved ${accountIdMap.size} Account IDs.`,
        );

        const employmentsWithIds = successfulEmployments.map((e) => {
          const instrId =
            e.External_ID_4D__c?.replace(/^instructor_/, "") ?? "";
          const sfId = accountIdMap.get(instrId);
          return { ...e, RelatedPersonId: sfId, AccountId: sfId };
        });

        const empJobResult = await runBulkJob(
          sfInstanceUrl,
          sfToken,
          "PersonEmployment",
          "External_ID_4D__c",
          employmentsWithIds as Record<string, unknown>[],
          logger,
          "[Instructor Import][PersonEmployment]",
        );

        try {
          const sheet = await createResultsSheetFromContacts({
            flowName: "Instructor Import",
            objectName: "PersonEmployment",
            contacts: employmentsWithIds as Record<string, unknown>[],
            externalIdField: "External_ID_4D__c",
            failedExternalIds: empJobResult.failedExternalIds,
            successfulCsv: empJobResult.successfulCsv,
            failedCsv: empJobResult.failedCsv,
            accessToken: gdToken,
            folderId: failedFolderId,
            spreadsheetId: empSheetId,
          });
          nextAccountSheetId = sheet.spreadsheetId;
          logger.info(
            `[Instructor Import] PersonEmployment results sheet: ${sheet.url}`,
          );
        } catch (err: unknown) {
          logger.warn(
            `[Instructor Import] Could not update PersonEmployment results sheet: ${String(err)}`,
          );
        }
      }
    } else {
      logger.info(
        "[Instructor Import] No employment records in window; skipping PersonEmployment upsert.",
      );
    }

    // ── 8. Recurse if more rows remain ─────────────────────────────────────────
    if (allCorrectionRecordsFound) {
      logger.info(
        `[Instructor Import] All ${correctionMap.size} correction records found — stopping early.`,
      );
    }
    if (hasMore) {
      logger.info(
        `[Instructor Import] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );

      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Instructor Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        accountSheetId: nextAccountSheetId,
        empSheetId,
      });
    } else {
      logger.info("[Instructor Import] All rows processed — import complete.");
    }

    return {
      data: {
        byteOffset,
        accountsProcessed: accounts.length,
        employmentsProcessed: employments.length,
        hasMore,
      },
    };
  },
});

export default [instructorImport];
