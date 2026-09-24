/**
 * Stanford CSP Migration – Student Import flow.
 *
 * Streams a large TSV from Google Drive in MAX_ROWS-row windows using HTTP Range
 * headers (cursor = byteOffset).  Each execution downloads only the bytes it needs,
 * maps the window to Salesforce Person Account records, and bulk-upserts via Bulk
 * API 2.0.  When more rows remain the flow invokes itself recursively via
 * context.invokeFlow, advancing the byte cursor to where the previous window ended.
 */

import { flow } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import {
  str,
  hasValue,
  toBool,
  // toYesNo, // unused — US_Citizen, Stanford_Alumnus excluded per architect
  toDate,
  normalizeCountryName,
  normalizeStateName,
  resolveCountryAndState,
  isBlankStatePlaceholder,
  // toSalesforceCountryCode, // replaced by normalizeCountryName (text field approach)
  // toSalesforceStateCode,   // no longer needed — state sent as-is
  cleanEmail,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
  SF_API_VERSION,
} from "./utils";
import {
  createResultsSheetFromContacts,
  appendWindowLog,
} from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 2000; // rows per execution window

// Set to true to overlay corrected values from the correction sheet onto TSV records.
// Set back to false for normal runs.
// Salesforce API field names to override from the correction sheet when correction mode is on.
// The client's correction sheet must use these exact column names.
// PersonMailingState and PersonMailingCountry are normalized automatically (same logic as the main flow).
// PersonBirthdate, PersonEmail, University_ID__pc, Discount_Verification_Date__pc (added Aug 2026):
// validated before being applied — an invalid correction value is applied as blank rather
// than passed through as-is. See the corrEntry block below. Correction mode only — the
// normal/full-import mapping in mapToAccount() is completely unaffected by this validation.
// University_ID__pc's 8-digit check runs for every record in a correction run (even if the
// correction sheet itself left that column blank for the row), not just overridden ones —
// see the corrEntry block below.
const CORRECTION_FIELDS: string[] = [
  "PersonMailingStreet",
  "PersonMailingCity",
  "PersonMailingState",
  "PersonMailingPostalCode",
  "PersonMailingCountry",
  "PersonBirthdate",
  "PersonEmail",
  "University_ID__pc",
  "Discount_Verification_Date__pc",
];

// These address fields are authoritative from the correction sheet even when the
// cell is blank — every record on the sheet was flagged for an address problem,
// so a blank Country/State/PostalCode means "clear this," not "no correction, use
// the original file's value." Falling back to the original value here just
// re-introduces the same bad data the correction was meant to fix. All other
// CORRECTION_FIELDS keep the opposite rule: a blank cell means "no override."
const ALWAYS_APPLY_FIELDS = new Set([
  "PersonMailingCountry",
  "PersonMailingState",
  "PersonMailingPostalCode",
]);

const CORRECTION_TAB = "Student";

// ── Raw TSV record ─────────────────────────────────────────────────────────────

interface RawStudentRecord {
  ID?: string;
  Last_Name?: string;
  First_Name?: string;
  Middle_Name?: string;
  Date_Of_Birth?: string;
  Evening_Phone?: string;
  Daytime_Phone?: string;
  EMail_Address?: string;
  Email_Address?: string; // fallback header casing — see mapToAccount
  Opt_In_Email?: string;
  Notes?: string;
  Address_Line1?: string;
  Address_Line2?: string;
  City?: string;
  State_Prov?: string;
  Zip_PC?: string;
  Country?: string;
  Gender?: string;
  Opt_In_Physical_Mail?: string;
  US_Citizen?: string;
  Stanford_Alumnus?: string;
  Highest_Degree?: string;
  Ethnicity?: string;
  TA_Discount_Type?: string;
  University_ID?: string;
  SAA_Number?: string;
  Do_Not_Enroll?: string;
  First_Quarter?: string;
  Total_Enrollments?: string;
  Discount_Verified?: string;
  Discount_Verification_Date?: string;
  Alert_Flag?: string;
  Most_Recent_Quarter?: string;
  Certificate_Issued?: string;
  Certificate_Issued_Date?: string;
  SBSAA_Number?: string;
  FERPA_Directory_Consent?: string;
  Discount_Lifetime?: string;
  // Allow extra columns (e.g. History) to be present so we can delete them
  [key: string]: string | undefined;
}

// ── Salesforce Person Account shape ───────────────────────────────────────────

interface SalesforceAccount {
  Student_ID_4D__c: string;
  Student_ID_4D__pc: string;
  CSP_Student_Id__pc: string;
  LastName: string;
  FirstName?: string;
  MiddleName?: string;
  PersonBirthdate?: string;
  PersonOtherPhone?: string;
  PersonEmail?: string;
  PersonHasOptedOutOfEmail?: boolean;
  Description?: string;
  PersonMailingStreet?: string;
  PersonMailingCity?: string;
  PersonMailingState?: string;
  PersonMailingPostalCode?: string;
  PersonMailingCountry?: string;
  // PersonMailingStateCode?: string;   // used when State/Country picklists require 2-letter codes
  // PersonMailingCountryCode?: string; // used when State/Country picklists require 2-letter codes
  Legal_Sex__pc?: string;
  Opt_In_Physical_Mail__pc?: boolean;
  US_Citizen__pc?: string; // "Yes" | "No"
  Stanford_Alumnus__pc?: string; // "Yes" | "No"
  Highest_Degree__pc?: string;
  Ethnicity__pc?: string;
  TA_Discount_Type__pc?: string;
  University_ID__pc?: string;
  SAA_Number__pc?: string;
  Do_Not_Enroll__pc?: boolean;
  First_Quarter__pc?: string;
  Total_Enrollments__pc?: number;
  Discount_Verified__pc?: string;
  Discount_Verification_Date__pc?: string;
  Alert_Flag__pc?: boolean;
  Most_Recent_Quarter__pc?: string;
  Certificate_Issued__pc?: boolean;
  Certificate_Issued_Date__pc?: string;
  SBSAA_Number__pc?: string;
  FERPA_Directory_Consent__pc?: boolean;
  Discount_Lifetime__pc?: boolean;
}

interface StreamResult {
  contacts: Partial<SalesforceAccount>[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
  allCorrectionRecordsFound: boolean;
}

// ── Migration History shape ────────────────────────────────────────────────────

// interface MigrationHistoryRecord {
//   External_ID_4D__c: string;
//   "Contact__r.External_ID_4D__c": string;
//   "Person_Account__r.External_ID_4D__c": string;
//   Description__c: string;
// }

// ── Field mapping ──────────────────────────────────────────────────────────────

// Source sends junk placeholder dates in more than one shape — "00/00/00"
// AND "00/00/0000" both mean "no date", but toDate() only rejects the first
// one on its own: "00/00/0000" matches the MM/DD/YYYY pattern (4-digit year)
// and comes back as the literal string "0000-00-00" instead of blank. Same
// guard Instructor/Associate already use before calling toDate().
function isJunkDatePlaceholder(v: string | undefined): boolean {
  return /^0+([./-]0+){2}$/.test(str(v));
}

function mapToAccount(raw: RawStudentRecord): Partial<SalesforceAccount> {
  const record: Partial<SalesforceAccount> = {
    Student_ID_4D__c: str(raw.ID).trim(),
    Student_ID_4D__pc: str(raw.ID).trim(),
    CSP_Student_Id__pc: str(raw.ID).trim(),
    LastName: str(raw.Last_Name),
  };

  const setStr = (key: keyof SalesforceAccount, v: string | undefined) => {
    (record as Record<string, unknown>)[key] = str(v);
  };

  // Restricted picklist fields: use #N/A (Bulk API null marker) when empty
  const setPicklist = (key: keyof SalesforceAccount, v: string | undefined) => {
    const val = str(v);
    (record as Record<string, unknown>)[key] = val || "#N/A";
  };

  const setBool = (key: keyof SalesforceAccount, v: string | undefined) => {
    (record as Record<string, unknown>)[key] = hasValue(v) ? toBool(v) : "";
  };

  const setDate = (key: keyof SalesforceAccount, v: string | undefined) => {
    (record as Record<string, unknown>)[key] = isJunkDatePlaceholder(v)
      ? ""
      : (toDate(v) ?? "");
  };

  // const setYesNo = (key: keyof SalesforceAccount, v: string | undefined) => {
  //   (record as Record<string, unknown>)[key] = hasValue(v) ? toYesNo(v) : "";
  // };

  setStr("FirstName", raw.First_Name);
  setStr("MiddleName", raw.Middle_Name);
  setDate("PersonBirthdate", raw.Date_Of_Birth);

  const eveningPhone = str(raw.Evening_Phone);
  const daytimePhone = str(raw.Daytime_Phone);
  const otherPhone =
    !eveningPhone || eveningPhone.toLowerCase() === "same"
      ? daytimePhone
      : eveningPhone;
  record.PersonOtherPhone = otherPhone || "";

  // Accept either header casing — the current TSV export uses "EMail_Address"
  // but fall back to "Email_Address" in case that ever changes.
  record.PersonEmail =
    cleanEmail(raw.EMail_Address ?? raw.Email_Address) || "";

  record.PersonHasOptedOutOfEmail = !toBool(raw.Opt_In_Email);

  const studentNotes = str(raw.Notes)
    .replace(/_4DNL_/g, "\n")
    .trim();
  // Always set Description so the key is present in every record object.
  // If omitted for records with no Notes, PapaParse skips the column when
  // serialising the Bulk API upload CSV, shifting all address columns left
  // and misaligning every appended row in the result sheet.
  record.Description = studentNotes;

  record.PersonMailingStreet = [str(raw.Address_Line1), str(raw.Address_Line2)]
    .filter(Boolean)
    .join("\n");

  setStr("PersonMailingCity", raw.City);
  {
    const { country, state } = resolveCountryAndState(
      raw.Country,
      raw.State_Prov,
    );
    record.PersonMailingCountry = country;
    record.PersonMailingState = state;
  }
  // const isUS = record.PersonMailingCountry === "United States";
  // record.PersonMailingState = isUS ? normalizeStateName(raw.State_Prov) : "";
  // Same rule as State above: blank out only when country resolved to a
  // known non-US value. If country is blank we can't tell whether the
  // address is US or not, so keep the zip as-is.
  record.PersonMailingPostalCode =
    record.PersonMailingCountry === "United States" ||
    !record.PersonMailingCountry
      ? str(raw.Zip_PC)
      : "";
  // Previous 2-letter code approach (kept for reference):
  // const mailingCountryCode = toSalesforceCountryCode(raw.Country);
  // record.PersonMailingCountryCode = mailingCountryCode;
  // record.PersonMailingStateCode = toSalesforceStateCode(raw.State_Prov, mailingCountryCode);

  const genderNorm = str(raw.Gender).trim().toUpperCase();
  const gender =
    genderNorm === "M" || genderNorm === "MA" || genderNorm === "MALE"
      ? "M"
      : genderNorm === "F" ||
          genderNorm === "FA" ||
          genderNorm === "FE" ||
          genderNorm === "FEMALE"
        ? "F"
        : genderNorm === "D" ||
            genderNorm === "DE" ||
            genderNorm.startsWith("DECLINE")
          ? "D"
          : genderNorm === "N" ||
              genderNorm === "NOT" ||
              genderNorm.startsWith("NON")
            ? "N"
            : "";
  setPicklist("Legal_Sex__pc", gender);

  setBool("Opt_In_Physical_Mail__pc", raw.Opt_In_Physical_Mail);
  // setYesNo("US_Citizen__pc", raw.US_Citizen);       // excluded per architect
  // setYesNo("Stanford_Alumnus__pc", raw.Stanford_Alumnus); // excluded per architect
  setPicklist("Highest_Degree__pc", raw.Highest_Degree);
  // setPicklist("Ethnicity__pc", raw.Ethnicity);       // excluded per architect
  // Normalize "(S T A P)" → "STAP"; fix case mismatches; invalid values → #N/A
  const TA_DISCOUNT_INVALID = new Set(["not", "0", "00/00/00"]);
  // CSV has wrong case for these two — SF picklist uses lowercase variant
  const TA_CASE_MAP: Record<string, string> = {
    "PROMO-EZRA": "PROMO-ezra",
    "PROMO-Think15": "PROMO-think15",
  };
  const taDiscountRaw = str(raw.TA_Discount_Type);
  const taDiscountNorm = taDiscountRaw.replace(
    /^\(([A-Z](?:\s[A-Z])+)\)$/,
    (_, g) => g.replace(/\s/g, ""),
  );
  const taDiscountMapped = TA_CASE_MAP[taDiscountNorm] ?? taDiscountNorm;
  const taDiscount = TA_DISCOUNT_INVALID.has(taDiscountMapped.toLowerCase())
    ? ""
    : taDiscountMapped;
  setPicklist("TA_Discount_Type__pc", taDiscount);
  setStr("University_ID__pc", raw.University_ID);
  setStr("SAA_Number__pc", raw.SAA_Number);
  setBool("Do_Not_Enroll__pc", raw.Do_Not_Enroll);
  setStr("First_Quarter__pc", raw.First_Quarter);
  const totalEnrollments = parseInt(str(raw.Total_Enrollments), 10);
  // Same reason as Description above — always set the key, use "" as the
  // Bulk API null sentinel when the raw value isn't a valid number.
  (record as Record<string, unknown>).Total_Enrollments__pc = !isNaN(
    totalEnrollments,
  )
    ? totalEnrollments
    : "";
  // Restricted picklist — use setPicklist so a blank source value sends
  // "#N/A" and actually clears a stale value on re-import (setStr's plain
  // "" is a no-op on Bulk API updates, leaving old data in place).
  setPicklist("Discount_Verified__pc", raw.Discount_Verified);
  setDate("Discount_Verification_Date__pc", raw.Discount_Verification_Date);
  setBool("Alert_Flag__pc", raw.Alert_Flag);
  setStr("Most_Recent_Quarter__pc", raw.Most_Recent_Quarter);
  setBool("Certificate_Issued__pc", raw.Certificate_Issued);
  setDate("Certificate_Issued_Date__pc", raw.Certificate_Issued_Date);
  setStr("SBSAA_Number__pc", raw.SBSAA_Number);
  setBool("FERPA_Directory_Consent__pc", raw.FERPA_Directory_Consent);
  setBool("Discount_Lifetime__pc", raw.Discount_Lifetime);
  return record;
}

// ── Correction sheet loader ────────────────────────────────────────────────────

/**
 * Reads a Google Sheet and returns a Map<rawId, {tsvField: correctedValue}>.
 * Only fields listed in `CORRECTION_FIELDS` are captured.
 * Blank cells in the sheet are skipped (existing TSV value is kept).
 */
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
  CORRECTION_FIELDS: string[],
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

  // Intercept the raw byte stream to record exact file byte positions after
  // each newline. This is byte-accurate for all UTF-8 content including
  // multi-byte characters (é, ñ, 中, etc.) — unlike PapaParse's meta.cursor
  // which counts decoded JS string characters and undercounts for such chars.
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
          // Record absolute file position immediately after this '\n'.
          lineEndBytes.push(byteOffset + totalBytesReceived + i + 1);
        }
      }
      totalBytesReceived += chunk.length;
      callback(null, chunk);
    },
  });

  (response.data as NodeJS.ReadableStream).pipe(byteTracker);

  return new Promise((resolve, reject) => {
    // For byteOffset > 0 the downloaded chunk has no header row — use knownHeaders.
    // For the first window (byteOffset = 0) the first row IS the header.
    let parsedHeaders: string[] =
      knownHeaders.length > 0 ? [...knownHeaders] : [];
    const contacts: Partial<SalesforceAccount>[] = [];
    let aborted = false;
    let firstId = "";
    let lastId = "";
    let skippedCount = 0;
    // Byte position after the last fully-processed row. When the window is full
    // we abort before processing the current row, so this is already the start
    // byte of that unprocessed row — the correct resume point for next window.
    let lastCompletedCursor = byteOffset;
    // Counts every step() call (one per raw line, including empty lines) so we
    // can look up the matching entry in lineEndBytes.
    let stepCount = 0;

    parse(byteTracker as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: false,
      // Must be false so step() fires for every raw line (including empty ones),
      // keeping stepCount in sync with lineEndBytes.
      skipEmptyLines: false,
      // TSV fields are never quoted. The default quoteChar (") would cause
      // PapaParse to read across tab delimiters if a field starts with ",
      // merging multiple fields into one and shifting all subsequent columns.
      quoteChar: "\x00",

      step: (result: ParseResult<string[]>, parser: Parser) => {
        if (aborted) return;

        const row = result.data as unknown as string[];
        // lineEndBytes[stepCount] is the exact file byte position after this
        // row's newline, computed from raw Buffer bytes — always accurate.
        const rowEndByte =
          lineEndBytes[stepCount] ?? byteOffset + totalBytesReceived;
        stepCount++;

        // Skip empty lines (replaces skipEmptyLines: true).
        if (row.length === 0 || row.every((c) => c.replace(/\r/g, "") === "")) {
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (parsedHeaders.length === 0) {
          parsedHeaders = row.map((h) => h.replace(/\r/g, "").trim());
          lastCompletedCursor = rowEndByte;
          return;
        }

        // Check BEFORE processing: lastCompletedCursor already points to the
        // start of this row — correct resume byte for the next window.
        if (contacts.length >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        // Skip rows whose column count doesn't match the header. These have an
        // embedded tab or a missing field and would map values to wrong columns.
        if (row.length !== parsedHeaders.length) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const record: RawStudentRecord = {};
        parsedHeaders.forEach((header, i) => {
          record[header] = row[i] ?? "";
        });

        // /^[1-9]\d*$/ rejects empty, non-numeric, and ID=0.
        if (!record.ID || !/^[1-9]\d*$/.test(record.ID.trim())) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        // In correction mode, only submit records that exist in the correction sheet.
        // All other records are skipped — they already succeeded and don't need re-importing.
        if (correctionEnabled && !correctionMap.has(record.ID.trim())) {
          lastCompletedCursor = rowEndByte;
          return;
        }

        delete record.History;

        const account = mapToAccount(record);

        // Apply SF field-level corrections after mapping so country/state go through
        // the same normalisation logic as the main flow (2-letter → full name).
        const corrEntry = correctionMap.get(record.ID.trim());
        if (corrEntry) {
          const acc = account as Record<string, unknown>;
          // Hong Kong short-circuit: this org has no top-level "Hong Kong"
          // country — it only exists as a China subdivision — so a country
          // correction of HK/Hong Kong sets both fields directly and skips
          // the normal branches below (which would otherwise blank the state
          // out, since China isn't "United States").
          if (
            corrEntry.PersonMailingCountry !== undefined &&
            ["HK", "HONG KONG"].includes(
              corrEntry.PersonMailingCountry.trim().toUpperCase(),
            )
          ) {
            acc.PersonMailingCountry = "China";
            acc.PersonMailingState = "Hong Kong";
          } else {
            // Country first — normalizeStateName needs the resolved country to decide isUS
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
              // Literal "N/A"-style placeholder text is never a real state.
              if (isBlankStatePlaceholder(stateVal)) {
                stateVal = "";
              }
              acc.PersonMailingState = stateVal;
              // const isUSCorr = countryVal === "United States";
              // acc.PersonMailingState = isUSCorr
              //   ? normalizeStateName(corrEntry.PersonMailingState)
              //   : "";
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
          // Non-address fields (Aug 2026) — validate before applying. If the
          // correction sheet's value doesn't match the field's required
          // format, apply blank instead of the bad value rather than passing
          // it through. Correction mode only.
          {
            // Validate regardless of whether the correction sheet supplied an
            // override for this row — otherwise a blank cell here lets the
            // original (possibly invalid) TSV value pass through unchecked.
            const val = (
              corrEntry.University_ID__pc ?? String(acc.University_ID__pc ?? "")
            ).trim();
            // Salesforce validation rule requires exactly 8 digits.
            acc.University_ID__pc = /^\d{8}$/.test(val) ? val : "";
          }
          {
            // Validate regardless of whether the correction sheet supplied an
            // override for this row — otherwise a blank cell here lets the
            // original (possibly invalid) TSV value pass through unchecked.
            // toDate() returns "" for anything it can't parse, but a value
            // like "1/22/254" (truncated year) parses "successfully" into a
            // nonsensical date instead of failing — Salesforce then rejects
            // it with FIELD_INTEGRITY_EXCEPTION. Guard against that with a
            // plausible-year check (assumption: 1900–current year).
            const source =
              corrEntry.PersonBirthdate ?? String(acc.PersonBirthdate ?? "");
            const parsed = toDate(source);
            const year = parsed ? Number(parsed.slice(0, 4)) : NaN;
            const currentYear = new Date().getFullYear();
            acc.PersonBirthdate =
              parsed && year >= 1900 && year <= currentYear ? parsed : "";
          }
          if (corrEntry.Discount_Verification_Date__pc !== undefined) {
            acc.Discount_Verification_Date__pc = toDate(
              corrEntry.Discount_Verification_Date__pc,
            );
          }
          if (corrEntry.PersonEmail !== undefined) {
            // cleanEmail() already returns "" for anything that isn't a
            // valid single email address.
            acc.PersonEmail = cleanEmail(corrEntry.PersonEmail);
          }
          // All other fields applied directly (field is a variable — bracket notation required)
          for (const field of CORRECTION_FIELDS) {
            if (
              field === "PersonMailingCountry" ||
              field === "PersonMailingState" ||
              field === "PersonMailingPostalCode" ||
              field === "University_ID__pc" ||
              field === "PersonBirthdate" ||
              field === "Discount_Verification_Date__pc" ||
              field === "PersonEmail"
            )
              continue;
            if (corrEntry[field] !== undefined) {
              acc[field] = corrEntry[field];
            }
          }
        }
        const extId = `person${record.ID.trim()}`;
        if (!firstId) firstId = extId;
        lastId = extId;
        contacts.push(account);
        lastCompletedCursor = rowEndByte;

        // In correction mode, stop as soon as every correction record has been found.
        // Avoids scanning the rest of the file unnecessarily.
        if (
          correctionEnabled &&
          correctionMap.size > 0 &&
          contacts.length >= correctionMap.size
        ) {
          aborted = true;
          parser.abort();
        }
      },

      complete: () =>
        resolve({
          contacts,
          hasMore:
            aborted &&
            !(
              correctionEnabled &&
              correctionMap.size > 0 &&
              contacts.length >= correctionMap.size
            ),
          nextByteOffset: lastCompletedCursor,
          parsedHeaders,
          firstId,
          lastId,
          skippedCount,
          allCorrectionRecordsFound:
            correctionEnabled &&
            correctionMap.size > 0 &&
            contacts.length >= correctionMap.size,
        }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const studentImport = flow({
  name: "Student Import",
  stableKey: "e5f6a7b8-5555-4e9f-2a3b-eeff00112233",
  description:
    "Streams the student TSV from Google Drive one window at a time, maps rows to " +
    "Salesforce Person Account records, and upserts via Bulk API 2.0.  Recurses until " +
    "the full file has been processed.",

  onTrigger: async (_context, payload) => {
    await Promise.resolve();
    return { payload };
  },

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    // ── 1. Read cursor (byteOffset) from trigger payload ──────────────────────
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
    const windowNumber =
      typeof triggerBody?.windowNumber === "number"
        ? triggerBody.windowNumber
        : 1;
    const accountSheetId =
      typeof triggerBody?.accountSheetId === "string"
        ? triggerBody.accountSheetId
        : undefined;

    logger.info(`[Student Import] Starting at byte offset ${byteOffset}`);

    // ── 2. Resolve connections and file ID ─────────────────────────────────────
    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = configVars["Student File ID"];
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;
    const correctionEnabled =
      String(configVars["Student Correction Enabled"]) === "true";

    if (!fileId) throw new Error("Student File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── DEBUG: token diagnostics ───────────────────────────────────────────────
    const sfRawToken = (sfConn as unknown as { token?: Record<string, unknown> }).token ?? {};
    const issuedAt = sfRawToken["issued_at"] ? Number(sfRawToken["issued_at"]) : null;
    const tokenAgeSeconds = issuedAt ? Math.floor((Date.now() - issuedAt) / 1000) : null;
    logger.info(
      `[Student Import][TokenDebug] access_token (first 10): ${sfToken.slice(0, 10)}…` +
      `\n  expires_in  : ${sfRawToken["expires_in"] ?? "n/a"}` +
      `\n  issued_at   : ${sfRawToken["issued_at"] ?? "n/a"}` +
      `\n  token_age   : ${tokenAgeSeconds !== null ? tokenAgeSeconds + "s" : "n/a"}` +
      `\n  token_type  : ${sfRawToken["token_type"] ?? "n/a"}` +
      `\n  has_refresh : ${sfRawToken["refresh_token"] ? "yes" : "no"}` +
      `\n  local_time  : ${new Date().toISOString()}`
    );
    // ──────────────────────────────────────────────────────────────────────────

    // ── 2a. Correction sheet config ────────────────────────────────────────────
    // When enabled, CORRECTION_FIELDS (defined at top of file) are overridden
    // per-record from the correction sheet before mapping. All other fields come from TSV.
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
          `[Student Import] Correction sheet loaded — ${loaded.size} rows, fields: [${CORRECTION_FIELDS.join(", ")}]`,
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
          `[Student Import] Could not load correction sheet: ${detail}`,
        );
      }
    }

    // ── 3. Stream & parse the TSV window from Google Drive ─────────────────────
    logger.info(
      `[Student Import] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
    );
    const {
      contacts,
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
      `[Student Import] Parsed ${contacts.length} contacts (hasMore=${hasMore}, nextByte=${nextByteOffset})`,
    );

    // ── 5. Upsert to Salesforce Bulk API 2.0 ──────────────────────────────────
    let nextAccountSheetId = accountSheetId;
    if (contacts.length > 0) {
      // ── Token health check before bulk job ────────────────────────────────
      try {
        await axios.get(
          `${sfInstanceUrl}/services/data/${SF_API_VERSION}/limits`,
          { headers: { Authorization: `Bearer ${sfToken}` } },
        );
        logger.info(`[Student Import][TokenDebug] Token valid at ${new Date().toISOString()} — token (first 10): ${sfToken.slice(0, 10)}…`);
      } catch (tokenErr: unknown) {
        const te = tokenErr as { response?: { status?: number; data?: unknown } };
        logger.error(
          `[Student Import][TokenDebug] Token INVALID at ${new Date().toISOString()} — ` +
          `status: ${te.response?.status ?? "unknown"} — ` +
          `token (first 10): ${sfToken.slice(0, 10)}…` +
          `\n  Details: ${JSON.stringify(te.response?.data, null, 2)}`,
        );
      }
      // ──────────────────────────────────────────────────────────────────────
      let accountJobResult;
      try {
        accountJobResult = await runBulkJob(
          sfInstanceUrl,
          sfToken,
          "Account",
          "Student_ID_4D__c",
          contacts as Record<string, unknown>[],
          logger,
          "[Student Import]",
        );
      } finally {
        const ageAfter = issuedAt ? Math.floor((Date.now() - issuedAt) / 1000) : null;
        try {
          await axios.get(
            `${sfInstanceUrl}/services/data/${SF_API_VERSION}/limits`,
            { headers: { Authorization: `Bearer ${sfToken}` } },
          );
          logger.info(`[Student Import][ConnectionStatus] VALID — token age: ${ageAfter !== null ? ageAfter + "s" : "n/a"} — at: ${new Date().toISOString()}`);
        } catch (postErr: unknown) {
          const pe = postErr as { response?: { status?: number; data?: unknown } };
          logger.error(`[Student Import][ConnectionStatus] EXPIRED — token age: ${ageAfter !== null ? ageAfter + "s" : "n/a"} — status: ${pe.response?.status ?? "unknown"} — at: ${new Date().toISOString()}`);
        }
      }

      // ── 5a. Create / append Google Sheet results report ─────────────────────
      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Student Import",
          objectName: "Account",
          contacts: contacts as Record<string, unknown>[],
          externalIdField: "Student_ID_4D__c",
          failedExternalIds: accountJobResult.failedExternalIds,
          successfulCsv: accountJobResult.successfulCsv,
          failedCsv: accountJobResult.failedCsv,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: accountSheetId,
        });
        nextAccountSheetId = sheet.spreadsheetId;
        logger.info(`[Student Import] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(
          `[Student Import] Could not update results sheet: ${String(err)}`,
        );
      }

      // ── 5b. Window tracking log ─────────────────────────────────────────────
      if (nextAccountSheetId) {
        try {
          const failedCount = accountJobResult.failedExternalIds.size;
          await appendWindowLog(nextAccountSheetId, gdToken, {
            windowNumber,
            timestamp: new Date().toISOString(),
            startByte: byteOffset,
            endByte: nextByteOffset,
            recordsInWindow: contacts.length,
            successful: contacts.length - failedCount,
            failed: failedCount,
            skipped: skippedCount,
            firstId,
            lastId,
          });
        } catch (err: unknown) {
          logger.warn(
            `[Student Import] Could not write window log: ${String(err)}`,
          );
        }
      }

      // ── 5c. Upsert Migration_History__c — temporarily disabled ──────────────
      // const timestamp = new Date().toISOString();
      // const migrationRecords: MigrationHistoryRecord[] = contacts
      //   .filter(
      //     (c) =>
      //       c.External_ID_4D__c &&
      //       !accountJobResult.failedExternalIds.has(c.External_ID_4D__c),
      //   )
      //   .map((c) => {
      //     const rec: MigrationHistoryRecord = {
      //       External_ID_4D__c: `${c.External_ID_4D__c!}_Student_${timestamp}`,
      //       "Contact__r.External_ID_4D__c": c.External_ID_4D__c!,
      //       "Person_Account__r.External_ID_4D__c": c.External_ID_4D__c!,
      //       Description__c: `Upserted from Student Import | Object: Account | Executed: ${timestamp}`,
      //     };
      //     return rec;
      //   });
      // logger.info(
      //   `[Student Import] Upserting ${migrationRecords.length} Migration_History__c records` +
      //     (accountJobResult.failedExternalIds.size > 0
      //       ? ` (${accountJobResult.failedExternalIds.size} skipped — Account upsert failed)`
      //       : "") +
      //     "…",
      // );
      // if (migrationRecords.length > 0) {
      //   await runBulkJob(
      //     sfInstanceUrl,
      //     sfToken,
      //     "Migration_History__c",
      //     "External_ID_4D__c",
      //     migrationRecords as unknown as Record<string, unknown>[],
      //     logger,
      //     "[Student Import][MigrationHistory]",
      //   );
      // }
    } else {
      logger.info(
        "[Student Import] Window contained no records; skipping upload.",
      );
    }

    // ── 6. Recurse if more rows remain ─────────────────────────────────────────
    if (allCorrectionRecordsFound) {
      logger.info(
        `[Student Import] All ${correctionMap.size} correction records found — stopping early.`,
      );
    }
    if (hasMore) {
      logger.info(
        `[Student Import] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );

      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Student Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        accountSheetId: nextAccountSheetId,
      });
    } else {
      logger.info("[Student Import] All rows processed — import complete.");
    }

    return {
      data: {
        byteOffset,
        rowsProcessed: contacts.length,
        hasMore,
      },
    };
  },
});

export default [studentImport];
