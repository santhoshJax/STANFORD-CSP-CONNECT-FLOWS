/**
 * Stanford CSP Migration – Quarter Import flow.
 *
 * Reads the quarter Google Sheet (exported as TSV) and upserts three
 * Salesforce objects in dependency order:
 *   1. AcademicYear  (upsert key: External_ID__c)
 *   2. AcademicTerm  (upsert key: Code__c, references AcademicYear)
 *   3. AcademicSession (upsert key: Code__c, references AcademicTerm)
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import { str, hasValue, toBool, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createResultsSheet } from "./reportResults";

// ── Term season mapping ────────────────────────────────────────────────────────

const TERM_FULL_NAMES: Record<string, string> = {
  su: "Summer",
  fa: "Fall",
  wi: "Winter",
  sp: "Spring",
};

// ── Academic year key helper ──────────────────────────────────────────────────
// Fall defines the academic year; Winter/Spring/Summer belong to the prior fall.
// e.g. fa26 → "2026", wi27/sp27/su27 → "2026"
function getAcademicYearKey(seasonCode: string, calendarYear: string): string {
  return seasonCode === "fa" ? calendarYear : String(parseInt(calendarYear, 10) - 1);
}

// ── Date helper ───────────────────────────────────────────────────────────────
// Expects MM/DD/YY or MM/DD/YYYY. Returns ISO string or undefined.

function parseDate(v: string | undefined): string | undefined {
  const raw = str(v).trim();
  if (!raw || raw === "00/00/00" || raw === "00/00/0000") return undefined;
  const match = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (!match) return undefined;
  const [, m, d, y] = match;
  const year = y.length === 2 ? `20${y}` : y;
  const date = new Date(
    `${year}-${m.padStart(2, "0")}-${d.padStart(2, "0")}T00:00:00.000Z`,
  );
  if (isNaN(date.getTime())) return undefined;
  return date.toISOString();
}

// ── SF record shapes ──────────────────────────────────────────────────────────

interface AcademicYearRecord {
  Name: string;
  Year: string;
  External_ID_4D__c: string;
  External_ID__c: string;
}

interface AcademicTermRecord {
  Name: string;
  IsActive: boolean;
  "AcademicYear.External_ID_4D__c": string;
  Season: string;
  RegistrationOpenDate?: string;
  Code__c: string;
  External_ID_4D__c: string;
  Abbreviation__c: string;
}

interface AcademicSessionRecord {
  Name: string;
  "AcademicTerm.Code__c": string;
  Code__c: string;
  IsActive: boolean;
  External_ID_4D__c: string;
  Season: string;
  Abbreviation__c: string;
  Web_Launch_Date__c?: string;
  ClassStartDate?: string;
  ClassEndDate?: string;
}

// ── Fetch Google Sheet as TSV ─────────────────────────────────────────────────

async function fetchSheetAsTsv(
  fileId: string,
  accessToken: string,
): Promise<string[][]> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: string[][] = [];
    Papa.parse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: false,
      skipEmptyLines: true,
      step: (result: Papa.ParseResult<string[]>) => {
        rows.push(result.data as unknown as string[]);
      },
      complete: () => resolve(rows),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const quarterImport = flow({
  name: "Quarter",
  stableKey: "b2c3d4e5-2222-4b2c-9d3e-bbccdd002222",
  description:
    "Reads the quarter Google Sheet and upserts AcademicYear, AcademicTerm, " +
    "and AcademicSession records to Salesforce via Bulk API 2.0.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = configVars["Quarter File ID"] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as string | undefined;

    if (!fileId) throw new Error("Quarter File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 1. Fetch & parse ───────────────────────────────────────────────────────
    logger.info(`[Quarter] Fetching sheet ${fileId}…`);
    const rows = await fetchSheetAsTsv(fileId, gdToken);

    // Row 0 is the header — skip it
    if (rows.length <= 1) {
      logger.info("[Quarter] No data rows found.");
      return { data: { academicYears: 0, academicTerms: 0, academicSessions: 0 } };
    }

    const dataRows = rows.slice(1);
    logger.info(`[Quarter] Parsed ${dataRows.length} rows.`);

    // ── 2. Build records ───────────────────────────────────────────────────────
    // TSV column indices (0-based):
    //   [0]  External_ID_4D__c
    //   [1]  Abbreviation__c  (e.g. "su23" — first 2 chars = season code)
    //   [5]  IsActive
    //   [7]  Web_Launch_Date__c
    //   [8]  RegistrationOpenDate
    //   [9]  ClassStartDate
    //   [17] Code__c  (first 4 chars = year, e.g. "2024")
    //   [18] ClassEndDate

    const academicYearsMap = new Map<string, AcademicYearRecord>();
    const academicTerms: AcademicTermRecord[] = [];
    const academicSessions: AcademicSessionRecord[] = [];

    for (const row of dataRows) {
      const externalId = str(row[0]);
      if (!externalId) continue;

      const abbreviation = str(row[1]);
      const isActive = hasValue(row[5]) ? toBool(row[5]) : false;
      const webLaunchDate = parseDate(row[7]);
      const regOpenDate = parseDate(row[8]);
      const classStartDate = parseDate(row[9]);
      const code = str(row[17]);
      const classEndDate = parseDate(row[18]);

      const year = code.slice(0, 4);
      const seasonCode = abbreviation.slice(0, 2).toLowerCase();
      const seasonName = TERM_FULL_NAMES[seasonCode] ?? seasonCode;
      const ayKey = getAcademicYearKey(seasonCode, year);

      // AcademicYear (deduped by academic year key)
      if (ayKey && !academicYearsMap.has(ayKey)) {
        academicYearsMap.set(ayKey, {
          Name: `${ayKey}-${parseInt(ayKey, 10) + 1}`,
          Year: ayKey,
          External_ID_4D__c: ayKey,
          External_ID__c: ayKey,
        });
      }

      // AcademicTerm
      const term: AcademicTermRecord = {
        Name: `${seasonName} ${year}`,
        IsActive: isActive,
        "AcademicYear.External_ID_4D__c": ayKey,
        Season: seasonName,
        Code__c: code,
        External_ID_4D__c: externalId,
        Abbreviation__c: abbreviation,
      };
      if (regOpenDate) term.RegistrationOpenDate = regOpenDate;
      academicTerms.push(term);

      // AcademicSession (always created; include dates only when they exist)
      const session: AcademicSessionRecord = {
        Name: `${seasonName} ${year}`,
        "AcademicTerm.Code__c": code,
        Code__c: code,
        IsActive: isActive,
        External_ID_4D__c: externalId,
        Season: seasonName,
        Abbreviation__c: abbreviation,
      };
      if (webLaunchDate) session.Web_Launch_Date__c = webLaunchDate;
      if (classStartDate) session.ClassStartDate = classStartDate;
      if (classEndDate) session.ClassEndDate = classEndDate;
      academicSessions.push(session);
    }

    const academicYears = Array.from(academicYearsMap.values());

    // ── 3. Upsert in dependency order ─────────────────────────────────────────
    logger.info(`[Quarter] Upserting ${academicYears.length} AcademicYear records…`);
    const ayResult = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "AcademicYear",
      "External_ID__c",
      academicYears as unknown as Record<string, unknown>[],
      logger,
      "[Quarter][AcademicYear]",
    );
    try {
      const sheet = await createResultsSheet({
        flowName: "Quarter",
        objectName: "AcademicYear",
        successfulCsv: ayResult.successfulCsv,
        failedCsv: ayResult.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Quarter] AcademicYear results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(`[Quarter] Could not update AcademicYear results sheet: ${String(err)}`);
    }

    logger.info(`[Quarter] Upserting ${academicTerms.length} AcademicTerm records…`);
    const atResult = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "AcademicTerm",
      "Code__c",
      academicTerms as unknown as Record<string, unknown>[],
      logger,
      "[Quarter][AcademicTerm]",
    );
    try {
      const sheet = await createResultsSheet({
        flowName: "Quarter",
        objectName: "AcademicTerm",
        successfulCsv: atResult.successfulCsv,
        failedCsv: atResult.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Quarter] AcademicTerm results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(`[Quarter] Could not update AcademicTerm results sheet: ${String(err)}`);
    }

    logger.info(`[Quarter] Upserting ${academicSessions.length} AcademicSession records…`);
    const asResult = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "AcademicSession",
      "Code__c",
      academicSessions as unknown as Record<string, unknown>[],
      logger,
      "[Quarter][AcademicSession]",
    );
    try {
      const sheet = await createResultsSheet({
        flowName: "Quarter",
        objectName: "AcademicSession",
        successfulCsv: asResult.successfulCsv,
        failedCsv: asResult.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Quarter] AcademicSession results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(`[Quarter] Could not update AcademicSession results sheet: ${String(err)}`);
    }

    logger.info("[Quarter] Import complete.");
    return {
      data: {
        academicYears: academicYears.length,
        academicTerms: academicTerms.length,
        academicSessions: academicSessions.length,
      },
    };
  },
});

export default [quarterImport];
