/**
 * Stanford CSP Migration – Tuition Rule Import flow.
 *
 * Reads the Tuition Rule TSV from Google Drive and upserts
 * Tuition_Rule__c records to Salesforce via Bulk API 2.0.
 * Upsert key: External_ID_4D__c
 *
 * Fields NOT mapped (SA Decision – Do Not Map):
 *   Created_By, Created_Date, Created_Time,
 *   Last_Modified_By, Last_Modified_Date, Last_Modified_Time
 *
 * Status__c has no 4D source column but IS mapped — derived from Name:
 * "INACTIVE" in the name → Inactive, otherwise Active (per mapping doc).
 *
 * Effective_Start_Date__c / Effective_End_Date__c are genuinely skipped —
 * no 4D source and no derivation rule. Left null for all migrated records;
 * staff sets these going forward during the clone-and-archive workflow.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import { str, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createResultsSheet } from "./reportResults";

// ── Raw TSV row shape ─────────────────────────────────────────────────────────

interface TuitionRuleRaw {
  ID?: string;
  Name?: string;
  Rate_Type?: string;
  Base?: string;
  Rate?: string;
  Limit_31_39?: string;
  Limit_23_30?: string;
  Limit_19_22?: string;
  Limit_18_1?: string;
  Cap?: string;
  Comments?: string;
}

// ── Salesforce record shape ───────────────────────────────────────────────────

interface TuitionRuleSf {
  External_ID_4D__c: string;
  Name: string;
  Rate_Type__c?: string;
  Base_Amount__c?: number;
  Rate_Per_Unit__c?: number;
  Limit_Surcharge_31_39__c?: number;
  Limit_Surcharge_23_30__c?: number;
  Limit_Surcharge_19_22__c?: number;
  Limit_Surcharge_18_Under__c?: number;
  Tuition_Cap__c?: number;
  Comments__c?: string;
  Status__c: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function toCurrency(v: string | undefined): number | undefined {
  const raw = str(v).trim();
  if (!raw) return undefined;
  const n = parseFloat(raw);
  return isNaN(n) ? undefined : n;
}

// 4D exports line breaks as _4DNL_ tokens — replace with actual newlines.
function stripLineBreakTokens(v: string | undefined): string | undefined {
  const raw = str(v).trim();
  if (!raw) return undefined;
  return raw.replace(/_4DNL_/g, "\n").trim() || undefined;
}

// ── Fetch TSV from Google Drive ───────────────────────────────────────────────

async function fetchTsv(
  fileId: string,
  accessToken: string,
): Promise<TuitionRuleRaw[]> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: TuitionRuleRaw[] = [];
    Papa.parse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: true,
      skipEmptyLines: true,
      quoteChar: "\0",
      step: (result: Papa.ParseResult<TuitionRuleRaw>) => {
        rows.push(result.data as unknown as TuitionRuleRaw);
      },
      complete: () => resolve(rows),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const tuitionRuleImport = flow({
  name: "Tuition Rule",
  stableKey: "a9b8c7d6-e5f4-4a3b-2c1d-0e9f8a7b6c5d",
  description:
    "Reads the Tuition Rule TSV from Google Drive and upserts " +
    "Tuition_Rule__c records to Salesforce via Bulk API 2.0.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = configVars["Tuition Rule File ID"] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Tuition Rule File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 1. Fetch & parse ───────────────────────────────────────────────────────
    logger.info(`[TuitionRule] Fetching file ${fileId}…`);
    const rawRows = await fetchTsv(fileId, gdToken);

    if (rawRows.length === 0) {
      logger.info("[TuitionRule] No data rows found.");
      return { data: { tuitionRules: 0, skipped: 0 } };
    }

    logger.info(`[TuitionRule] Parsed ${rawRows.length} rows.`);

    // ── 2. Build SF records ────────────────────────────────────────────────────
    const records: TuitionRuleSf[] = [];
    let skipped = 0;

    for (const raw of rawRows) {
      const externalId = str(raw.ID).trim();
      if (!externalId) {
        skipped++;
        logger.warn("[TuitionRule] Skipping row with blank ID.");
        continue;
      }

      const name = str(raw.Name).trim() || externalId;
      const record: TuitionRuleSf = {
        External_ID_4D__c: externalId,
        Name: name,
        Status__c: name.toUpperCase().includes("INACTIVE")
          ? "Inactive"
          : "Active",
      };

      const rateType = str(raw.Rate_Type).trim();
      if (rateType) record.Rate_Type__c = rateType;

      const base = toCurrency(raw.Base);
      if (base !== undefined) record.Base_Amount__c = base;

      const rate = toCurrency(raw.Rate);
      if (rate !== undefined) record.Rate_Per_Unit__c = rate;

      const lim3139 = toCurrency(raw.Limit_31_39);
      if (lim3139 !== undefined) record.Limit_Surcharge_31_39__c = lim3139;

      const lim2330 = toCurrency(raw.Limit_23_30);
      if (lim2330 !== undefined) record.Limit_Surcharge_23_30__c = lim2330;

      const lim1922 = toCurrency(raw.Limit_19_22);
      if (lim1922 !== undefined) record.Limit_Surcharge_19_22__c = lim1922;

      const lim181 = toCurrency(raw.Limit_18_1);
      if (lim181 !== undefined) record.Limit_Surcharge_18_Under__c = lim181;

      const cap = toCurrency(raw.Cap);
      if (cap !== undefined) record.Tuition_Cap__c = cap;

      const comments = stripLineBreakTokens(raw.Comments);
      if (comments) record.Comments__c = comments;

      records.push(record);
    }

    if (skipped > 0) {
      logger.warn(`[TuitionRule] Skipped ${skipped} rows with no ID.`);
    }

    // ── 3. Upsert to Salesforce ────────────────────────────────────────────────
    logger.info(
      `[TuitionRule] Upserting ${records.length} Tuition_Rule__c records…`,
    );
    const result = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "Tuition_Rule__c",
      "External_ID_4D__c",
      records as unknown as Record<string, unknown>[],
      logger,
      "[TuitionRule]",
    );

    try {
      const sheet = await createResultsSheet({
        flowName: "Tuition Rule",
        objectName: "Tuition_Rule__c",
        successfulCsv: result.successfulCsv,
        failedCsv: result.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[TuitionRule] Results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(
        `[TuitionRule] Could not update results sheet: ${String(err)}`,
      );
    }

    logger.info(
      `[TuitionRule] Import complete. ${records.length} records processed.`,
    );
    return { data: { tuitionRules: records.length, skipped } };
  },
});

export default [tuitionRuleImport];
