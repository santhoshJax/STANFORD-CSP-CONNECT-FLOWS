/**
 * Stanford CSP Migration – Unit Rule Import flow.
 *
 * Reads the Unit Rule TSV from Google Drive and upserts
 * Unit_Rule__c records to Salesforce via Bulk API 2.0.
 * Upsert key: External_ID_4D__c
 *
 * Fields NOT mapped (Do Not Map — confirmed by Liz, May 27):
 *   Format (Delivery_Format__c), Department_ID (Department__c)
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import { str, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createResultsSheet } from "./reportResults";

// ── Raw TSV row shape ─────────────────────────────────────────────────────────

interface UnitRuleRaw {
  ID?: string;
  Rule_Type?: string;
  Priority?: string;
  Maximum?: string;
  Units?: string;
  // Format and Department_ID — Do Not Map
}

// ── Salesforce record shape ───────────────────────────────────────────────────

interface UnitRuleSf {
  External_ID_4D__c: string;
  Rule_Type__c?: string;
  Priority__c?: number;
  Maximum_Threshold__c?: number;
  Units_Awarded__c?: number;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function toNumber(v: string | undefined): number | undefined {
  const raw = str(v).trim();
  if (!raw) return undefined;
  const n = parseFloat(raw);
  return isNaN(n) ? undefined : n;
}

// ── Fetch TSV from Google Drive ───────────────────────────────────────────────

async function fetchTsv(
  fileId: string,
  accessToken: string,
): Promise<UnitRuleRaw[]> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const rows: UnitRuleRaw[] = [];
    Papa.parse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: true,
      skipEmptyLines: true,
      quoteChar: "\0",
      step: (result: Papa.ParseResult<UnitRuleRaw>) => {
        rows.push(result.data as unknown as UnitRuleRaw);
      },
      complete: () => resolve(rows),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const unitRuleImport = flow({
  name: "Unit Rule",
  stableKey: "b7c8d9e0-f1a2-4b3c-5d6e-7f8a9b0c1d2e",
  description:
    "Reads the Unit Rule TSV from Google Drive and upserts " +
    "Unit_Rule__c records to Salesforce via Bulk API 2.0.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = configVars["Unit Rule File ID"] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Unit Rule File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 1. Fetch & parse ───────────────────────────────────────────────────────
    logger.info(`[UnitRule] Fetching file ${fileId}…`);
    const rawRows = await fetchTsv(fileId, gdToken);

    if (rawRows.length === 0) {
      logger.info("[UnitRule] No data rows found.");
      return { data: { unitRules: 0, skipped: 0 } };
    }

    logger.info(`[UnitRule] Parsed ${rawRows.length} rows.`);

    // ── 2. Build SF records ────────────────────────────────────────────────────
    const records: UnitRuleSf[] = [];
    let skipped = 0;

    for (const raw of rawRows) {
      const externalId = str(raw.ID).trim();
      if (!externalId) {
        skipped++;
        logger.warn("[UnitRule] Skipping row with blank ID.");
        continue;
      }

      const record: UnitRuleSf = {
        External_ID_4D__c: externalId,
      };

      const ruleType = str(raw.Rule_Type).trim();
      if (ruleType) record.Rule_Type__c = ruleType;

      const priority = toNumber(raw.Priority);
      if (priority !== undefined) record.Priority__c = priority;

      const maximum = toNumber(raw.Maximum);
      if (maximum !== undefined) record.Maximum_Threshold__c = maximum;

      const units = toNumber(raw.Units);
      if (units !== undefined) record.Units_Awarded__c = units;

      records.push(record);
    }

    if (skipped > 0) {
      logger.warn(`[UnitRule] Skipped ${skipped} rows with no ID.`);
    }

    // ── 3. Upsert to Salesforce ────────────────────────────────────────────────
    logger.info(
      `[UnitRule] Upserting ${records.length} Unit_Rule__c records…`,
    );
    const result = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "Unit_Rule__c",
      "External_ID_4D__c",
      records as unknown as Record<string, unknown>[],
      logger,
      "[UnitRule]",
    );

    try {
      const sheet = await createResultsSheet({
        flowName: "Unit Rule",
        objectName: "Unit_Rule__c",
        successfulCsv: result.successfulCsv,
        failedCsv: result.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[UnitRule] Results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(
        `[UnitRule] Could not update results sheet: ${String(err)}`,
      );
    }

    logger.info(
      `[UnitRule] Import complete. ${records.length} records processed.`,
    );
    return { data: { unitRules: records.length, skipped } };
  },
});

export default [unitRuleImport];
