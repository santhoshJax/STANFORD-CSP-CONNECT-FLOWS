/**
 * Stanford CSP Migration – Department Import flow.
 *
 * Reads the department Google Sheet (exported as TSV), splits records into
 * parent (category) departments and child departments, then upserts both
 * sets to Salesforce Account via Bulk API 2.0. Parents are upserted first
 * so children can reference them via Parent.External_ID_4D__c.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa from "papaparse";
import { str, getAccessToken, getSfInstanceUrl, runBulkJob } from "./utils";
import { createResultsSheet } from "./reportResults";

// ── Salesforce Account shape ───────────────────────────────────────────────────

interface SalesforceDepartment {
  External_ID_4D__c: string;
  Name: string;
  "Parent.External_ID_4D__c"?: string;
}

// interface MigrationHistoryRecord {
//   External_ID_4D__c: string;
//   "Person_Account__r.External_ID_4D__c": string;
//   Description__c: string;
// }

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

export const departmentImport = flow({
  name: "Department",
  stableKey: "a1b2c3d4-1111-4a1b-8c2d-aabbcc001111",
  description:
    "Reads the department Google Sheet, splits records into parent and child " +
    "departments, and upserts both to Salesforce Account via Bulk API 2.0.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;

    const gdConn = configVars["Google Drive Connection"] as Connection;
    const sfConn = configVars["Salesforce Connection"] as Connection;
    const fileId = configVars["Department File ID"] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as string | undefined;

    if (!fileId) throw new Error("Department File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 1. Fetch & parse ───────────────────────────────────────────────────────
    logger.info(`[Department] Fetching sheet ${fileId}…`);
    const rows = await fetchSheetAsTsv(fileId, gdToken);

    // Row 0 is the header — skip it
    if (rows.length <= 1) {
      logger.info("[Department] No data rows found.");
      return { data: { categoryDepts: 0, childDepts: 0 } };
    }

    // Read columns by header name, not raw position — immune to column
    // reordering, and any extra columns (e.g. a human-readable
    // Parent_Department_Name) are simply ignored.
    const headerRow = rows[0].map((h) => h.trim());
    const idIdx = headerRow.findIndex((h) => h.toLowerCase() === "id");
    const catIdIdx = headerRow.findIndex(
      (h) => h.toLowerCase() === "dept_category_id",
    );
    const nameIdx = headerRow.findIndex((h) => h.toLowerCase() === "name");
    if (idIdx === -1 || catIdIdx === -1 || nameIdx === -1) {
      throw new Error(
        `[Department] Sheet is missing one or more required headers ` +
          `(ID, Dept_Category_ID, Name). Found headers: [${headerRow.join(", ")}]`,
      );
    }

    const dataRows = rows.slice(1);
    logger.info(`[Department] Parsed ${dataRows.length} rows.`);

    // ── 2. Build department records ────────────────────────────────────────────
    // IDs 1–10 are the known, fixed set of top-level category departments —
    // forced to the "dept_cat_" prefix regardless of whether any row
    // currently references them as a parent. (Some categories may have no
    // children yet, so inferring "is category" purely from being referenced
    // would miss them.)
    const FORCED_CATEGORY_IDS = new Set([
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
    ]);

    const allDepts: SalesforceDepartment[] = [];
    for (const row of dataRows) {
      const id = str(row[idIdx]);
      if (!id || id.toLowerCase() === "null") continue;
      const parentId = str(row[catIdIdx]);
      const isCategory = FORCED_CATEGORY_IDS.has(id);
      const dept: SalesforceDepartment = {
        External_ID_4D__c: isCategory ? `dept_cat_${id}` : `dept_${id}`,
        Name: str(row[nameIdx]) || id,
      };
      // Categories are top-level — never attach a parent to them, even if
      // their own row has a (placeholder) Dept_Category_ID of 0.
      if (!isCategory && parentId && parentId.toLowerCase() !== "null") {
        dept["Parent.External_ID_4D__c"] =
          parentId === "0" ? "dept_cat_0" : `dept_cat_${parentId}`;
      }
      allDepts.push(dept);
    }

    // ── 3. Split into categories (parents) and children ────────────────────────
    // Always add "Default Department" as a static parent category
    const categoryDepts: Pick<SalesforceDepartment, "External_ID_4D__c" | "Name">[] = [
      { External_ID_4D__c: "dept_cat_0", Name: "Default Department" },
      ...allDepts
        .filter((d) => d.External_ID_4D__c.startsWith("dept_cat_"))
        .map((d) => ({ External_ID_4D__c: d.External_ID_4D__c, Name: d.Name })),
    ];

    const childDepts = allDepts.filter(
      (d) => !d.External_ID_4D__c.startsWith("dept_cat_"),
    );

    // ── 4. Upsert parents first, then children ─────────────────────────────────
    // const timestamp = new Date().toISOString();

    logger.info(
      `[Department] Upserting ${categoryDepts.length} parent departments…`,
    );
    const parentJobResult = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "Account",
      "External_ID_4D__c",
      categoryDepts as Record<string, unknown>[],
      logger,
      "[Department][Parents]",
    );
    try {
      const sheet = await createResultsSheet({
        flowName: "Department",
        objectName: "Account (Parents)",
        successfulCsv: parentJobResult.successfulCsv,
        failedCsv: parentJobResult.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Department] Parent results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(`[Department] Could not update parent results sheet: ${String(err)}`);
    }

    // const parentMigrationRecords: MigrationHistoryRecord[] = categoryDepts
    //   .filter((d) => !parentJobResult.failedExternalIds.has(d.External_ID_4D__c))
    //   .map((d) => ({
    //     External_ID_4D__c: `${d.External_ID_4D__c}_Department_${timestamp}`,
    //     "Person_Account__r.External_ID_4D__c": d.External_ID_4D__c,
    //     Description__c: `Upserted from Department Import | Object: Account (Parents) | Executed: ${timestamp}`,
    //   }));
    // if (parentMigrationRecords.length > 0) {
    //   logger.info(`[Department] Upserting ${parentMigrationRecords.length} Migration_History__c records for parents…`);
    //   await runBulkJob(
    //     sfInstanceUrl,
    //     sfToken,
    //     "Migration_History__c",
    //     "External_ID_4D__c",
    //     parentMigrationRecords as unknown as Record<string, unknown>[],
    //     logger,
    //     "[Department][MigrationHistory-Parents]",
    //   );
    // }

    logger.info(
      `[Department] Upserting ${childDepts.length} child departments…`,
    );
    const childJobResult = await runBulkJob(
      sfInstanceUrl,
      sfToken,
      "Account",
      "External_ID_4D__c",
      childDepts as unknown as Record<string, unknown>[],
      logger,
      "[Department][Children]",
    );
    try {
      const sheet = await createResultsSheet({
        flowName: "Department",
        objectName: "Account (Children)",
        successfulCsv: childJobResult.successfulCsv,
        failedCsv: childJobResult.failedCsv,
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Department] Child results sheet: ${sheet.url}`);
    } catch (err: unknown) {
      logger.warn(`[Department] Could not update child results sheet: ${String(err)}`);
    }

    // const childMigrationRecords: MigrationHistoryRecord[] = childDepts
    //   .filter((d) => !childJobResult.failedExternalIds.has(d.External_ID_4D__c))
    //   .map((d) => ({
    //     External_ID_4D__c: `${d.External_ID_4D__c}_Department_${timestamp}`,
    //     "Person_Account__r.External_ID_4D__c": d.External_ID_4D__c,
    //     Description__c: `Upserted from Department Import | Object: Account (Children) | Executed: ${timestamp}`,
    //   }));
    // if (childMigrationRecords.length > 0) {
    //   logger.info(`[Department] Upserting ${childMigrationRecords.length} Migration_History__c records for children…`);
    //   await runBulkJob(
    //     sfInstanceUrl,
    //     sfToken,
    //     "Migration_History__c",
    //     "External_ID_4D__c",
    //     childMigrationRecords as unknown as Record<string, unknown>[],
    //     logger,
    //     "[Department][MigrationHistory-Children]",
    //   );
    // }

    logger.info("[Department] Import complete.");
    return {
      data: {
        categoryDepts: categoryDepts.length,
        childDepts: childDepts.length,
      },
    };
  },
});

export default [departmentImport];
