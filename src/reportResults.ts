/**
 * reportResults.ts
 *
 * Manages a Google Sheet per integration execution:
 *   - First iteration (no spreadsheetId passed) → create a new sheet
 *   - Subsequent iterations (spreadsheetId passed) → append rows to existing sheet
 *   - If the active tab pair is near the row limit → add new numbered tabs
 *     e.g. "Success (2)" / "Errors (2)", then "Success (3)" / "Errors (3)", etc.
 *
 * Sheet title  : "Stanford CSP - {flowName} - {objectName} - {date} {time}"
 * Success tab  : all input fields  +  Salesforce Record ID  +  Action (Created/Updated)
 * Errors tab   : all input fields  +  Error Message
 */

import axios, { AxiosError } from "axios";
import Papa from "papaparse";

const SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets";
const DRIVE_FILES = "https://www.googleapis.com/drive/v3/files";

/** Row threshold per tab before a new numbered tab pair is created */
const MAX_ROWS_PER_TAB = 100_000;

// ── Types ──────────────────────────────────────────────────────────────────────

export interface ReportResultsParams {
  /** Human-readable flow name, e.g. "Student Import" */
  flowName: string;
  /** Salesforce object being upserted, e.g. "Account" */
  objectName: string;
  /** Raw CSV from Bulk API /successfulResults */
  successfulCsv: string;
  /** Raw CSV from Bulk API /failedResults */
  failedCsv: string;
  /** Google OAuth access token (from Google Drive connection) */
  accessToken: string;
  /** Drive folder ID — new sheets are placed here */
  folderId?: string;
  /**
   * If provided, append rows to this existing spreadsheet instead of creating a new one.
   * Pass the ID returned by the previous iteration's createResultsSheet call.
   */
  spreadsheetId?: string;
}

/** One object's worth of results for per-object tab reporting */
export interface ObjectResultData {
  objectName: string;
  successfulCsv: string;
  failedCsv: string;
}

export interface PerObjectReportParams {
  /** Human-readable flow name, e.g. "Course Import" */
  flowName: string;
  /** Per-object results — each gets its own Success/Errors tab pair */
  objects: ObjectResultData[];
  /** Google OAuth access token */
  accessToken: string;
  /** Drive folder ID — new sheets are placed here */
  folderId?: string;
  /** Pass the ID from the previous iteration to append rather than create */
  spreadsheetId?: string;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/**
 * Parse a Bulk API result CSV.
 * Original data columns come first; sf__ meta columns are renamed and appended last.
 * Success: adds "Salesforce Record ID" + "Action" (Created/Updated)
 * Errors:  adds "Error Message" (empty sf__Id column is dropped)
 */
function parseResultCsv(
  csv: string,
  type: "success" | "error",
): { headers: string[]; rows: string[][] } {
  if (!csv.trim()) return { headers: [], rows: [] };

  const parsed = Papa.parse<Record<string, string>>(csv, {
    header: true,
    skipEmptyLines: true,
  });

  const allFields = parsed.meta.fields ?? [];
  const dataCols = allFields.filter((f) => !f.startsWith("sf__"));
  const keepSfCols =
    type === "success"
      ? allFields.filter((c) => c === "sf__Id" || c === "sf__Created")
      : allFields.filter((c) => c === "sf__Error");

  const sfLabel: Record<string, string> = {
    sf__Id: "Salesforce Record ID",
    sf__Created: "Action",
    sf__Error: "Error Message",
  };

  const headers = [...dataCols, ...keepSfCols.map((c) => sfLabel[c] ?? c)];

  const rows = parsed.data.map((row) => [
    ...dataCols.map((c) => row[c] ?? ""),
    ...keepSfCols.map((c) => {
      const val = row[c] ?? "";
      if (c === "sf__Created")
        return val === "true" ? "Created" : val === "false" ? "Updated" : val;
      return val;
    }),
  ]);

  return { headers, rows };
}

/** Axios error → human-readable string including response body */
function describeAxiosError(err: unknown): string {
  const e = err as AxiosError;
  if (e.response) {
    return `HTTP ${e.response.status} – ${JSON.stringify(e.response.data)}`;
  }
  return String(err);
}

/**
 * Return the number of rows currently written in a sheet tab.
 * Returns 0 on any error (e.g. tab not yet populated).
 */
async function getTabRowCount(
  spreadsheetId: string,
  tabName: string,
  auth: { Authorization: string },
): Promise<number> {
  try {
    const { data } = await axios.get<{ values?: unknown[][] }>(
      `${SHEETS_BASE}/${spreadsheetId}/values/${encodeURIComponent(tabName)}`,
      { headers: auth },
    );
    return (data.values ?? []).length;
  } catch {
    return 0;
  }
}

/**
 * Return all existing tab titles in a spreadsheet.
 */
async function getTabTitles(
  spreadsheetId: string,
  auth: { Authorization: string },
): Promise<string[]> {
  const { data } = await axios.get<{
    sheets: { properties: { title: string } }[];
  }>(`${SHEETS_BASE}/${spreadsheetId}`, {
    params: { fields: "sheets.properties.title" },
    headers: auth,
  });
  return (data.sheets ?? []).map((s) => s.properties.title);
}

/**
 * Find the index of the highest-numbered active tab pair.
 * "Success" / "Errors" = 1, "Success (2)" / "Errors (2)" = 2, etc.
 */
function activeTabIndex(tabTitles: string[]): number {
  let max = 1;
  for (const t of tabTitles) {
    const m = /^(?:Success|Errors)\s+\((\d+)\)$/.exec(t);
    if (m) {
      const n = parseInt(m[1], 10);
      if (n > max) max = n;
    }
  }
  return max;
}

/** Build tab name for a given index: 1 → "Success", 2 → "Success (2)", etc. */
function tabName(base: "Success" | "Errors", index: number): string {
  return index === 1 ? base : `${base} (${index})`;
}

/**
 * Add a new numbered tab pair and write headers + rows into them.
 */
async function addTabPair(
  spreadsheetId: string,
  index: number,
  sHeaders: string[],
  sRows: string[][],
  eHeaders: string[],
  eRows: string[][],
  auth: { Authorization: string },
): Promise<void> {
  const sTab = tabName("Success", index);
  const eTab = tabName("Errors", index);

  await axios.post(
    `${SHEETS_BASE}/${spreadsheetId}:batchUpdate`,
    {
      requests: [
        { addSheet: { properties: { title: sTab } } },
        { addSheet: { properties: { title: eTab } } },
      ],
    },
    { headers: auth },
  );

  await axios.post(
    `${SHEETS_BASE}/${spreadsheetId}/values:batchUpdate`,
    {
      valueInputOption: "RAW",
      data: [
        {
          range: `${sTab}!A1`,
          values:
            sHeaders.length > 0 ? [sHeaders, ...sRows] : [["(no records)"]],
        },
        {
          range: `${eTab}!A1`,
          values:
            eHeaders.length > 0 ? [eHeaders, ...eRows] : [["(no records)"]],
        },
      ],
    },
    { headers: auth },
  );
}

// ── Shared write implementation ───────────────────────────────────────────────

async function writeRowsToResultsSheet(
  sHeaders: string[],
  sRows: string[][],
  eHeaders: string[],
  eRows: string[][],
  uHeaders: string[],
  uRows: string[][],
  existingSpreadsheetId: string | undefined,
  flowName: string,
  objectName: string,
  auth: { Authorization: string },
  folderId: string | undefined,
): Promise<{ url: string; spreadsheetId: string }> {
  // ── Append to existing sheet (subsequent windows) ─────────────────────────
  if (existingSpreadsheetId) {
    const sid = existingSpreadsheetId;
    const tabTitles = await getTabTitles(sid, auth);
    const idx = activeTabIndex(tabTitles);
    const sTab = tabName("Success", idx);
    const eTab = tabName("Errors", idx);

    const sCount = await getTabRowCount(sid, sTab, auth);
    const eCount = await getTabRowCount(sid, eTab, auth);

    if (
      sCount + sRows.length > MAX_ROWS_PER_TAB ||
      eCount + eRows.length > MAX_ROWS_PER_TAB
    ) {
      await addTabPair(sid, idx + 1, sHeaders, sRows, eHeaders, eRows, auth);
    } else {
      const appends: Promise<unknown>[] = [];
      if (sRows.length > 0) {
        appends.push(
          axios.post(
            `${SHEETS_BASE}/${sid}/values/${encodeURIComponent(sTab)}!A1:append`,
            { values: sRows },
            {
              params: {
                valueInputOption: "RAW",
                insertDataOption: "INSERT_ROWS",
              },
              headers: auth,
            },
          ),
        );
      }
      if (eRows.length > 0) {
        appends.push(
          axios.post(
            `${SHEETS_BASE}/${sid}/values/${encodeURIComponent(eTab)}!A1:append`,
            { values: eRows },
            {
              params: {
                valueInputOption: "RAW",
                insertDataOption: "INSERT_ROWS",
              },
              headers: auth,
            },
          ),
        );
      }
      await Promise.all(appends);
    }

    // "Not Processed" tab — only written when there are records for it.
    // Created on first occurrence; appended on subsequent ones.
    if (uRows.length > 0) {
      const NOT_PROCESSED_TAB = "Not Processed";
      if (tabTitles.includes(NOT_PROCESSED_TAB)) {
        await axios.post(
          `${SHEETS_BASE}/${sid}/values/${encodeURIComponent(NOT_PROCESSED_TAB)}!A1:append`,
          { values: uRows },
          {
            params: {
              valueInputOption: "RAW",
              insertDataOption: "INSERT_ROWS",
            },
            headers: auth,
          },
        );
      } else {
        await axios.post(
          `${SHEETS_BASE}/${sid}:batchUpdate`,
          {
            requests: [
              { addSheet: { properties: { title: NOT_PROCESSED_TAB } } },
            ],
          },
          { headers: auth },
        );
        await axios.post(
          `${SHEETS_BASE}/${sid}/values:batchUpdate`,
          {
            valueInputOption: "RAW",
            data: [
              {
                range: `${NOT_PROCESSED_TAB}!A1`,
                values: [uHeaders, ...uRows],
              },
            ],
          },
          { headers: auth },
        );
      }
    }

    return {
      url: `https://docs.google.com/spreadsheets/d/${sid}`,
      spreadsheetId: sid,
    };
  }

  // ── Create a new sheet (first window) ─────────────────────────────────────
  const datetime = new Date().toISOString().slice(0, 16).replace("T", " ");
  const title = `Stanford CSP - ${flowName} - ${objectName} - ${datetime}`;

  const { data: created } = await axios.post(
    SHEETS_BASE,
    { properties: { title } },
    { headers: auth },
  );
  const newSid = created.spreadsheetId as string;
  const defaultSheetId = (
    created.sheets as { properties: { sheetId: number } }[]
  )[0]?.properties?.sheetId;

  const addResponse = await axios.post(
    `${SHEETS_BASE}/${newSid}:batchUpdate`,
    {
      requests: [
        { addSheet: { properties: { title: "Success" } } },
        { addSheet: { properties: { title: "Errors" } } },
      ],
    },
    { headers: auth },
  );
  const addedSheets = addResponse.data.replies as {
    addSheet?: { properties: { sheetId: number } };
  }[];
  const successSheetId = addedSheets[0]?.addSheet?.properties?.sheetId;
  const errorsSheetId = addedSheets[1]?.addSheet?.properties?.sheetId;

  await axios.post(
    `${SHEETS_BASE}/${newSid}/values:batchUpdate`,
    {
      valueInputOption: "RAW",
      data: [
        {
          range: "Success!A1",
          values:
            sHeaders.length > 0 ? [sHeaders, ...sRows] : [["(no records)"]],
        },
        {
          range: "Errors!A1",
          values:
            eHeaders.length > 0 ? [eHeaders, ...eRows] : [["(no records)"]],
        },
      ],
    },
    { headers: auth },
  );

  if (defaultSheetId !== undefined) {
    await axios.post(
      `${SHEETS_BASE}/${newSid}:batchUpdate`,
      { requests: [{ deleteSheet: { sheetId: defaultSheetId } }] },
      { headers: auth },
    );
  }

  // "Not Processed" tab — only created when there are records for it
  let uSheetId: number | undefined;
  if (uRows.length > 0) {
    const addUResponse = await axios.post(
      `${SHEETS_BASE}/${newSid}:batchUpdate`,
      { requests: [{ addSheet: { properties: { title: "Not Processed" } } }] },
      { headers: auth },
    );
    uSheetId = (
      addUResponse.data.replies as {
        addSheet?: { properties: { sheetId: number } };
      }[]
    )[0]?.addSheet?.properties?.sheetId;
    await axios.post(
      `${SHEETS_BASE}/${newSid}/values:batchUpdate`,
      {
        valueInputOption: "RAW",
        data: [{ range: "Not Processed!A1", values: [uHeaders, ...uRows] }],
      },
      { headers: auth },
    );
  }

  const resizeRequests = [];
  if (successSheetId !== undefined) {
    resizeRequests.push({
      autoResizeDimensions: {
        dimensions: {
          sheetId: successSheetId,
          dimension: "COLUMNS",
          startIndex: 0,
          endIndex: sHeaders.length || 1,
        },
      },
    });
  }
  if (errorsSheetId !== undefined) {
    resizeRequests.push({
      autoResizeDimensions: {
        dimensions: {
          sheetId: errorsSheetId,
          dimension: "COLUMNS",
          startIndex: 0,
          endIndex: eHeaders.length || 1,
        },
      },
    });
  }
  if (uSheetId !== undefined) {
    resizeRequests.push({
      autoResizeDimensions: {
        dimensions: {
          sheetId: uSheetId,
          dimension: "COLUMNS",
          startIndex: 0,
          endIndex: uHeaders.length || 1,
        },
      },
    });
  }
  if (resizeRequests.length > 0) {
    await axios
      .post(
        `${SHEETS_BASE}/${newSid}:batchUpdate`,
        { requests: resizeRequests },
        { headers: auth },
      )
      .catch(() => {
        /* non-critical */
      });
  }

  if (folderId) {
    await axios.patch(
      `${DRIVE_FILES}/${newSid}`,
      {},
      {
        params: {
          addParents: folderId,
          removeParents: "root",
          supportsAllDrives: "true",
        },
        headers: auth,
      },
    );
  }

  return {
    url: `https://docs.google.com/spreadsheets/d/${newSid}`,
    spreadsheetId: newSid,
  };
}

// ── Main exports ───────────────────────────────────────────────────────────────

export async function createResultsSheet(
  params: ReportResultsParams,
): Promise<{ url: string; spreadsheetId: string }> {
  const auth = { Authorization: `Bearer ${params.accessToken}` };
  const { headers: sHeaders, rows: sRows } = parseResultCsv(
    params.successfulCsv,
    "success",
  );
  const { headers: eHeaders, rows: eRows } = parseResultCsv(
    params.failedCsv,
    "error",
  );
  return writeRowsToResultsSheet(
    sHeaders,
    sRows,
    eHeaders,
    eRows,
    [],
    [],
    params.spreadsheetId,
    params.flowName,
    params.objectName,
    auth,
    params.folderId,
  );
}

export interface ContactResultsParams {
  flowName: string;
  objectName: string;
  /** Every record sent to Salesforce this window — source of truth for all rows */
  contacts: Record<string, unknown>[];
  /** The Salesforce external ID field, e.g. "External_ID_4D__c" */
  externalIdField: string;
  /** External IDs of records that failed in Salesforce */
  failedExternalIds: Set<string>;
  /** Raw CSV from Bulk API /successfulResults — used to look up SF Record IDs */
  successfulCsv: string;
  /** Raw CSV from Bulk API /failedResults — used to look up error messages */
  failedCsv: string;
  accessToken: string;
  folderId?: string;
  spreadsheetId?: string;
}

/**
 * Write results using the contacts array as the source of truth.
 * Every record is guaranteed to appear in either Success or Errors — no gaps.
 * Salesforce Record IDs and error messages are merged in from the Bulk API CSVs.
 */
export async function createResultsSheetFromContacts(
  params: ContactResultsParams,
): Promise<{ url: string; spreadsheetId: string }> {
  const {
    contacts,
    externalIdField,
    failedExternalIds,
    successfulCsv,
    failedCsv,
    accessToken,
    folderId,
  } = params;

  const auth = { Authorization: `Bearer ${accessToken}` };

  if (contacts.length === 0) {
    return {
      url: params.spreadsheetId
        ? `https://docs.google.com/spreadsheets/d/${params.spreadsheetId}`
        : "",
      spreadsheetId: params.spreadsheetId ?? "",
    };
  }

  // ── Extract Salesforce metadata from result CSVs ──────────────────────────
  const sfIdMap = new Map<string, string>();
  const createdMap = new Map<string, boolean>();
  const errorMap = new Map<string, string>();

  if (successfulCsv.trim()) {
    const parsed = Papa.parse<Record<string, string>>(successfulCsv, {
      header: true,
      skipEmptyLines: true,
    });
    for (const row of parsed.data) {
      const extId = row[externalIdField];
      if (extId) {
        sfIdMap.set(extId, row.sf__Id ?? "");
        createdMap.set(extId, row.sf__Created === "true");
      }
    }
  }

  if (failedCsv.trim()) {
    const parsed = Papa.parse<Record<string, string>>(failedCsv, {
      header: true,
      skipEmptyLines: true,
    });
    for (const row of parsed.data) {
      const extId = row[externalIdField];
      if (extId) {
        errorMap.set(extId, row.sf__Error ?? "");
      }
    }
  }

  // ── Build rows from contacts (guaranteed complete — no missing records) ────
  // Union keys across every contact so a field absent from the first record
  // (e.g. set via setStr only when non-empty) still appears as a column.
  const allKeysSet = new Set<string>();
  for (const contact of contacts) {
    for (const key of Object.keys(contact)) {
      allKeysSet.add(key);
    }
  }
  const dataHeaders = Array.from(allKeysSet);
  const sHeaders = [...dataHeaders, "Salesforce Record ID", "Action"];
  const eHeaders = [...dataHeaders, "Error Message"];
  const uHeaders = [...dataHeaders];
  const sRows: string[][] = [];
  const eRows: string[][] = [];
  const uRows: string[][] = [];

  for (const contact of contacts) {
    const extId = String(contact[externalIdField] ?? "");
    const dataRow = dataHeaders.map((h) => {
      const v = contact[h];
      return v === null || v === undefined ? "" : String(v);
    });

    if (failedExternalIds.has(extId)) {
      eRows.push([...dataRow, errorMap.get(extId) ?? ""]);
    } else if (sfIdMap.has(extId)) {
      sRows.push([
        ...dataRow,
        sfIdMap.get(extId) ?? "",
        createdMap.get(extId) ? "Created" : "Updated",
      ]);
    } else {
      // Not in either Salesforce result CSV — separate tab so client can investigate
      uRows.push([...dataRow]);
    }
  }

  return writeRowsToResultsSheet(
    sHeaders,
    sRows,
    eHeaders,
    eRows,
    uHeaders,
    uRows,
    params.spreadsheetId,
    params.flowName,
    params.objectName,
    auth,
    folderId,
  );
}

export { describeAxiosError };

// ── Window tracking log ────────────────────────────────────────────────────────

const WINDOW_LOG_TAB = "Window Log";
const WINDOW_LOG_HEADERS = [
  "Window #",
  "Timestamp",
  "Start Byte",
  "End Byte",
  "Records In Window",
  "Successful",
  "Failed",
  "Skipped",
  "First ID",
  "Last ID",
];

export interface WindowLogEntry {
  windowNumber: number;
  timestamp: string;
  startByte: number;
  endByte: number;
  recordsInWindow: number;
  successful: number;
  failed: number;
  skipped: number;
  firstId: string;
  lastId: string;
}

/**
 * Appends one row to a "Window Log" tab in the given spreadsheet.
 * Creates the tab with headers on the first call.
 */
export async function appendWindowLog(
  spreadsheetId: string,
  accessToken: string,
  entry: WindowLogEntry,
): Promise<void> {
  const auth = { Authorization: `Bearer ${accessToken}` };
  const row = [
    entry.windowNumber,
    entry.timestamp,
    entry.startByte,
    entry.endByte,
    entry.recordsInWindow,
    entry.successful,
    entry.failed,
    entry.skipped,
    entry.firstId,
    entry.lastId,
  ];

  const tabTitles = await getTabTitles(spreadsheetId, auth);

  if (!tabTitles.includes(WINDOW_LOG_TAB)) {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}:batchUpdate`,
      { requests: [{ addSheet: { properties: { title: WINDOW_LOG_TAB } } }] },
      { headers: auth },
    );
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}/values:batchUpdate`,
      {
        valueInputOption: "RAW",
        data: [
          {
            range: `${WINDOW_LOG_TAB}!A1`,
            values: [WINDOW_LOG_HEADERS, row],
          },
        ],
      },
      { headers: auth },
    );
  } else {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}/values/${encodeURIComponent(WINDOW_LOG_TAB)}!A1:append`,
      { values: [row] },
      {
        params: { valueInputOption: "RAW", insertDataOption: "INSERT_ROWS" },
        headers: auth,
      },
    );
  }
}

// ── Skipped-row reporting ──────────────────────────────────────────────────────
// Rows dropped during TSV parsing (bad/missing ID, column count mismatch) never
// become a contact object, so they never reach createResultsSheetFromContacts —
// without this they'd vanish with no success/failure/not-processed trace at all.

const SKIPPED_TAB = "Skipped";

/**
 * Appends rows to a "Skipped" tab in the given spreadsheet, creating the tab
 * (with headers) on first use. No-ops when entries is empty.
 */
export async function appendSkippedRecords(
  spreadsheetId: string,
  accessToken: string,
  headers: string[],
  rows: string[][],
  tabName: string = SKIPPED_TAB,
): Promise<void> {
  if (rows.length === 0) return;
  const auth = { Authorization: `Bearer ${accessToken}` };

  const tabTitles = await getTabTitles(spreadsheetId, auth);

  if (!tabTitles.includes(tabName)) {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}:batchUpdate`,
      { requests: [{ addSheet: { properties: { title: tabName } } }] },
      { headers: auth },
    );
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}/values:batchUpdate`,
      {
        valueInputOption: "RAW",
        data: [{ range: `${tabName}!A1`, values: [headers, ...rows] }],
      },
      { headers: auth },
    );
  } else {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}/values/${encodeURIComponent(tabName)}!A1:append`,
      { values: rows },
      {
        params: { valueInputOption: "RAW", insertDataOption: "INSERT_ROWS" },
        headers: auth,
      },
    );
  }
}

// ── Per-object tab helpers ─────────────────────────────────────────────────────

/** Build a tab name for a given object + side + overflow index */
function objTabName(
  objectName: string,
  side: "Success" | "Errors",
  index: number,
): string {
  const base = `${objectName} - ${side}`;
  return index === 1 ? base : `${base} (${index})`;
}

/** Find the highest overflow index for a given object's tab pair */
function activeObjTabIndex(tabTitles: string[], objectName: string): number {
  let max = 1;
  const escaped = objectName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  for (const t of tabTitles) {
    const m = new RegExp(`^${escaped} - (?:Success|Errors) \\((\\d+)\\)$`).exec(
      t,
    );
    if (m) {
      const n = parseInt(m[1], 10);
      if (n > max) max = n;
    }
  }
  return max;
}

/**
 * Add a new numbered tab pair for one object and write headers + rows into them.
 */
async function addObjectTabPair(
  spreadsheetId: string,
  objectName: string,
  index: number,
  sHeaders: string[],
  sRows: string[][],
  eHeaders: string[],
  eRows: string[][],
  auth: { Authorization: string },
): Promise<void> {
  const sTab = objTabName(objectName, "Success", index);
  const eTab = objTabName(objectName, "Errors", index);

  await axios.post(
    `${SHEETS_BASE}/${spreadsheetId}:batchUpdate`,
    {
      requests: [
        { addSheet: { properties: { title: sTab } } },
        { addSheet: { properties: { title: eTab } } },
      ],
    },
    { headers: auth },
  );

  await axios.post(
    `${SHEETS_BASE}/${spreadsheetId}/values:batchUpdate`,
    {
      valueInputOption: "RAW",
      data: [
        {
          range: `${sTab}!A1`,
          values:
            sHeaders.length > 0 ? [sHeaders, ...sRows] : [["(no records)"]],
        },
        {
          range: `${eTab}!A1`,
          values:
            eHeaders.length > 0 ? [eHeaders, ...eRows] : [["(no records)"]],
        },
      ],
    },
    { headers: auth },
  );
}

// ── Per-object sheet main export ───────────────────────────────────────────────

/**
 * Create or append to a results sheet with one Success/Errors tab pair per object.
 * Tab names: "{ObjectName} - Success", "{ObjectName} - Errors" (overflow: "(2)", "(3)", …)
 * Returns the spreadsheet ID — pass it back on the next iteration via params.spreadsheetId.
 */
export async function createPerObjectResultsSheet(
  params: PerObjectReportParams,
): Promise<{ url: string; spreadsheetId: string }> {
  const { flowName, objects, accessToken, folderId } = params;
  const auth = { Authorization: `Bearer ${accessToken}` };

  // Parse CSVs up-front
  const parsed = objects
    .map((obj) => ({
      name: obj.objectName,
      success: parseResultCsv(obj.successfulCsv, "success"),
      error: parseResultCsv(obj.failedCsv, "error"),
    }))
    .filter((o) => o.success.rows.length > 0 || o.error.rows.length > 0);

  // ── Append to existing sheet (subsequent iterations) ─────────────────────
  if (params.spreadsheetId) {
    const sid = params.spreadsheetId;
    const tabTitles = await getTabTitles(sid, auth);

    for (const obj of parsed) {
      const baseExists = tabTitles.includes(`${obj.name} - Success`);

      if (!baseExists) {
        await addObjectTabPair(
          sid,
          obj.name,
          1,
          obj.success.headers,
          obj.success.rows,
          obj.error.headers,
          obj.error.rows,
          auth,
        );
      } else {
        const idx = activeObjTabIndex(tabTitles, obj.name);
        const sTab = objTabName(obj.name, "Success", idx);
        const eTab = objTabName(obj.name, "Errors", idx);

        const sCount = await getTabRowCount(sid, sTab, auth);
        const eCount = await getTabRowCount(sid, eTab, auth);

        if (
          sCount + obj.success.rows.length > MAX_ROWS_PER_TAB ||
          eCount + obj.error.rows.length > MAX_ROWS_PER_TAB
        ) {
          await addObjectTabPair(
            sid,
            obj.name,
            idx + 1,
            obj.success.headers,
            obj.success.rows,
            obj.error.headers,
            obj.error.rows,
            auth,
          );
        } else {
          const appends: Promise<unknown>[] = [];
          if (obj.success.rows.length > 0) {
            appends.push(
              axios.post(
                `${SHEETS_BASE}/${sid}/values/${encodeURIComponent(sTab)}!A1:append`,
                { values: obj.success.rows },
                {
                  params: {
                    valueInputOption: "RAW",
                    insertDataOption: "INSERT_ROWS",
                  },
                  headers: auth,
                },
              ),
            );
          }
          if (obj.error.rows.length > 0) {
            appends.push(
              axios.post(
                `${SHEETS_BASE}/${sid}/values/${encodeURIComponent(eTab)}!A1:append`,
                { values: obj.error.rows },
                {
                  params: {
                    valueInputOption: "RAW",
                    insertDataOption: "INSERT_ROWS",
                  },
                  headers: auth,
                },
              ),
            );
          }
          await Promise.all(appends);
        }
      }
    }

    return {
      url: `https://docs.google.com/spreadsheets/d/${sid}`,
      spreadsheetId: sid,
    };
  }

  // ── Create a new sheet (first iteration) ─────────────────────────────────
  const datetime = new Date().toISOString().slice(0, 16).replace("T", " ");
  const title = `Stanford CSP - ${flowName} - ${datetime}`;

  const { data: created } = await axios.post(
    SHEETS_BASE,
    { properties: { title } },
    { headers: auth },
  );
  const spreadsheetId = created.spreadsheetId as string;
  const defaultSheetId = (
    created.sheets as { properties: { sheetId: number } }[]
  )[0]?.properties?.sheetId;

  // Add all tab pairs in one batchUpdate
  const addRequests = parsed.flatMap((obj) => [
    { addSheet: { properties: { title: `${obj.name} - Success` } } },
    { addSheet: { properties: { title: `${obj.name} - Errors` } } },
  ]);

  if (addRequests.length > 0) {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}:batchUpdate`,
      { requests: addRequests },
      { headers: auth },
    );
  }

  // Write all data in one batchUpdate
  const writeData = parsed.flatMap((obj) => [
    {
      range: `${obj.name} - Success!A1`,
      values:
        obj.success.headers.length > 0
          ? [obj.success.headers, ...obj.success.rows]
          : [["(no records)"]],
    },
    {
      range: `${obj.name} - Errors!A1`,
      values:
        obj.error.headers.length > 0
          ? [obj.error.headers, ...obj.error.rows]
          : [["(no records)"]],
    },
  ]);

  if (writeData.length > 0) {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}/values:batchUpdate`,
      { valueInputOption: "RAW", data: writeData },
      { headers: auth },
    );
  }

  // Delete the default Sheet1
  if (defaultSheetId !== undefined) {
    await axios.post(
      `${SHEETS_BASE}/${spreadsheetId}:batchUpdate`,
      { requests: [{ deleteSheet: { sheetId: defaultSheetId } }] },
      { headers: auth },
    );
  }

  // Move to Drive folder
  if (folderId) {
    await axios
      .patch(
        `${DRIVE_FILES}/${spreadsheetId}`,
        {},
        {
          params: {
            addParents: folderId,
            removeParents: "root",
            supportsAllDrives: "true",
          },
          headers: auth,
        },
      )
      .catch(() => {
        /* non-critical */
      });
  }

  return {
    url: `https://docs.google.com/spreadsheets/d/${spreadsheetId}`,
    spreadsheetId,
  };
}
