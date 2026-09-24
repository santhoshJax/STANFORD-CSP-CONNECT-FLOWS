/**
 * Stanford CSP Migration – Enrollment Waiver Import flow.
 *
 * Streams the Enrollment_Waiver TSV from Google Drive one window (MAX_ROWS rows) at a time.
 * For each row:
 *   1. Resolves the parent CourseOfferingParticipant by matching Enrollment_ID to
 *      CourseOfferingParticipant.External_ID_4D__c (set by the Enrollment flow).
 *   2. Denormalises the Waiver name by stamping "Waiver.Name (Waiver.ID)" as text
 *      using an optional Waiver reference file (15 definitions).
 *   3. Parses Date_Time_New from YYYYMMDDHHMMSS into Salesforce Date/Time.
 *   4. Upserts Course_Offering_Participant_Waiver__c records via REST API on External_ID_4D__c.
 *
 * Prerequisites:
 *   - The Enrollment flow must have run first and loaded CourseOfferingParticipant
 *     records with External_ID_4D__c = "ENR-{Enrollment.ID}" (e.g. "ENR-200058").
 *
 * Field mapping (per architect workbook, Jun 17):
 *   ID            → External_ID_4D__c      (Direct Map)
 *   Enrollment_ID → Enrollment_ID__c       (Lookup → COP via "ENR-{Enrollment_ID}")
 *   Waiver_ID     → Historical_Waiver__c   (Denormalize: "Waiver.Name (Waiver.ID)")
 *   Date_Time_New → Date_Time_New__c       (Parse YYYYMMDDHHMMSS → DateTime)
 *   Verification  → Verification__c        (Direct Map — contains IP; load as-is per workbook)
 *   Option        → Option__c              (Direct Map — all values are "ACCEPT")
 *
 * Open questions (flagged in workbook):
 *   - Verification: business decision pending on whether to load IP addresses as-is.
 *   - Date_Time_New: source timezone not specified; stored as-is (no UTC offset applied).
 */
import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse, unparse as papaUnparse } from "papaparse";
import { str, getAccessToken, getSfInstanceUrl } from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// ── Constants ─────────────────────────────────────────────────────────────────
const MAX_ROWS = 500;
const SF_API = "v60.0";
const SF_OBJECT = "Course_Offering_Participant_Waiver__c";

// ── Salesforce REST helpers ───────────────────────────────────────────────────

function authHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}

function readHeaders(token: string): Record<string, string> {
  return { Authorization: `Bearer ${token}` };
}

async function sfUpsert(
  base: string,
  token: string,
  object: string,
  extIdField: string,
  extIdValue: string,
  payload: Record<string, unknown>,
): Promise<{ id: string; created: boolean }> {
  const url = `${base}/services/data/${SF_API}/sobjects/${object}/${extIdField}/${encodeURIComponent(extIdValue)}`;
  const { data, status } = await axios.patch<{
    id?: string;
    created?: boolean;
  }>(url, payload, {
    headers: authHeaders(token),
    validateStatus: (s) => s < 500,
  });
  if (status === 201) return { id: data.id!, created: true };
  if (status === 200) return { id: data.id!, created: data.created ?? false };
  if (status === 204) {
    const { data: rec } = await axios.get<{ Id: string }>(
      `${base}/services/data/${SF_API}/sobjects/${object}/${extIdField}/${encodeURIComponent(extIdValue)}`,
      { params: { fields: "Id" }, headers: readHeaders(token) },
    );
    return { id: rec.Id, created: false };
  }
  throw new Error(`Upsert ${object} HTTP ${status}: ${JSON.stringify(data)}`);
}

async function buildFieldCache(
  base: string,
  token: string,
  soql: string,
  keyField: string,
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  let nextUrl: string | null = null;
  let done = false;

  const fetchPage = async (url: string | null) => {
    const { data } = await axios.get<{
      records: Record<string, string>[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${base}/services/data/${SF_API}/query`,
      url
        ? { headers: readHeaders(token) }
        : { params: { q: soql }, headers: readHeaders(token) },
    );
    for (const r of data.records) {
      if (r[keyField]) map.set(r[keyField], r.Id);
    }
    done = data.done;
    nextUrl = data.nextRecordsUrl ? `${base}${data.nextRecordsUrl}` : null;
  };

  await fetchPage(null);
  while (!done && nextUrl) await fetchPage(nextUrl);
  return map;
}

function sfErrMsg(err: unknown): string {
  const e = err as {
    response?: { status?: number; data?: unknown };
    message?: string;
  };
  if (e.response?.data) {
    const d = e.response.data;
    if (Array.isArray(d)) {
      return (
        `HTTP ${e.response.status} — ` +
        (d as { errorCode?: string; message?: string }[])
          .map((r) => [r.errorCode, r.message].filter(Boolean).join(": "))
          .join("; ")
      );
    }
    return `HTTP ${e.response.status} — ${JSON.stringify(d)}`;
  }
  return e.message ?? String(err);
}

// ── Date/Time parser ──────────────────────────────────────────────────────────
// Source format: YYYYMMDDHHMMSS (14-character numeric string, e.g. "20151130083040")
// Note: source timezone is unknown; value is stored without UTC offset adjustment.
function parseDateTime4D(raw: string | undefined): string | null {
  const s = str(raw).trim();
  if (s.length !== 14 || !/^\d{14}$/.test(s)) return null;
  const year   = s.slice(0, 4);
  const month  = s.slice(4, 6);
  const day    = s.slice(6, 8);
  const hour   = s.slice(8, 10);
  const minute = s.slice(10, 12);
  const second = s.slice(12, 14);
  return `${year}-${month}-${day}T${hour}:${minute}:${second}.000Z`;
}

// ── Waiver reference loader ───────────────────────────────────────────────────
// Expects a tab-delimited file with columns: ID, Name
// Returns Map<waiver_id, waiver_name> (15 entries expected)

interface WaiverRefRow {
  ID?: string;
  Name?: string;
  [key: string]: string | undefined;
}

async function loadWaiverRefMap(
  fileId: string,
  accessToken: string,
): Promise<Map<string, string>> {
  const { data } = await axios.get<string>(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "text",
    },
  );
  const content = data.replace(/^﻿/, "");
  const { data: rows } = papaParse<WaiverRefRow>(content, {
    header: true,
    skipEmptyLines: true,
    delimiter: "\t",
  });
  const map = new Map<string, string>();
  for (const row of rows) {
    const id = row.ID?.trim();
    const name = row.Name?.trim();
    if (id && name) map.set(id, name);
  }
  return map;
}

// ── Raw row type ──────────────────────────────────────────────────────────────

interface RawWaiverRow {
  ID?: string;
  Enrollment_ID?: string;
  Waiver_ID?: string;
  Date_Time_New?: string;
  Verification?: string;
  Option?: string;
  [key: string]: string | undefined;
}

// ── Result rows ───────────────────────────────────────────────────────────────

interface SuccessRow {
  Source_ID: string;
  Enrollment_ID: string;
  Object: string;
  sf__Id: string;
  sf__Created: string;
}

interface ErrorRow {
  Source_ID: string;
  Enrollment_ID: string;
  Object: string;
  sf__Error: string;
}

// ── TSV streaming ─────────────────────────────────────────────────────────────

interface StreamResult {
  rows: RawWaiverRow[];
  hasMore: boolean;
  nextStartRow: number;
}

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  startRow: number,
  maxRows: number,
): Promise<StreamResult> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    let headers: string[] = [];
    let dataRowIndex = 0;
    const rows: RawWaiverRow[] = [];
    let aborted = false;

    papaParse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      skipEmptyLines: true,

      step: (result: Papa.ParseResult<string[]>, parser: Papa.Parser) => {
        if (aborted) return;
        const raw = result.data as unknown as string[];

        if (headers.length === 0) {
          headers = raw.map(
            (h, i) =>
              h
                .replace(/^﻿/, "")
                .replace(/\r/g, "")
                .trim() || `__blank_${i}`,
          );
          return;
        }

        if (dataRowIndex < startRow) {
          dataRowIndex++;
          return;
        }

        if (rows.length >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        const record: RawWaiverRow = {};
        headers.forEach((header, i) => {
          if (!header.startsWith("__blank_")) {
            record[header] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        if (!str(record.ID).trim()) {
          dataRowIndex++;
          return;
        }

        rows.push(record);
        dataRowIndex++;
      },

      complete: () =>
        resolve({ rows, hasMore: aborted, nextStartRow: dataRowIndex }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const enrollmentWaiverImport = flow({
  name: "Enrollment Waiver Import",
  stableKey: "ew112233-4455-4c67-8899-aabbccddeeff",
  description:
    "Streams the Enrollment_Waiver TSV from Google Drive and upserts COP_Waiver__c records " +
    "to Salesforce via REST API. Resolves parent CourseOfferingParticipant by Enrollment_ID. " +
    "Must run after the Enrollment flow.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    const triggerBody = (
      params.onTrigger.results as unknown as
        | { body?: { data?: unknown } }
        | undefined
    )?.body?.data as Record<string, unknown> | undefined;

    const startRow =
      typeof triggerBody?.startRow === "number" ? triggerBody.startRow : 0;
    const sheetId =
      typeof triggerBody?.sheetId === "string"
        ? triggerBody.sheetId
        : undefined;

    logger.info(`[Enrollment Waiver Import] Starting at row ${startRow}`);

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars[
      "Google Drive Connection"
    ] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars[
      "Enrollment Waiver File ID"
    ] as unknown as string;
    const waiverRefFileId = configVars[
      "Waiver Reference File ID"
    ] as unknown as string | undefined;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      | string
      | undefined;

    if (!fileId) throw new Error("Enrollment Waiver File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── Build COP cache ───────────────────────────────────────────────────────
    // Enrollment COPs carry External_ID_4D__c = "ENR-{Enrollment.ID}" (set by the
    // Enrollment flow). Instructor/Associate COPs use CINST-/CASSOC- prefixes
    // so they will not collide with ENR- lookups.
    logger.info(
      "[Enrollment Waiver Import] Building CourseOfferingParticipant cache…",
    );
    const copCache = await buildFieldCache(
      sfBase,
      sfToken,
      "SELECT Id, External_ID_4D__c FROM CourseOfferingParticipant WHERE External_ID_4D__c != null",
      "External_ID_4D__c",
    );
    logger.info(
      `[Enrollment Waiver Import] COP cache: ${copCache.size} records`,
    );

    // ── Load Waiver reference map (15 entries) ────────────────────────────────
    // Tab-delimited file with columns ID, Name.
    // Waiver table is marked "Do Not Migrate" so we denormalise the name as text.
    let waiverRefMap = new Map<string, string>();
    if (waiverRefFileId) {
      try {
        waiverRefMap = await loadWaiverRefMap(waiverRefFileId, gdToken);
        logger.info(
          `[Enrollment Waiver Import] Waiver reference map: ${waiverRefMap.size} entries`,
        );
      } catch (err) {
        logger.warn(
          `[Enrollment Waiver Import] Could not load Waiver reference file: ${String(err)}` +
            " — Historical_Waiver__c will contain only the Waiver ID",
        );
      }
    } else {
      logger.warn(
        "[Enrollment Waiver Import] Waiver Reference File ID not configured" +
          " — Historical_Waiver__c will contain only the Waiver ID",
      );
    }

    // ── Stream TSV window ─────────────────────────────────────────────────────
    logger.info(
      `[Enrollment Waiver Import] Streaming rows ${startRow}–${startRow + MAX_ROWS - 1}…`,
    );
    const { rows, hasMore, nextStartRow } = await streamAndParseTsv(
      fileId,
      gdToken,
      startRow,
      MAX_ROWS,
    );
    logger.info(
      `[Enrollment Waiver Import] Parsed ${rows.length} rows (hasMore=${hasMore})`,
    );

    // ── Counters ──────────────────────────────────────────────────────────────
    const counts = { ok: 0, skipped: 0, err: 0 };
    const successRows: SuccessRow[] = [];
    const errorRows: ErrorRow[] = [];

    // ── Row loop ──────────────────────────────────────────────────────────────
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const sourceId = str(row.ID).trim();
      const enrollmentId = str(row.Enrollment_ID).trim();

      if (!sourceId) continue;

      const ok = (sfId: string, created: boolean) =>
        successRows.push({
          Source_ID: sourceId,
          Enrollment_ID: enrollmentId,
          Object: SF_OBJECT,
          sf__Id: sfId,
          sf__Created: String(created),
        });

      const fail = (error: string) =>
        errorRows.push({
          Source_ID: sourceId,
          Enrollment_ID: enrollmentId,
          Object: SF_OBJECT,
          sf__Error: error,
        });

      // Resolve parent CourseOfferingParticipant.
      // Enrollment flow sets External_ID_4D__c = "ENR-{Enrollment.ID}" so we
      // must prepend the same prefix when looking up by Enrollment_Waiver.Enrollment_ID.
      const copSfId = enrollmentId
        ? copCache.get(`ENR-${enrollmentId}`)
        : undefined;
      if (!copSfId) {
        const msg = `COP not found for Enrollment_ID="${enrollmentId}"`;
        logger.warn(
          `[Enrollment Waiver Import] Row ${startRow + i} (${sourceId}): ${msg}`,
        );
        fail(msg);
        counts.skipped++;
        continue;
      }

      // Historical_Waiver__c: "Waiver.Name (Waiver.ID)"
      const waiverId = str(row.Waiver_ID).trim();
      let historicalWaiver: string | undefined;
      if (waiverId) {
        const waiverName = waiverRefMap.get(waiverId);
        historicalWaiver = waiverName ? `${waiverName} (${waiverId})` : waiverId;
      }

      // Parse Date_Time_New (YYYYMMDDHHMMSS → ISO DateTime)
      const dateTimeNew = parseDateTime4D(row.Date_Time_New);
      if (row.Date_Time_New && !dateTimeNew) {
        logger.warn(
          `[Enrollment Waiver Import] Row ${startRow + i} (${sourceId})` +
            ` Date_Time_New "${row.Date_Time_New}" is not valid YYYYMMDDHHMMSS — field skipped`,
        );
      }

      // Build payload
      const payload: Record<string, unknown> = {
        Enrollment_ID__c: copSfId,
        Option__c: str(row.Option).trim() || "ACCEPT",
      };
      if (historicalWaiver) payload.Historical_Waiver__c = historicalWaiver;
      if (dateTimeNew) payload.Date_Time_New__c = dateTimeNew;
      const verification = str(row.Verification).trim();
      if (verification) payload.Verification__c = verification;

      try {
        const r = await sfUpsert(
          sfBase,
          sfToken,
          SF_OBJECT,
          "External_ID_4D__c",
          sourceId,
          payload,
        );
        counts.ok++;
        ok(r.id, r.created);
      } catch (err) {
        counts.err++;
        const msg = sfErrMsg(err);
        logger.error(
          `[Enrollment Waiver Import] Row ${startRow + i} (${sourceId}): ${msg}`,
        );
        fail(msg);
      }
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    let nextSheetId = sheetId;
    if (successRows.length > 0 || errorRows.length > 0) {
      try {
        const sheet = await createPerObjectResultsSheet({
          flowName: "Enrollment Waiver Import",
          objects: [
            {
              objectName: SF_OBJECT,
              successfulCsv:
                successRows.length > 0
                  ? papaUnparse(
                      successRows as unknown as Record<string, unknown>[],
                      { newline: "\n" },
                    )
                  : "",
              failedCsv:
                errorRows.length > 0
                  ? papaUnparse(
                      errorRows as unknown as Record<string, unknown>[],
                      { newline: "\n" },
                    )
                  : "",
            },
          ],
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: sheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Enrollment Waiver Import] Results sheet: ${sheet.url}`);
      } catch (err) {
        logger.warn(
          `[Enrollment Waiver Import] Could not update results sheet: ${String(err)}`,
        );
      }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Enrollment Waiver Import] Window summary (rows ${startRow}–${nextStartRow - 1}):` +
        `\n  ${SF_OBJECT}: ${counts.ok} ok, ${counts.skipped} skip (COP not found), ${counts.err} err`,
    );

    // ── Recurse ───────────────────────────────────────────────────────────────
    if (hasMore) {
      logger.info(
        `[Enrollment Waiver Import] Invoking next iteration at startRow=${nextStartRow}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Enrollment Waiver Import", {
        startRow: nextStartRow,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(
        "[Enrollment Waiver Import] All rows processed — import complete.",
      );
    }

    return {
      data: { startRow, rowsProcessed: rows.length, hasMore, counts },
    };
  },
});

export default [enrollmentWaiverImport];
