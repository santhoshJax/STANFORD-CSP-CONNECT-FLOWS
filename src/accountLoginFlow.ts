/**
 * Stanford CSP Migration – Account Login Import flow.
 *
 * Source: Account Login TSV (4D) — one row per portal login credential.
 * Target: Salesforce User records for the Student community/portal profile.
 *
 * MUST run after Student Import — every row is resolved against an existing
 * Account (Student_ID_4D__c) to obtain the Contact, Email, First/Last Name
 * that the User record is built from. See "CSP Data Migration Mapping
 * Workbook - Account Login.pdf" for the field-by-field mapping this file
 * implements.
 *
 * Field mapping (source → target):
 *   Student_ID  → User.FederationIdentifier   (Direct Map)
 *   Student_ID  → User.ContactId              (Conversion — lookup Contact via
 *                                               the Account matching Student_ID_4D__c)
 *   Inactive    → User.IsActive               (see note below)
 *   —           → User.Username               (Conversion — Email on the matched Contact)
 *   —           → User.Email                  (Conversion — Email on the matched Contact)
 *   —           → User.FirstName / LastName   (Conversion — from the matched Contact)
 *   —           → User.Alias                  (Conversion — LOWER(LEFT(First,1) & LEFT(Last,7)))
 *   —           → User.ProfileId              (Direct Map — default "Student_Profile")
 *   —           → User.LocaleSidKey           (Direct Map — default "en_US")
 *   —           → User.LanguageLocaleKey      (Direct Map — default "en_US")
 *   —           → User.TimeZoneSidKey         (Direct Map — default "America/Los_Angeles")
 *   —           → User.EmailEncodingKey       (Direct Map — default "UTF-8")
 *
 * Do Not Map (per mapping doc): Instructor_ID, Associate_ID, ID, Last_Login_Date,
 * Password_Hash_Char, Created_Date/Time/By, Last_Modified_Date/Time/By,
 * Student_ID_Previous, Override_Hash_Storage/Start_Date/Start_Time/History,
 * Previous_Hash_Chars.
 *
 * Row-skip rules:
 *   1. No Student_ID, or Student_ID = 0 → skip (per mapping doc).
 *   2. Student_ID doesn't match any existing Account (Student_ID_4D__c) → skip
 *      (Student Import must run first; not explicitly called out in the doc
 *      but every other target field on this object depends on the match).
 *   3. Matched Contact has no email → skip (Username/Email are required,
 *      unique-format fields on User; can't create the record without one).
 *
 * NOTE — Active/Inactive (confirmed with architect): the source "Inactive"
 * flag IS the duplicate marker — 4D only ever sets it true when a row was
 * identified as a duplicate. So the field names being opposites is expected:
 * this flow inverts the value (IsActive = !Inactive) rather than copying it
 * literally. No separate duplicate-detection logic is needed — the source
 * flag already encodes it.
 *
 * PREREQUISITES (Salesforce setup, not covered by this flow):
 *   - User.FederationIdentifier must be usable as an upsert key (Salesforce
 *     natively supports upsert-by-FederationIdentifier for User).
 *   - A Profile named "Student_Profile" (DEFAULT_PROFILE_NAME below) must exist.
 */

import { flow } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import {
  str,
  toBool,
  cleanEmail,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
  SF_API_VERSION,
} from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 2000; // rows per execution window

const DEFAULT_PROFILE_NAME = "Student_Profile";
const DEFAULT_LOCALE_SID_KEY = "en_US";
const DEFAULT_LANGUAGE_LOCALE_KEY = "en_US";
const DEFAULT_TIME_ZONE_SID_KEY = "America/Los_Angeles";
const DEFAULT_EMAIL_ENCODING_KEY = "UTF-8";

// ── Raw TSV shape ──────────────────────────────────────────────────────────────

interface RawAccountLoginRecord {
  // Username is intentionally unused — per mapping doc, User.Username is
  // always derived from the matched Contact's email, not this column.
  Username?: string;
  Student_ID?: string;
  Inactive?: string;
  [key: string]: string | undefined;
}

// ── Intermediate row shape (pre Contact-lookup enrichment) ───────────────────

interface PendingLoginRow {
  studentId: string;
  isActive: boolean;
}

// ── Salesforce User shape ─────────────────────────────────────────────────────

interface SalesforceUserRecord {
  FederationIdentifier: string;
  ContactId: string;
  IsActive: boolean;
  FirstName?: string;
  LastName: string;
  Alias: string;
  Username: string;
  Email: string;
  ProfileId: string;
  LocaleSidKey: string;
  LanguageLocaleKey: string;
  TimeZoneSidKey: string;
  EmailEncodingKey: string;
}

interface StreamResult {
  rows: PendingLoginRow[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/** Alias formula from the mapping doc: LOWER(LEFT(FirstName,1) & LEFT(LastName,7)) */
function buildAlias(firstName: string, lastName: string): string {
  const first = firstName.trim().slice(0, 1);
  const last = lastName.trim().slice(0, 7);
  return (first + last).toLowerCase();
}

/** Resolves the Id of a Profile by name. Returns null if not found. */
async function resolveProfileId(
  instanceUrl: string,
  accessToken: string,
  profileName: string,
): Promise<string | null> {
  const soql = `SELECT Id FROM Profile WHERE Name = '${profileName.replace(/'/g, "\\'")}' LIMIT 1`;
  const { data } = await axios.get(
    `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
    {
      params: { q: soql },
      headers: { Authorization: `Bearer ${accessToken}` },
    },
  );
  const records = data.records as { Id: string }[];
  return records[0]?.Id ?? null;
}

interface StudentContactInfo {
  contactId: string;
  firstName: string;
  lastName: string;
  email: string;
}

/**
 * Given a list of Student_ID values, returns { studentId → matched Contact info }
 * resolved from the person Account created by Student Import.
 */
async function resolveContactsByStudentId(
  instanceUrl: string,
  accessToken: string,
  studentIds: string[],
): Promise<Map<string, StudentContactInfo>> {
  if (studentIds.length === 0) return new Map();

  const CHUNK_SIZE = 200;
  const map = new Map<string, StudentContactInfo>();
  const headers = { Authorization: `Bearer ${accessToken}` };

  for (let i = 0; i < studentIds.length; i += CHUNK_SIZE) {
    const chunk = studentIds.slice(i, i + CHUNK_SIZE);
    const inClause = chunk
      .map((id) => `'${id.replace(/'/g, "\\'")}'`)
      .join(",");
    const soql =
      `SELECT Student_ID_4D__c, PersonContactId, FirstName, LastName, PersonEmail ` +
      `FROM Account WHERE IsPersonAccount = true AND Student_ID_4D__c IN (${inClause})`;
    const { data } = await axios.get(
      `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      { params: { q: soql }, headers },
    );
    for (const rec of data.records as {
      Student_ID_4D__c: string;
      PersonContactId: string;
      FirstName: string;
      LastName: string;
      PersonEmail: string;
    }[]) {
      if (rec.Student_ID_4D__c && rec.PersonContactId) {
        map.set(rec.Student_ID_4D__c, {
          contactId: rec.PersonContactId,
          firstName: rec.FirstName ?? "",
          lastName: rec.LastName ?? "",
          email: rec.PersonEmail ?? "",
        });
      }
    }
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
): Promise<StreamResult> {
  const reqHeaders: Record<string, string> = {
    Authorization: `Bearer ${accessToken}`,
  };
  if (byteOffset > 0) reqHeaders.Range = `bytes=${byteOffset}-`;

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
    const rows: PendingLoginRow[] = [];
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

        if (rows.length >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        if (row.length !== parsedHeaders.length) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const record: RawAccountLoginRecord = {};
        parsedHeaders.forEach((header, i) => {
          record[header] = row[i] ?? "";
        });

        // Skip rule #1 (mapping doc): no Student_ID, or Student_ID = 0.
        // /^[1-9]\d*$/ rejects blank, non-numeric, and "0"/"00" style values.
        const studentId = str(record.Student_ID);
        if (!/^[1-9]\d*$/.test(studentId)) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        // Inverted — see "NOTE — Active/Inactive" in the file header comment.
        const isActive = !toBool(record.Inactive);

        if (!firstId) firstId = studentId;
        lastId = studentId;
        rows.push({ studentId, isActive });
        lastCompletedCursor = rowEndByte;
      },

      complete: () =>
        resolve({
          rows,
          hasMore: aborted,
          nextByteOffset: lastCompletedCursor,
          parsedHeaders,
          firstId,
          lastId,
          skippedCount,
        }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Flow ───────────────────────────────────────────────────────────────────────

export const accountLoginImport = flow({
  name: "Account Login Import",
  stableKey: "7c1d9e2a-4b3f-4a6c-9d1e-2f3a4b5c6d7e",
  description:
    "Streams the Account Login TSV from Google Drive, resolves each Student_ID " +
    "against the Account created by Student Import, and upserts Salesforce User " +
    "records (FederationIdentifier as the upsert key) via Bulk API 2.0. Must run " +
    "after Student Import. Recurses until the full file is processed.",

  onTrigger: async (_context, payload) => {
    await Promise.resolve();
    return { payload };
  },

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    // ── 1. Read cursor (byteOffset) from trigger payload ──────────────────────
    const triggerBody = (
      params.onTrigger.results as unknown as
        { body?: { data?: unknown } } | undefined
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
    const sheetId =
      typeof triggerBody?.sheetId === "string"
        ? triggerBody.sheetId
        : undefined;

    logger.info(`[Account Login Import] Starting at byte offset ${byteOffset}`);

    // ── 2. Resolve connections and file ID ─────────────────────────────────────
    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = configVars["Account Login File ID"] as string | undefined;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      string | undefined;

    if (!fileId) throw new Error("Account Login File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // ── 3. Stream & parse the TSV window from Google Drive ─────────────────────
    logger.info(
      `[Account Login Import] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
    );
    const {
      rows,
      hasMore,
      nextByteOffset,
      parsedHeaders,
      firstId,
      lastId,
      skippedCount,
    } = await streamAndParseTsv(
      fileId,
      gdToken,
      byteOffset,
      MAX_ROWS,
      knownHeaders,
    );
    logger.info(
      `[Account Login Import] Parsed ${rows.length} rows` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    let nextSheetId = sheetId;

    if (rows.length > 0) {
      // ── 4. Resolve the Profile once per window ────────────────────────────
      const profileId = await resolveProfileId(
        sfInstanceUrl,
        sfToken,
        DEFAULT_PROFILE_NAME,
      );
      if (!profileId) {
        throw new Error(
          `[Account Login Import] No Profile named "${DEFAULT_PROFILE_NAME}" was found in this org.`,
        );
      }

      // ── 5. Resolve Student_ID → Contact/Email/Name via the migrated Account ─
      const studentIds = [...new Set(rows.map((r) => r.studentId))];
      const contactsByStudentId = await resolveContactsByStudentId(
        sfInstanceUrl,
        sfToken,
        studentIds,
      );

      let droppedNoContact = 0;
      let droppedNoEmail = 0;
      const records: SalesforceUserRecord[] = [];

      for (const row of rows) {
        const contact = contactsByStudentId.get(row.studentId);
        if (!contact) {
          droppedNoContact++;
          continue;
        }

        const email = cleanEmail(contact.email);
        if (!email) {
          droppedNoEmail++;
          continue;
        }

        records.push({
          FederationIdentifier: row.studentId,
          ContactId: contact.contactId,
          IsActive: row.isActive,
          FirstName: contact.firstName || undefined,
          LastName: contact.lastName,
          Alias: buildAlias(contact.firstName, contact.lastName),
          Username: email,
          Email: email,
          ProfileId: profileId,
          LocaleSidKey: DEFAULT_LOCALE_SID_KEY,
          LanguageLocaleKey: DEFAULT_LANGUAGE_LOCALE_KEY,
          TimeZoneSidKey: DEFAULT_TIME_ZONE_SID_KEY,
          EmailEncodingKey: DEFAULT_EMAIL_ENCODING_KEY,
        });
      }

      if (droppedNoContact > 0) {
        logger.warn(
          `[Account Login Import] ${droppedNoContact} row(s) had no matching Account for their Student_ID — skipped. Run Student Import first.`,
        );
      }
      if (droppedNoEmail > 0) {
        logger.warn(
          `[Account Login Import] ${droppedNoEmail} row(s) matched a Contact with no usable email — skipped (Username/Email are required).`,
        );
      }

      // ── 6. Upsert to Salesforce Bulk API 2.0 ──────────────────────────────
      if (records.length > 0) {
        const jobResult = await runBulkJob(
          sfInstanceUrl,
          sfToken,
          "User",
          "FederationIdentifier",
          records as unknown as Record<string, unknown>[],
          logger,
          "[Account Login Import]",
        );

        try {
          const sheet = await createResultsSheetFromContacts({
            flowName: "Account Login Import",
            objectName: "User",
            contacts: records as unknown as Record<string, unknown>[],
            externalIdField: "FederationIdentifier",
            failedExternalIds: jobResult.failedExternalIds,
            successfulCsv: jobResult.successfulCsv,
            failedCsv: jobResult.failedCsv,
            accessToken: gdToken,
            folderId: failedFolderId,
            spreadsheetId: sheetId,
          });
          nextSheetId = sheet.spreadsheetId;
          logger.info(`[Account Login Import] Results sheet: ${sheet.url}`);
        } catch (err: unknown) {
          logger.warn(
            `[Account Login Import] Could not update results sheet: ${String(err)}`,
          );
        }
      } else {
        logger.info(
          "[Account Login Import] Window contained no upsertable records after Contact resolution.",
        );
      }
    } else {
      logger.info(
        "[Account Login Import] Window contained no records; skipping upload.",
      );
    }

    // ── 7. Recurse if more rows remain ─────────────────────────────────────────
    if (hasMore) {
      logger.info(
        `[Account Login Import] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Account Login Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(
        "[Account Login Import] All rows processed — import complete.",
      );
    }

    return {
      data: {
        byteOffset,
        rowsProcessed: rows.length,
        hasMore,
      },
    };
  },
});

export default [accountLoginImport];
