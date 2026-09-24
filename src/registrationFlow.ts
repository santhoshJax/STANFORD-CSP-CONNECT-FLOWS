/**
 * Stanford CSP Migration – Registration Import flow.
 *
 * Streams the Registration TSV from Google Drive and bulk-upserts FOUR
 * Salesforce objects from each source row:
 *   1. CardPaymentMethod — stored card details (billing name/address, card
 *      type, last four, expiry).
 *   2. Order             — the registration/order record itself (billing
 *      address, amounts, dates, discount/TA info, notes).
 *   3. PaymentGroup       — one per Order, created after Order so it can
 *      point SourceObjectId at the real Order Id. Upserted on
 *      External_ID_4D__c = Registration.ID like everything else, which
 *      satisfies the workbook's "if a PaymentGroup already exists for this
 *      Order, reuse it" rule for free (a re-run just updates the same row
 *      instead of creating a duplicate). PaymentNumber is an AutoNumber —
 *      Salesforce assigns it, nothing is mapped to it.
 *   4. Payment            — a single Capture/Processed payment record per
 *      registration (amount, effective date, payee, PaymentGroupId).
 *
 * Runs in 4 phases because of the PaymentGroup dependency chain: Order must
 * exist before PaymentGroup can reference it (SourceObjectId), and
 * PaymentGroup must exist before Payment can reference it (PaymentGroupId).
 * CardPaymentMethod has no such dependency and could run anywhere, but stays
 * first to match the workbook's field order. Real Salesforce Ids are pulled
 * out of each phase's Bulk API successfulResults CSV (sf__Id column) and
 * matched back to source rows via External_ID_4D__c — no extra SOQL queries
 * needed for this chaining.
 *
 * Source: "CSP Data Migration Mapping Workbook - Registration.pdf" (architect
 * mapping doc, updated with the PaymentGroup/Order.Status addendum). Field-
 * by-field mapping below follows that workbook's "Map Type" column: "Direct
 * Map" rows are passed through with only type coercion (currency/date
 * parsing); "Conversion" rows apply the specific logic the workbook calls
 * out in its Notes column. Anything the workbook marks "Do Not Map" is
 * intentionally excluded (Batch_Print, Created_By, Prior_Registration_ID,
 * Reconciliation_ID, Student_Country, Student_ID_Previous) — see exception
 * for audit fields below.
 *
 * PREREQUISITES (must run before this flow):
 *   Student flow — loads Person Account / Contact, both carrying
 *   Student_ID_4D__c (confirmed present on BOTH Account and Contact in this
 *   org). Because of that, AccountId and BillToContactId are resolved by
 *   Salesforce itself during the Bulk API job via external-ID relationship
 *   syntax (`Account.Student_ID_4D__c` / `BillToContact.Student_ID_4D__c`)
 *   — no pre-flight SOQL query needed for either. Only Payment.GroupPayee
 *   (an actual email string, not a lookup) still needs a real value fetched
 *   ahead of time — see "Student email cache" below.
 *
 * Upsert key: External_ID_4D__c = Registration.ID, set on all three objects
 *   (no prefix needed — each lives in its own object namespace, unlike the
 *   Enrollment flow's CourseOfferingParticipant which shares a namespace
 *   with instructor/associate junction rows).
 *
 * Student email cache: Salesforce's external-ID relationship trick only
 *   resolves lookup fields (a value copied into a Text field, like
 *   GroupPayee, is out of scope for it — Bulk API can't do that during
 *   ingest). This flow still runs one SOQL query
 *   (Student_ID_4D__c → PersonEmail only, ~190k Account rows / ~95 pages of
 *   2,000) but fires it in parallel with the TSV streaming/mapping step via
 *   Promise.all, instead of blocking on it first — the two no longer have
 *   any dependency on each other. GroupPayee is stitched onto the already-
 *   mapped Payment records once both finish. Each page is logged as it
 *   comes in so this step is never silently stuck.
 *
 * Compound address fields — the workbook lists target API names using
 * dotted "compound field" notation (e.g. "PaymentMethodAddress.street",
 * "BillingAddress.street"), which describes the field *grouping* in Setup
 * but is not what Bulk API 2.0 accepts on ingest (dotted names in a Bulk
 * API CSV/JSON payload mean "traverse this relationship", not "set this
 * compound sub-field"). This flow submits the actual flat, writable
 * component field names instead:
 *   CardPaymentMethod : Street, City, State, PostalCode, CountryCode
 *   Order              : BillingStreet, BillingCity, BillingState (via
 *                        BillingStateCode where the org's field is a
 *                        picklist), BillingPostalCode, BillingCountryCode
 *   ⚠ Verify these exact API names in the target org before first run —
 *     not independently confirmed against org metadata from this repo.
 *
 * OPEN ITEMS (need architect/SA confirmation before production run):
 *   - Order.Status — RESOLVED (addendum): defaults to "Activated" per the
 *     architect's update. (Order.EffectiveDate — RESOLVED: this org's Order
 *     object has no such field; EffectiveDate only exists on Payment,
 *     already mapped there per the workbook.)
 *   - Payment.PaymentGroupId — RESOLVED (addendum): the architect's update
 *     specifies Payment.PaymentGroupId → PaymentGroup, created per-Order
 *     via PaymentGroup.SourceObjectId = Order.Id, reused on re-run. (An
 *     earlier manual test showed PaymentGroup isn't strictly *required* to
 *     save a Payment in this org, but the workbook now asks for it to be
 *     populated regardless, so this flow does.)
 *   - PaymentGroup.External_ID_4D__c — ASSUMPTION, not in the workbook: the
 *     workbook doesn't map an upsert key for PaymentGroup at all, only
 *     describing "reuse if one already exists for this Order" in prose.
 *     This flow gives PaymentGroup its own External_ID_4D__c =
 *     Registration.ID (same convention as every other object here), which
 *     satisfies that reuse requirement via ordinary upsert semantics
 *     without a separate existence-check query. Confirm PaymentGroup
 *     actually has that custom field in the target org — if not, this
 *     needs to change to a SOQL existence-check on SourceObjectId instead.
 *   - PaymentGroup — confirm no other fields are required to save one
 *     beyond SourceObjectId (mirrors the earlier Order.Status surprise —
 *     the workbook only mentions SourceObjectId and the auto-generated
 *     PaymentNumber).
 *   - Payment.GroupPayee — RESOLVED: confirmed as a real custom field on
 *     Payment, GroupPayee__c (Text(255)), populated with the student's
 *     email. (Workbook listed it as "GroupPayee" with no "__c" — the
 *     actual API name has the custom-field suffix.)
 *   - CC_Transaction_ID → Order.OrderReferenceNumber is marked
 *     "Conversion" in the workbook but no logic is described — this flow
 *     passes the raw value through unchanged.
 *   - Check_No → Order.Check_Number__c is typed "Number" in the workbook;
 *     non-numeric values (if any exist in source data) are skipped with a
 *     warning rather than silently truncated.
 *   - Audit fields (CreatedDate/LastModifiedDate on Order): workbook marks
 *     Created_By/Date/Time and Last_Modified_By/Date/Time "Do Not Map",
 *     but per the same precedent set in the Enrollment flow, this flow
 *     still writes them to the standard CreatedDate/LastModifiedDate
 *     fields (requires "Set Audit Fields upon Record Creation" / "Create
 *     Audit Fields" permission for the migration user). Remove if that
 *     precedent doesn't apply here.
 *   - External_Id_4D field: workbook writes "External_Id_4D" (no object
 *     given on that row — appears to apply to all three target objects).
 *     This flow uses the project-wide convention External_ID_4D__c;
 *     confirm that custom field exists on CardPaymentMethod, Order, and
 *     Payment in the target org.
 *   - Relationship names assumed for the external-ID lookup trick:
 *     `Account.Student_ID_4D__c` for AccountId and
 *     `BillToContact.Student_ID_4D__c` for BillToContactId (Salesforce's
 *     auto-generated relationship name for a standard "...Id" field is the
 *     field name minus "Id" — Account for AccountId, BillToContact for
 *     BillToContactId). Confirm both `Student_ID_4D__c` fields are flagged
 *     "External ID" in Setup — Bulk API will reject the relationship
 *     reference outright (a clear per-batch error, not a hang) if not.
 *     A row whose Student_ID doesn't match any Account/Contact now fails
 *     silently into the Bulk API failed-records CSV (visible in the
 *     results sheet) rather than a pre-check warning in the flow log,
 *     since there's no local cache to check against anymore.
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, { parse as papaParse } from "papaparse";
import {
  str,
  toDate,
  toSalesforceCountryCode,
  toSalesforceStateCode,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
} from "./utils";
import { createPerObjectResultsSheet } from "./reportResults";

// ── Constants ─────────────────────────────────────────────────────────────────
const BULK_BATCH_SIZE = 50_000;
const EXT_ID_FIELD = "External_ID_4D__c";
const SF_CARD_PAYMENT_METHOD = "CardPaymentMethod";
const SF_ORDER = "Order";
const SF_PAYMENT_GROUP = "PaymentGroup";
const SF_PAYMENT = "Payment";
const TEST_MODE = true; // set to false to process all records
const TEST_LIMIT = 100; // max records (per object) to upsert when TEST_MODE is true

// Order.Status default — per architect addendum, see OPEN ITEMS above.
const DEFAULT_ORDER_STATUS = "Activated";

// ── Raw row type ──────────────────────────────────────────────────────────────

interface RawRegistrationRow {
  ID?: string;
  Student_ID?: string;
  CC_Billing_Address?: string;
  CC_Billing_City?: string;
  CC_Billing_Country?: string;
  CC_Billing_State?: string;
  CC_Billing_Zip?: string;
  CC_Card_Type?: string;
  CC_Exp_Date?: string;
  CC_Last_Four?: string;
  Adjustment?: string;
  Balance_Due?: string;
  Batch_Print?: string; // Do Not Map
  CC_Amt?: string;
  CC_Billing_First_Name?: string;
  CC_Billing_Last_Name?: string;
  CC_Transaction_ID?: string;
  CC_Transaction_ID_Date_Entered?: string;
  Check_Amt?: string;
  Check_No?: string;
  Courses_Subtotal?: string;
  Created_By?: string; // Do Not Map (field-level) — see audit-field note above
  Created_Date?: string;
  Created_Time?: string;
  Discount_Amt?: string;
  Discount_Type?: string;
  Last_Modified_By?: string; // Do Not Map (field-level) — see audit-field note above
  Last_Modified_Date?: string;
  Last_Modified_Time?: string;
  Notes?: string;
  Prior_Registration_ID?: string; // Do Not Map
  Reconciliation_ID?: string; // Do Not Map
  Reg_Fee?: string;
  Registration_Date?: string;
  Student_Country?: string; // Do Not Map
  Student_ID_Previous?: string; // Do Not Map
  [key: string]: string | undefined;
}

// ── Value mapping helpers ─────────────────────────────────────────────────────

// Strips trailing ".0" (4D Longint-to-Text export artifact).
function stripDotZero(v: string | undefined): string {
  return str(v).trim().replace(/\.0$/, "");
}

function parseCurrency(raw: string | undefined): number | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  const n = parseFloat(s);
  return isNaN(n) ? undefined : n;
}

function parseInteger(raw: string | undefined): number | undefined {
  const s = str(raw).trim();
  if (!s) return undefined;
  const n = parseInt(s, 10);
  return isNaN(n) ? undefined : n;
}

function combineDateTime(
  dateRaw: string | undefined,
  timeRaw: string | undefined,
): string | undefined {
  const d = toDate(dateRaw);
  if (!d) return undefined;
  const t = str(timeRaw).trim() || "00:00:00";
  return `${d}T${t}.000Z`;
}

/**
 * CC_Exp_Date — packed "M" + "YY" digits with no separator, e.g. "610" =
 * month 6 / year 10, "1210" = month 12 / year 10. Per workbook: last two
 * digits are always the year, everything before that is the month.
 */
function parseExpiry(
  raw: string | undefined,
): { month: number; year: number } | undefined {
  const digits = str(raw).replace(/\D/g, "");
  if (digits.length < 2) return undefined;
  const year = parseInt(digits.slice(-2), 10);
  const month = parseInt(digits.slice(0, -2), 10);
  if (isNaN(month) || isNaN(year) || month < 1 || month > 12) return undefined;
  return { month, year };
}

// ── Salesforce query helper ───────────────────────────────────────────────────

// Hard cap on each query page so a stalled connection surfaces as a clear
// timeout error instead of leaving the flow spinning with no signal.
const SF_QUERY_TIMEOUT_MS = 120_000;

async function sfQueryAll<T>(
  base: string,
  token: string,
  soql: string,
  logger?: { info: (m: string) => void },
): Promise<T[]> {
  const SF_API = "v60.0";
  const headers = { Authorization: `Bearer ${token}` };
  const all: T[] = [];
  let nextUrl: string | null = null;
  let done = false;
  let page = 0;

  const fetch = async (url: string | null) => {
    const { data } = await axios.get<{
      records: T[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${base}/services/data/${SF_API}/query`,
      url
        ? { headers, timeout: SF_QUERY_TIMEOUT_MS }
        : { params: { q: soql }, headers, timeout: SF_QUERY_TIMEOUT_MS },
    );
    all.push(...data.records);
    done = data.done;
    nextUrl = data.nextRecordsUrl ? `${base}${data.nextRecordsUrl}` : null;
    page++;
    logger?.info(
      `  … page ${page} — ${all.length} record(s) so far${done ? " (done)" : ""}`,
    );
  };

  await fetch(null);
  while (!done && nextUrl) await fetch(nextUrl);
  return all;
}

// ── Student email cache ─────────────────────────────────────────────────────
// Only needed for Payment.GroupPayee — AccountId/BillToContactId are
// resolved by Salesforce itself via external-ID relationship syntax (see
// mapToCardPaymentMethod / mapToOrder / mapToPayment below), so this no
// longer blocks anything else. Runs in parallel with streamAndMap.
// ~190k Account rows / ~95 pages — expect a couple of minutes; each page is
// logged as it arrives (see sfQueryAll) so it's visibly progressing.

async function buildStudentEmailCache(
  base: string,
  token: string,
  logger: { info: (m: string) => void },
): Promise<Map<string, string>> {
  const records = await sfQueryAll<{
    PersonEmail: string | null;
    Student_ID_4D__c: string;
  }>(
    base,
    token,
    "SELECT PersonEmail, Student_ID_4D__c FROM Account WHERE IsPersonAccount = true AND Student_ID_4D__c != null",
    logger,
  );
  const map = new Map<string, string>();
  for (const r of records) {
    if (r.Student_ID_4D__c && r.PersonEmail) {
      map.set(r.Student_ID_4D__c, r.PersonEmail);
    }
  }
  return map;
}

// ── SF record mappers ──────────────────────────────────────────────────────────

type SfRecord = Record<string, unknown>;

/** Shared billing-address fields, reused across CardPaymentMethod and Order. */
function billingAddressFields(raw: RawRegistrationRow): {
  street: string;
  city: string;
  countryCode: string;
  stateCode: string;
  zip: string;
} {
  const street = str(raw.CC_Billing_Address);
  const city = str(raw.CC_Billing_City);
  const countryCode = toSalesforceCountryCode(raw.CC_Billing_Country);
  const stateCode = countryCode
    ? toSalesforceStateCode(raw.CC_Billing_State, countryCode)
    : str(raw.CC_Billing_State);
  const zip = str(raw.CC_Billing_Zip);
  return { street, city, countryCode, stateCode, zip };
}

function mapToCardPaymentMethod(raw: RawRegistrationRow): SfRecord | null {
  const id = str(raw.ID).trim();
  if (!id) return null;

  const record: SfRecord = { [EXT_ID_FIELD]: id };

  // AccountId — resolved by Salesforce during the Bulk API job via external-ID
  // relationship syntax; no pre-fetch needed. See OPEN ITEMS re: relationship name.
  const studentId = stripDotZero(raw.Student_ID);
  if (studentId && studentId !== "0") {
    record["Account.Student_ID_4D__c"] = studentId;
  }

  const { street, city, countryCode, stateCode, zip } =
    billingAddressFields(raw);
  if (street) record.Street = street;
  if (city) record.City = city;
  if (countryCode) record.CountryCode = countryCode;
  if (stateCode) record.State = stateCode;
  if (zip) record.PostalCode = zip;

  const cardType = str(raw.CC_Card_Type);
  if (cardType) record.CardType = cardType; // Direct Map per workbook

  const expiry = parseExpiry(raw.CC_Exp_Date);
  if (expiry) {
    record.ExpiryMonth = expiry.month;
    record.ExpiryYear = expiry.year;
  }

  const lastFour = parseInteger(raw.CC_Last_Four);
  if (lastFour !== undefined) record.CardLastFour = lastFour;

  const firstName = str(raw.CC_Billing_First_Name);
  if (firstName) record.CardHolderFirstName = firstName;

  const lastName = str(raw.CC_Billing_Last_Name);
  if (lastName) record.CardHolderLastName = lastName;

  return record;
}

function mapToOrder(
  raw: RawRegistrationRow,
  logger: { warn: (m: string) => void },
): SfRecord | null {
  const id = str(raw.ID).trim();
  if (!id) return null;

  const record: SfRecord = {
    [EXT_ID_FIELD]: id,
    Status: DEFAULT_ORDER_STATUS, // "Activated" per architect addendum
  };

  // AccountId / BillToContactId — resolved by Salesforce during the Bulk API
  // job via external-ID relationship syntax; no pre-fetch needed.
  // See OPEN ITEMS re: relationship names.
  const studentId = stripDotZero(raw.Student_ID);
  if (studentId && studentId !== "0") {
    record["Account.Student_ID_4D__c"] = studentId;
    record["BillToContact.Student_ID_4D__c"] = studentId; // workbook: "Bill To Contact"
  }

  const { street, city, countryCode, stateCode, zip } =
    billingAddressFields(raw);
  if (street) record.BillingStreet = street;
  if (city) record.BillingCity = city;
  if (countryCode) record.BillingCountryCode = countryCode;
  if (stateCode) record.BillingState = stateCode;
  if (zip) record.BillingPostalCode = zip;

  const adjustment = parseCurrency(raw.Adjustment);
  if (adjustment !== undefined) record.Adjustment__c = adjustment;

  const balanceDue = parseCurrency(raw.Balance_Due);
  if (balanceDue !== undefined) record.Balance_Due__c = balanceDue;

  // Batch_Print — Do Not Map

  const totalAmount = parseCurrency(raw.CC_Amt);
  if (totalAmount !== undefined) record.TotalAmount = totalAmount;

  // OrderReferenceNumber — workbook marks "Conversion" with no described logic;
  // passthrough raw value. See OPEN ITEMS.
  const txnId = str(raw.CC_Transaction_ID);
  if (txnId) record.OrderReferenceNumber = txnId;

  const poDate = toDate(raw.CC_Transaction_ID_Date_Entered);
  if (poDate) record.PoDate = poDate;

  const checkAmt = parseCurrency(raw.Check_Amt);
  if (checkAmt !== undefined) record.Check_Amount__c = checkAmt;

  const checkNoRaw = str(raw.Check_No);
  if (checkNoRaw) {
    const checkNo = parseInteger(checkNoRaw);
    if (checkNo !== undefined) {
      record.Check_Number__c = checkNo;
    } else {
      logger.warn(
        `[Registration Import][Order] Non-numeric Check_No "${checkNoRaw}" for Registration ${id} — skipped (field type is Number)`,
      );
    }
  }

  const coursesSubtotal = parseCurrency(raw.Courses_Subtotal);
  if (coursesSubtotal !== undefined)
    record.Courses_Subtotal__c = coursesSubtotal;

  const discountAmt = parseCurrency(raw.Discount_Amt);
  if (discountAmt !== undefined) record.Discount_Amount__c = discountAmt;

  const discountType = str(raw.Discount_Type);
  if (discountType) record.TA_Discount_Type__c = discountType; // Direct Map per workbook

  const notes = str(raw.Notes)
    .replace(/_4DNL_/g, "\n")
    .trim();
  if (notes) record.Description = notes.slice(0, 32000);

  const regFee = parseCurrency(raw.Reg_Fee);
  if (regFee !== undefined) record.Registration_Fee__c = regFee;

  const regDate = toDate(raw.Registration_Date);
  if (regDate) record.Registration_Date__c = regDate;

  // Audit fields — see OPEN ITEMS (mirrors Enrollment flow precedent).
  const createdDateTime = combineDateTime(raw.Created_Date, raw.Created_Time);
  if (createdDateTime) record.CreatedDate = createdDateTime;

  const lastModDate = toDate(raw.Last_Modified_Date);
  const lastModDateTime = lastModDate
    ? combineDateTime(raw.Last_Modified_Date, raw.Last_Modified_Time)
    : createdDateTime;
  if (lastModDateTime) record.LastModifiedDate = lastModDateTime;

  return record;
}

// Temp key stashed on mapped Payment records to carry the raw Student_ID
// through to the post-stream GroupPayee stitch (see onExecution). Stripped
// before any record is sent to Salesforce — never actually uploaded.
const TEMP_STUDENT_ID_KEY = "_studentId4D";

function mapToPayment(raw: RawRegistrationRow): SfRecord | null {
  const id = str(raw.ID).trim();
  if (!id) return null;

  const record: SfRecord = {
    [EXT_ID_FIELD]: id,
    Type: "Capture", // Default value per workbook
    Status: "Processed", // Default value per workbook
  };

  // AccountId — resolved by Salesforce during the Bulk API job via
  // external-ID relationship syntax; no pre-fetch needed.
  // GroupPayee (a Text field, not a lookup) can't use that trick — the raw
  // Student_ID is stashed here and resolved against the email cache once
  // it's ready (see onExecution), then this temp key is deleted.
  const studentId = stripDotZero(raw.Student_ID);
  if (studentId && studentId !== "0") {
    record["Account.Student_ID_4D__c"] = studentId;
    record[TEMP_STUDENT_ID_KEY] = studentId;
  }

  const amount = parseCurrency(raw.CC_Amt);
  if (amount !== undefined) record.Amount = amount;

  const effectiveDate = toDate(raw.CC_Transaction_ID_Date_Entered);
  if (effectiveDate) record.EffectiveDate = effectiveDate;

  return record;
}

// ── TSV streaming ──────────────────────────────────────────────────────────────

async function streamAndMap(
  fileId: string,
  accessToken: string,
  logger: { info: (m: string) => void; warn: (m: string) => void },
): Promise<{
  cardPaymentMethods: SfRecord[];
  orders: SfRecord[];
  payments: SfRecord[];
  totalRows: number;
}> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const cardPaymentMethods: SfRecord[] = [];
    const orders: SfRecord[] = [];
    const payments: SfRecord[] = [];
    let headers: string[] = [];
    let totalRows = 0;

    papaParse(response.data as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      skipEmptyLines: true,

      step: (result: Papa.ParseResult<string[]>) => {
        const raw = result.data as unknown as string[];

        if (headers.length === 0) {
          headers = raw.map(
            (h, i) =>
              h.replace(/^﻿/, "").replace(/\r/g, "").trim() || `__blank_${i}`,
          );
          return;
        }

        const row: RawRegistrationRow = {};
        headers.forEach((h, i) => {
          if (!h.startsWith("__blank_")) {
            row[h] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        if (!str(row.ID).trim()) return;
        totalRows++;

        const cpm = mapToCardPaymentMethod(row);
        if (cpm) cardPaymentMethods.push(cpm);

        const order = mapToOrder(row, logger);
        if (order) orders.push(order);

        const payment = mapToPayment(row);
        if (payment) payments.push(payment);
      },

      complete: () =>
        resolve({ cardPaymentMethods, orders, payments, totalRows }),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Bulk upsert helper ────────────────────────────────────────────────────────

async function upsertInBatches(
  records: SfRecord[],
  objectName: string,
  sfBase: string,
  sfToken: string,
  logger: {
    info: (m: string) => void;
    warn: (m: string) => void;
    error: (m: string) => void;
  },
  logPrefix: string,
): Promise<{
  processed: number;
  failed: number;
  successfulCsv: string;
  failedCsv: string;
}> {
  let processed = 0;
  let failed = 0;
  let successfulCsv = "";
  let failedCsv = "";

  const batches: SfRecord[][] = [];
  for (let i = 0; i < records.length; i += BULK_BATCH_SIZE) {
    batches.push(records.slice(i, i + BULK_BATCH_SIZE));
  }

  for (let b = 0; b < batches.length; b++) {
    logger.info(
      `${logPrefix} Batch ${b + 1}/${batches.length} — ${batches[b].length} records`,
    );
    const result = await runBulkJob(
      sfBase,
      sfToken,
      objectName,
      EXT_ID_FIELD,
      batches[b],
      logger,
      `${logPrefix} Batch ${b + 1}`,
    );
    processed += result.numberRecordsProcessed;
    failed += result.numberRecordsFailed;
    if (result.successfulCsv)
      successfulCsv += (successfulCsv ? "\n" : "") + result.successfulCsv;
    if (result.failedCsv)
      failedCsv += (failedCsv ? "\n" : "") + result.failedCsv;
  }

  return { processed, failed, successfulCsv, failedCsv };
}

/**
 * Bulk API 2.0's /successfulResults CSV includes every originally-submitted
 * column plus two extras: sf__Id (the real Salesforce record Id assigned)
 * and sf__Created. This pulls out extIdField → sf__Id so a later phase can
 * reference the real Id of a record created in an earlier phase, without a
 * separate SOQL round-trip.
 */
function parseSuccessfulSfIds(
  successfulCsv: string,
  extIdField: string,
): Map<string, string> {
  const map = new Map<string, string>();
  if (!successfulCsv.trim()) return map;
  const parsed = Papa.parse<Record<string, string>>(successfulCsv, {
    header: true,
    skipEmptyLines: true,
  });
  for (const row of parsed.data) {
    const extId = row[extIdField];
    const sfId = row["sf__Id"];
    if (extId && sfId) map.set(extId, sfId);
  }
  return map;
}

// ── Flow ──────────────────────────────────────────────────────────────────────

export const registrationImport = flow({
  name: "Registration Import",
  stableKey: "reg22334-4556-4778-9900-aabbccddeeff",
  description:
    "Streams the Registration TSV from Google Drive and bulk-upserts " +
    "CardPaymentMethod, Order, and Payment records via Bulk API 2.0. " +
    "Must run after the Student flow.",

  onTrigger: (_context, payload) => Promise.resolve({ payload }),

  onExecution: async (context, _params) => {
    const { logger, configVars } = context;
    logger.info("[Registration Import] Starting…");

    // ── Connections ───────────────────────────────────────────────────────────
    const gdConn = configVars[
      "Google Drive Connection"
    ] as unknown as Connection;
    const sfConn = configVars["Salesforce Connection"] as unknown as Connection;
    const fileId = configVars["Registration File ID"] as unknown as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      string | undefined;

    if (!fileId) throw new Error("Registration File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfBase = getSfInstanceUrl(sfConn);

    // ── Build email cache + stream/map in parallel ───────────────────────────
    // Neither depends on the other any more: AccountId/BillToContactId are
    // resolved by Salesforce itself (external-ID relationship syntax), so
    // the only thing the cache still feeds is GroupPayee, stitched on below
    // once both finish.
    logger.info(
      "[Registration Import] Building Student email cache and streaming " +
        "Registration TSV in parallel…",
    );
    const [emailCache, streamResult] = await Promise.all([
      buildStudentEmailCache(sfBase, sfToken, logger),
      streamAndMap(fileId, gdToken, logger),
    ]);
    logger.info(
      `[Registration Import] Email cache — students=${emailCache.size}`,
    );

    let { cardPaymentMethods, orders, payments, totalRows } = streamResult;

    logger.info(
      `[Registration Import] Stream complete — total=${totalRows}, ` +
        `cardPaymentMethods=${cardPaymentMethods.length}, orders=${orders.length}, ` +
        `payments=${payments.length}`,
    );

    // ── Stitch GroupPayee onto Payment records now that the email cache is ready ──
    let groupPayeeMissing = 0;
    for (const payment of payments) {
      const studentId = payment[TEMP_STUDENT_ID_KEY] as string | undefined;
      delete payment[TEMP_STUDENT_ID_KEY];
      if (!studentId) continue;
      const email = emailCache.get(studentId);
      if (email) {
        payment.GroupPayee__c = email;
      } else {
        groupPayeeMissing++;
      }
    }
    if (groupPayeeMissing > 0) {
      logger.warn(
        `[Registration Import][Payment] ${groupPayeeMissing} record(s) missing GroupPayee — ` +
          "Student_ID had no matching Account email in the cache",
      );
    }

    if (TEST_MODE) {
      cardPaymentMethods = cardPaymentMethods.slice(0, TEST_LIMIT);
      orders = orders.slice(0, TEST_LIMIT);
      payments = payments.slice(0, TEST_LIMIT);
      logger.info(
        `[Registration Import] TEST MODE: limited to first ${TEST_LIMIT} records per object`,
      );
    }

    if (
      cardPaymentMethods.length === 0 &&
      orders.length === 0 &&
      payments.length === 0
    ) {
      logger.info("[Registration Import] No records to upsert — done.");
      return {
        data: { totalRows, cardPaymentMethods: 0, orders: 0, payments: 0 },
      };
    }

    // ── Phase 1: CardPaymentMethod ────────────────────────────────────────────
    let cpmProcessed = 0;
    let cpmFailed = 0;
    let cpmSuccessfulCsv = "";
    let cpmFailedCsv = "";

    if (cardPaymentMethods.length > 0) {
      logger.info(
        `[Registration Import] Upserting ${cardPaymentMethods.length} CardPaymentMethod record(s)…`,
      );
      const r = await upsertInBatches(
        cardPaymentMethods,
        SF_CARD_PAYMENT_METHOD,
        sfBase,
        sfToken,
        logger,
        "[Registration Import][CardPaymentMethod]",
      );
      cpmProcessed = r.processed;
      cpmFailed = r.failed;
      cpmSuccessfulCsv = r.successfulCsv;
      cpmFailedCsv = r.failedCsv;
    }

    // ── Phase 2: Order ────────────────────────────────────────────────────────
    let orderProcessed = 0;
    let orderFailed = 0;
    let orderSuccessfulCsv = "";
    let orderFailedCsv = "";

    if (orders.length > 0) {
      logger.info(
        `[Registration Import] Upserting ${orders.length} Order record(s)…`,
      );
      const r = await upsertInBatches(
        orders,
        SF_ORDER,
        sfBase,
        sfToken,
        logger,
        "[Registration Import][Order]",
      );
      orderProcessed = r.processed;
      orderFailed = r.failed;
      orderSuccessfulCsv = r.successfulCsv;
      orderFailedCsv = r.failedCsv;
    }

    // ── Phase 3: PaymentGroup ─────────────────────────────────────────────────
    // One per successfully-upserted Order: SourceObjectId = that Order's real
    // Salesforce Id (pulled from Order's successfulResults, not re-queried).
    // Upserted on External_ID_4D__c = Registration.ID, same as every other
    // object here — re-running the flow updates the same PaymentGroup instead
    // of creating a duplicate, which is what the workbook's "reuse if one
    // already exists for this Order" instruction asks for. See OPEN ITEMS re:
    // this field's existence on PaymentGroup.
    const orderSfIds = parseSuccessfulSfIds(orderSuccessfulCsv, EXT_ID_FIELD);
    const paymentGroups: SfRecord[] = [];
    for (const [registrationId, orderSfId] of orderSfIds) {
      paymentGroups.push({
        [EXT_ID_FIELD]: registrationId,
        SourceObjectId: orderSfId,
      });
    }
    if (orders.length > 0 && paymentGroups.length < orders.length) {
      logger.warn(
        `[Registration Import][PaymentGroup] ${orders.length - paymentGroups.length} ` +
          "registration(s) have no PaymentGroup — their Order did not succeed",
      );
    }

    let pgProcessed = 0;
    let pgFailed = 0;
    let pgSuccessfulCsv = "";
    let pgFailedCsv = "";

    if (paymentGroups.length > 0) {
      logger.info(
        `[Registration Import] Upserting ${paymentGroups.length} PaymentGroup record(s)…`,
      );
      const r = await upsertInBatches(
        paymentGroups,
        SF_PAYMENT_GROUP,
        sfBase,
        sfToken,
        logger,
        "[Registration Import][PaymentGroup]",
      );
      pgProcessed = r.processed;
      pgFailed = r.failed;
      pgSuccessfulCsv = r.successfulCsv;
      pgFailedCsv = r.failedCsv;
    }

    // ── Stitch PaymentGroupId onto Payment records now that PaymentGroup exists ──
    const paymentGroupSfIds = parseSuccessfulSfIds(
      pgSuccessfulCsv,
      EXT_ID_FIELD,
    );
    let paymentGroupMissing = 0;
    for (const payment of payments) {
      const registrationId = payment[EXT_ID_FIELD] as string;
      const pgId = paymentGroupSfIds.get(registrationId);
      if (pgId) {
        payment.PaymentGroupId = pgId;
      } else {
        paymentGroupMissing++;
      }
    }
    if (paymentGroupMissing > 0) {
      logger.warn(
        `[Registration Import][Payment] ${paymentGroupMissing} record(s) missing ` +
          "PaymentGroupId — their PaymentGroup (or its parent Order) did not succeed",
      );
    }

    // ── Phase 4: Payment ──────────────────────────────────────────────────────
    let paymentProcessed = 0;
    let paymentFailed = 0;
    let paymentSuccessfulCsv = "";
    let paymentFailedCsv = "";

    if (payments.length > 0) {
      logger.info(
        `[Registration Import] Upserting ${payments.length} Payment record(s)…`,
      );
      const r = await upsertInBatches(
        payments,
        SF_PAYMENT,
        sfBase,
        sfToken,
        logger,
        "[Registration Import][Payment]",
      );
      paymentProcessed = r.processed;
      paymentFailed = r.failed;
      paymentSuccessfulCsv = r.successfulCsv;
      paymentFailedCsv = r.failedCsv;
    }

    // ── Results sheet ─────────────────────────────────────────────────────────
    try {
      const sheet = await createPerObjectResultsSheet({
        flowName: "Registration Import",
        objects: [
          {
            objectName: SF_CARD_PAYMENT_METHOD,
            successfulCsv: cpmSuccessfulCsv,
            failedCsv: cpmFailedCsv,
          },
          {
            objectName: SF_ORDER,
            successfulCsv: orderSuccessfulCsv,
            failedCsv: orderFailedCsv,
          },
          {
            objectName: SF_PAYMENT_GROUP,
            successfulCsv: pgSuccessfulCsv,
            failedCsv: pgFailedCsv,
          },
          {
            objectName: SF_PAYMENT,
            successfulCsv: paymentSuccessfulCsv,
            failedCsv: paymentFailedCsv,
          },
        ],
        accessToken: gdToken,
        folderId: failedFolderId,
      });
      logger.info(`[Registration Import] Results sheet: ${sheet.url}`);
    } catch (err) {
      logger.warn(
        `[Registration Import] Could not write results sheet: ${String(err)}`,
      );
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    logger.info(
      `[Registration Import] Complete —` +
        `\n  Source rows:              ${totalRows}` +
        `\n  CardPaymentMethod submitted: ${cardPaymentMethods.length}, processed: ${cpmProcessed}, failed: ${cpmFailed}` +
        `\n  Order submitted:             ${orders.length}, processed: ${orderProcessed}, failed: ${orderFailed}` +
        `\n  PaymentGroup submitted:      ${paymentGroups.length}, processed: ${pgProcessed}, failed: ${pgFailed}` +
        `\n  Payment submitted:           ${payments.length}, processed: ${paymentProcessed}, failed: ${paymentFailed}`,
    );

    return {
      data: {
        totalRows,
        cardPaymentMethods: cardPaymentMethods.length,
        cardPaymentMethodsProcessed: cpmProcessed,
        cardPaymentMethodsFailed: cpmFailed,
        orders: orders.length,
        ordersProcessed: orderProcessed,
        ordersFailed: orderFailed,
        paymentGroups: paymentGroups.length,
        paymentGroupsProcessed: pgProcessed,
        paymentGroupsFailed: pgFailed,
        payments: payments.length,
        paymentsProcessed: paymentProcessed,
        paymentsFailed: paymentFailed,
      },
    };
  },
});

export default [registrationImport];
