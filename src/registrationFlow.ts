/**
 * Stanford CSP Migration – Registration Import flow.
 *
 * Windowed execution (same model as the Student flow): streams the
 * Registration TSV from Google Drive in MAX_ROWS-row windows using HTTP
 * Range headers (cursor = byteOffset). Each execution downloads only the
 * bytes it needs, maps that window to Salesforce records, and bulk-upserts
 * via Bulk API 2.0. When more rows remain the flow invokes itself
 * recursively via context.invokeFlow, advancing the byte cursor to where
 * the previous window ended — this is what keeps a single execution from
 * having to hold the entire (very large) source file in memory at once.
 *
 * Each window bulk-upserts FOUR Salesforce objects from each source row:
 *   1. CardPaymentMethod — stored card details (billing name/address, card
 *      type, last four, expiry).
 *   2. Order             — the registration/order record itself (billing
 *      address, amounts, dates, discount/TA info, notes).
 *   3. PaymentGroup       — one per Order, created after Order so it can
 *      point SourceObjectId at the real Order Id. Upserted on
 *      External_ID_4D__c = Registration.ID like everything else, which
 *      satisfies the workbook's "if a PaymentGroup already exists for this
 *      Order, reuse it" rule for free (a re-run just updates the same row
 *      instead of creating a duplicate). Payment_Number__c is an AutoNumber
 *      field — Salesforce assigns it, nothing is mapped to it.
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
 * intentionally excluded and skipped entirely, with no exceptions borrowed
 * from other flows: Batch_Print, Created_By/Date/Time, Last_Modified_By/
 * Date/Time, Prior_Registration_ID, Reconciliation_ID, Student_Country,
 * Student_ID_Previous.
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
 * Upsert key: External_ID_4D__c = Registration.ID, set on all four objects
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
 * Compound address fields — VERIFIED against the real org (`sf sobject
 * describe` run against the STANFORD-DEV sandbox, not just the workbook):
 *   CardPaymentMethod : PaymentMethodStreet, PaymentMethodCity,
 *                        PaymentMethodStateCode, PaymentMethodPostalCode,
 *                        PaymentMethodCountryCode
 *   Order              : BillingStreet, BillingCity, BillingStateCode,
 *                        BillingPostalCode, BillingCountryCode
 *   The workbook's dotted notation (e.g. "PaymentMethodAddress.street")
 *   describes the compound field *grouping* in Setup, not what Bulk API 2.0
 *   accepts on ingest — dotted names in a Bulk API payload mean "traverse
 *   this relationship", not "set this compound sub-field". Bulk fix applied
 *   after the describe: CardPaymentMethod's bare Street/City/StateCode/
 *   PostalCode/CountryCode do not exist on that object at all — real names
 *   are "PaymentMethod"-prefixed. (Order's Billing* names were already
 *   right.) Also confirmed: State/Country are Picklist-typed on both
 *   objects, so the *Code component is the real writable field, not the
 *   plain State/Country text-mirror field — this flow already used
 *   CountryCode/BillingCountryCode but had been inconsistently writing
 *   State/BillingState until fixed to StateCode/BillingStateCode too.
 *
 * ══════════════════════════════════════════════════════════════════════════
 * VERIFIED 2026-09-29 via `sf sobject describe` against the STANFORD-DEV
 * sandbox (not just the workbook). This is real org metadata, not
 * assumption — see below for what's confirmed working vs. what's a hard
 * blocker right now.
 * ══════════════════════════════════════════════════════════════════════════
 *
 * ✅ CONFIRMED against Stanford_UAT (the authoritative org — confirmed
 * 2026-09-29 after STANFORD-DEV and Stanford_UAT were found out of sync;
 * UAT is the one that matters). No changes needed, all verified working:
 *   - External_ID_4D__c exists and is flagged External ID on all 4 objects
 *     this flow writes to (CardPaymentMethod, Order, PaymentGroup, Payment)
 *     plus Account/Contact (used by the relationship-syntax lookups).
 *   - All 9 of Order's mapped custom fields exist: Adjustment__c,
 *     Balance_Due__c, Check_Amount__c, Check_Number__c, Courses_Subtotal__c,
 *     Discount_Amount__c, TA_Discount_Type__c, Registration_Fee__c,
 *     Registration_Date__c.
 *   - Payment.GroupPayee__c exists (Text 255) — initially came back missing
 *     from an `sf sobject describe` check, which turned out to be a
 *     Field-Level Security restriction on the checking user, not a missing
 *     field (Setup showed it fine). FLS was updated to grant the migration
 *     permission set access, re-verified accessible via describe after.
 *     Worth remembering: a field "missing" from describe() can mean FLS,
 *     not "doesn't exist" — check Setup directly if a describe result looks
 *     surprising.
 *   - PaymentGroup.SourceObjectId exists; no other field is required to
 *     save one — confirmed independently both by describe() (only 9 fields
 *     total on the object) and by the architect directly in a design
 *     review call ("the payment group itself doesn't really have any other
 *     fields on it... it's the ID and then here's your source object").
 *     That same call also confirmed creating a new PaymentGroup per
 *     registration (rather than a stricter reuse-lookup) is fine — matches
 *     this flow's upsert-by-External_ID_4D__c approach already.
 *   - PaymentGroup's AutoNumber field is actually named PaymentGroupNumber
 *     in this org — neither "PaymentNumber" nor "Payment_Number__c", the
 *     two names the workbook has used across its revisions. Not mapped to
 *     either way since it's AutoNumber, but worth knowing for accuracy.
 *   - CardPaymentMethod/Order/Payment's other fields (AccountId,
 *     BillToContactId, billing address, Status, OrderReferenceNumber,
 *     PoDate, Description, CardType, ExpiryMonth/Year, CardLastFour,
 *     CardHolderFirstName/LastName, Type, Amount, PaymentGroupId) all exist
 *     with the expected types. (Order.TotalAmount is the one exception —
 *     see the 2026-09-30 real-run findings below: it exists but isn't
 *     writable by anyone, which `describe()` alone didn't make obvious.)
 *
 * 🐛 BUGS FOUND AND FIXED BY THIS VERIFICATION:
 *   - CardPaymentMethod's address fields were wrong. Bare Street/City/
 *     StateCode/PostalCode/CountryCode do not exist on this object — the
 *     real fields are prefixed "PaymentMethod" (PaymentMethodStreet, etc.).
 *     Every CardPaymentMethod record would have been rejected outright.
 *     Fixed.
 *   - Order.EffectiveDate: previously believed (from a prior manual check)
 *     not to exist on Order at all, and the mapping was removed on that
 *     basis. The describe call shows it DOES exist and is REQUIRED. That
 *     earlier information was wrong — restored the mapping (Registration_Date
 *     falling back to PoDate), since without it every Order fails to save.
 *     Still no real source mapped to it in the workbook — confirm with
 *     architect whether the Registration_Date fallback is correct.
 *   - State/Country picklist fields: this flow was inconsistently writing
 *     State/BillingState instead of StateCode/BillingStateCode (while
 *     already correctly using CountryCode/BillingCountryCode) — fixed.
 *
 * ══════════════════════════════════════════════════════════════════════════
 * FIRST REAL BULK API RUN — 2026-09-30, 100 records/object against
 * Stanford_UAT (TEST_MODE window 1). All 300 records failed; every failure
 * had a specific, actionable Bulk API error — none were silent or generic.
 * This surfaced 4 things `sf sobject describe` alone didn't catch, since it
 * shows a field exists but not always whether it's actually writable or
 * what a default should be:
 * ══════════════════════════════════════════════════════════════════════════
 *   - CardPaymentMethod.Status and .ProcessingMode — both required, neither
 *     in the workbook, neither had a code default. FIXED: Status="Active",
 *     ProcessingMode="External". CONFIRMED by architect (2026-10-01) — both
 *     *values* are correct, no longer a guess. BUT a second real run the
 *     same day showed ProcessingMode is also blocked by Field-Level
 *     Security on this org's migration permission set
 *     (INVALID_FIELD_FOR_INSERT_UPDATE, same failure mode as the earlier
 *     GroupPayee__c issue) — needs FLS edit access granted, same fix
 *     pattern as before, on both CardPaymentMethod and Payment.
 *   - Payment.Status and .ProcessingMode — Status="Processed" was already
 *     from the workbook; ProcessingMode="External" was the same guess as
 *     CardPaymentMethod's. Both CONFIRMED by architect (2026-10-01), same
 *     FLS caveat as CardPaymentMethod above.
 *   - Order.Status — REOPENED (2026-10-01): "Default to Activated" (the
 *     architect's earlier addendum) fails outright — FAILED_ACTIVATION:
 *     "For a new or cloned order, choose Draft. An Activated order's status
 *     can't be edited." New Orders must be created as Draft; Activated is a
 *     separate follow-up transition, almost certainly gated on the Order
 *     having real line items (same root cause as the TotalAmount finding
 *     above — this flow creates none). Defaulted to "Draft" so Orders can
 *     actually be created; needs the architect to confirm whether Draft is
 *     the intended final state or a later activation step is expected.
 *   - Order.TotalAmount — NOT a Field-Level Security issue like GroupPayee__c
 *     was. `describe()` shows createable=false/updateable=false — nobody can
 *     write this field via API; it's Order's own built-in platform behavior
 *     once OrderItem line items exist (confirmed not a visible Roll-Up
 *     Summary field via Setup screenshot — native standard-object behavior).
 *     REMOVED the CC_Amt → TotalAmount mapping entirely. RESOLVED
 *     (2026-10-01): the workbook has no Order line item mapping anywhere, so
 *     no OrderItems are created; CC_Amt stays on Payment.Amount only and
 *     Order.TotalAmount is intentionally left blank.
 *   - Order.TA_Discount_Type__c — real source values ("65+", "MLA",
 *     "SAA onetime", "Teacher", ...) don't match any of the object's actual
 *     picklist values ("Senior 65+", "Stanford Affiliate", "SAA Member",
 *     "Educator", etc. — see VALID_TA_DISCOUNT_TYPES). The workbook called
 *     this "Direct Map"; it isn't. FIXED (defensively): only an exact match
 *     is now set, anything else is skipped with a warning instead of
 *     rejecting the whole Order record — but the real source→target mapping
 *     still needs the architect (some pairs look obvious — "65+" →
 *     "Senior 65+" — others don't, e.g. "MLA").
 *
 * ══════════════════════════════════════════════════════════════════════════
 * ARCHITECT ANSWERS — 2026-10-06 (all four implemented):
 * ══════════════════════════════════════════════════════════════════════════
 *   - "Let's set the Order Start Date as the Registration_Date" — RESOLVED.
 *     Order.EffectiveDate is now a direct map from Registration_Date, no
 *     fallback to PoDate. This also resolves the 141,255-row blank-date
 *     problem as a side effect: the new 2-year scope filter (below) is
 *     keyed on the same Registration_Date field and already excludes any
 *     row where it's blank/unparseable before mapToOrder ever sees it, so
 *     EffectiveDate is guaranteed non-blank for every row that makes it
 *     through.
 *   - "Let's go ahead and add the picklist values for the discount types"
 *     — RESOLVED (2026-10-06). 5 raw values that were just a shorter/
 *     differently-cased form of an existing option are translated via
 *     TA_DISCOUNT_TYPE_ALIASES, no Salesforce change needed for those:
 *     "65+"→Senior 65+, "CSP full"→CSP Full (case only), "SAA"→SAA Member,
 *     "STAP"/"STEP"→STAP Benefit. The other 6 (MLA, SAA onetime, Teacher,
 *     Medical Center, Faculty, World Affairs Council) were added as new
 *     picklist options in Salesforce and re-verified via
 *     `sf sobject describe` — exact spelling confirmed matching — then
 *     added to VALID_TA_DISCOUNT_TYPES below. Every Discount_Type value
 *     seen in the source data so far now resolves to a valid option.
 *   - "Let's do the last 2 years and the date to go off of is the
 *     Registration_Date" — RESOLVED, implemented as TWO_YEAR_CUTOFF
 *     (currently "2024-10-06", ~2 years back from when this was decided —
 *     a fixed date, not dynamically computed from the run date, same
 *     convention as Transcript Request's TWO_YEAR_CUTOFF). Confirm this
 *     exact cutoff date is what was intended. Rows with a blank or
 *     unparseable Registration_Date are treated as out-of-scope, not given
 *     the benefit of the doubt — same precedent as Transcript Request.
 *   - "Go ahead and set them all as Draft as the default values to start
 *     and then we can rerun them as Activated" — CONFIRMS the existing
 *     Status="Draft" default is correct. Also tells us a FUTURE flow (not
 *     built yet) will be needed to transition these Orders from Draft to
 *     Activated after this initial load — out of scope for this flow today.
 *
 * ⚠️  STILL OPEN:
 *   - Check_No → Order.Check_Number__c is typed "Number"; non-numeric
 *     values are skipped with a warning. DEFERRED (2026-10-01) — architect
 *     indicated historically all payments were by credit card, so this is
 *     lower priority for now; revisit once the card-vs-check question
 *     below is fully resolved.
 *   - The cash/check-paid-registration gap — CONFIRMED real with evidence
 *     (2026-10-01): checked the actual source file directly — rows where
 *     Check_No is "cash" or "SNAP - ..." have CC_Card_Type, CC_Last_Four,
 *     and CC_Amt all completely blank, not just unused. These are
 *     genuinely non-card payments. Still creating a CardPaymentMethod and
 *     a Type="Capture" Payment unconditionally for every registration
 *     regardless. Question sent to architect: should Card/Capture-Payment
 *     be skipped for these rows, with Payment.Amount coming from
 *     Check_Amt instead of CC_Amt? Code unchanged pending their answer.
 *   - Windowed execution (new): rebuilds the ~190k-row Student email cache
 *     on every window rather than once for the whole file, since each
 *     execution is otherwise stateless. MAX_ROWS=25,000 keeps this to a few
 *     dozen rebuilds for the full file rather than hundreds — if the real
 *     row count turns out much larger than expected, or the cache rebuild
 *     cost becomes a real problem, worth revisiting (e.g. a wider MAX_ROWS,
 *     or caching the email lookup somewhere windows can share).
 */

import { flow, type Connection } from "@prismatic-io/spectral";
import axios from "axios";
import Papa, {
  parse as papaParse,
  type ParseResult,
  type Parser,
} from "papaparse";
import { Transform } from "stream";
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
// Rows per execution window (see "Windowed execution" note in the header
// comment). Bounded well under BULK_BATCH_SIZE so each window submits
// exactly one Bulk API batch per object.
const MAX_ROWS = 25_000;

// 2-year scope filter per architect (2026-10-06): "Let's do the last 2
// years and the date to go off of is the Registration_Date" — same pattern
// as Transcript Request's TWO_YEAR_CUTOFF (a fixed date, not dynamically
// computed from the run date, so the migration scope stays stable across
// re-runs). Set to ~2 years back from the date this was decided; confirm
// with architect if a different exact cutoff was intended.
const TWO_YEAR_CUTOFF = "2024-10-06";

const TEST_MODE = true; // when true: process one window only, then stop (no self-invoke)
const TEST_LIMIT = 50; // additionally cap records (per object) within that one window
// Skips the first N mapped rows before applying TEST_LIMIT — lets a test run
// target fresh, never-before-touched registrations instead of re-hitting
// ones from an earlier test (some of which may now be in a locked Salesforce
// state, e.g. Payment.Status="Canceled" records that can never be upserted
// again). Set to 0 for normal behavior (test the first TEST_LIMIT rows).
const TEST_SKIP = 100;

// Order.Status default — REOPENED (2026-10-01). Architect's addendum said
// "Default to Activated", but the real org rejects that: FAILED_ACTIVATION
// — "For a new or cloned order, choose Draft. An Activated order's status
// can't be edited." Salesforce requires every new Order to be created as
// Draft; Activated is a separate follow-up transition, typically gated on
// the Order actually having line items (same root cause as the TotalAmount
// issue — this flow doesn't create any). Defaulting to "Draft" so Orders
// can actually be created; confirm with architect whether that's the real
// final state, or whether a later activation step is expected once/if
// OrderItems enter scope.
const DEFAULT_ORDER_STATUS = "Draft";

// Order.TA_Discount_Type__c — real picklist values confirmed via
// `sf sobject describe` against Stanford_UAT. Only an exact match (after
// the alias translation below) is set; anything else is skipped with a
// warning rather than sent and rejecting the whole Order.
const VALID_TA_DISCOUNT_TYPES = new Set([
  "Senior 65+",
  "Stanford Affiliate",
  "Stanford Healthcare",
  "SAA Member",
  "SBSAA Member",
  "CSP Full",
  "Century Club V1",
  "Century Club V2",
  "DCI",
  "Educator",
  "Promo Code",
  "STAP Benefit",
  // Added by architect/admin 2026-10-06, re-verified via `sf sobject
  // describe` against Stanford_UAT — exact spelling confirmed matching:
  "MLA",
  "SAA onetime",
  "Teacher",
  "Medical Center",
  "Faculty",
  "World Affairs Council",
]);

// Raw Discount_Type values that are just a shorter/differently-cased form
// of an existing picklist option, confirmed by user (2026-10-06) — these
// translate to the existing option rather than needing a new picklist
// value added in Salesforce. Keyed lowercase for a case-insensitive match
// (covers "CSP full" vs "CSP Full"); everything else still needs an exact
// match against VALID_TA_DISCOUNT_TYPES, or gets added as a new picklist
// value per the architect ("let's go ahead and add the picklist values").
const TA_DISCOUNT_TYPE_ALIASES = new Map<string, string>([
  ["65+", "Senior 65+"],
  ["csp full", "CSP Full"],
  ["saa", "SAA Member"],
  ["stap", "STAP Benefit"],
  ["step", "STAP Benefit"], // likely a typo of STAP
]);

function resolveTaDiscountType(raw: string): string | undefined {
  const alias = TA_DISCOUNT_TYPE_ALIASES.get(raw.toLowerCase());
  if (alias) return alias;
  return VALID_TA_DISCOUNT_TYPES.has(raw) ? raw : undefined;
}

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
  Created_By?: string; // Do Not Map
  Created_Date?: string; // Do Not Map
  Created_Time?: string; // Do Not Map
  Discount_Amt?: string;
  Discount_Type?: string;
  Last_Modified_By?: string; // Do Not Map
  Last_Modified_Date?: string; // Do Not Map
  Last_Modified_Time?: string; // Do Not Map
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

  // Status and ProcessingMode are both required (confirmed via `sf sobject
  // describe`) and not in the workbook at all — every CardPaymentMethod
  // upload failed with REQUIRED_FIELD_MISSING before these were added.
  // Status="Active", ProcessingMode="External" — both CONFIRMED correct by
  // the architect (2026-10-01).
  const record: SfRecord = {
    [EXT_ID_FIELD]: id,
    Status: "Active",
    ProcessingMode: "External",
  };

  // AccountId — resolved by Salesforce during the Bulk API job via external-ID
  // relationship syntax; no pre-fetch needed. See OPEN ITEMS re: relationship name.
  const studentId = stripDotZero(raw.Student_ID);
  if (studentId && studentId !== "0") {
    record["Account.Student_ID_4D__c"] = studentId;
  }

  // Real field names confirmed via `sf sobject describe CardPaymentMethod`
  // against STANFORD-DEV: the PaymentMethodAddress compound field's flat,
  // writable components are prefixed "PaymentMethod", not bare Street/City/
  // etc. (which don't exist on this object at all).
  const { street, city, countryCode, stateCode, zip } =
    billingAddressFields(raw);
  if (street) record.PaymentMethodStreet = street;
  if (city) record.PaymentMethodCity = city;
  if (countryCode) record.PaymentMethodCountryCode = countryCode;
  if (stateCode) record.PaymentMethodStateCode = stateCode;
  if (zip) record.PaymentMethodPostalCode = zip;

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
    Status: DEFAULT_ORDER_STATUS, // "Draft" — see note above re: FAILED_ACTIVATION
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
  if (stateCode) record.BillingStateCode = stateCode; // Picklist per workbook
  if (zip) record.BillingPostalCode = zip;

  const adjustment = parseCurrency(raw.Adjustment);
  if (adjustment !== undefined) record.Adjustment__c = adjustment;

  const balanceDue = parseCurrency(raw.Balance_Due);
  if (balanceDue !== undefined) record.Balance_Due__c = balanceDue;

  // Batch_Print — Do Not Map

  // TotalAmount — REMOVED, CONFIRMED (2026-10-01). Workbook maps CC_Amt
  // here as "Direct Map", but `sf sobject describe` shows createable=false,
  // updateable=false on this field in the real org — not a Field-Level
  // Security issue (that can be granted per-profile), this is Order's
  // built-in platform behavior: once OrderItem line items exist, Salesforce
  // itself takes over this field and nobody can write it directly via API,
  // even though it displays as a plain Currency field in Setup (confirmed
  // via screenshot — not a visible Roll-Up Summary field, this behavior is
  // native to the standard Order object). The workbook has no Order line
  // item mapping anywhere, so no OrderItems are created here — CC_Amt stays
  // on Payment.Amount only, and Order.TotalAmount is intentionally left
  // blank. No longer an open item.

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
      // Also logging the CC_* fields here (not just Check_No) to answer the
      // architect's question directly from real data: for a cash/check/SNAP
      // row, are the credit card fields blank, or do some rows carry both a
      // check number AND card details? See OPEN ITEMS re: card-vs-check.
      logger.warn(
        `[Registration Import][Order] Non-numeric Check_No "${checkNoRaw}" for Registration ${id} ` +
          `— skipped (field type is Number). CC_Card_Type="${str(raw.CC_Card_Type)}" ` +
          `CC_Last_Four="${str(raw.CC_Last_Four)}" CC_Amt="${str(raw.CC_Amt)}"`,
      );
    }
  }

  const coursesSubtotal = parseCurrency(raw.Courses_Subtotal);
  if (coursesSubtotal !== undefined)
    record.Courses_Subtotal__c = coursesSubtotal;

  const discountAmt = parseCurrency(raw.Discount_Amt);
  if (discountAmt !== undefined) record.Discount_Amount__c = discountAmt;

  const discountType = str(raw.Discount_Type);
  if (discountType) {
    const resolved = resolveTaDiscountType(discountType);
    if (resolved) {
      record.TA_Discount_Type__c = resolved;
    } else {
      logger.warn(
        `[Registration Import][Order] Discount_Type "${discountType}" for Registration ${id} ` +
          "doesn't match any TA_Discount_Type__c picklist value — skipped (field left blank)",
      );
    }
  }

  const notes = str(raw.Notes)
    .replace(/_4DNL_/g, "\n")
    .trim();
  if (notes) record.Description = notes.slice(0, 32000);

  const regFee = parseCurrency(raw.Reg_Fee);
  if (regFee !== undefined) record.Registration_Fee__c = regFee;

  const regDate = toDate(raw.Registration_Date);
  if (regDate) record.Registration_Date__c = regDate;

  // Order.EffectiveDate — RESOLVED (2026-10-06): architect confirmed
  // "Let's set the Order Start Date as the Registration_Date". Direct map,
  // no fallback to PoDate needed — and the 141,255-blank-date problem this
  // field had is now moot anyway, since the 2-year scope filter above
  // (also keyed on Registration_Date, per the same architect message)
  // already excludes any row with a blank/unparseable Registration_Date
  // before it ever reaches this mapper. Kept as a conditional set rather
  // than unconditional purely as a defensive guard, not because it's
  // expected to actually be empty here.
  if (regDate) record.EffectiveDate = regDate;

  // Created_By/Date/Time, Last_Modified_By/Date/Time — Do Not Map per this
  // workbook. Each flow's mapping is independent; not borrowing Enrollment's
  // audit-field override here since Registration's own workbook doesn't ask
  // for it.

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
    // ProcessingMode is required (confirmed via describe) and not in the
    // workbook — every Payment upload failed with REQUIRED_FIELD_MISSING
    // before this was added. "External" — CONFIRMED correct by the
    // architect (2026-10-01), same as CardPaymentMethod.
    ProcessingMode: "External",
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

// ── TSV streaming (windowed) ────────────────────────────────────────────────
// Same mechanism as the Student flow: an HTTP Range header downloads only the
// bytes needed for this window (byteOffset onward), a Transform stream tracks
// the exact file-byte position after every newline (accurate even for
// multi-byte UTF-8 characters, unlike PapaParse's own character-counting
// cursor), and parsing stops once MAX_ROWS rows have been mapped. The byte
// position of the last *fully processed* row becomes nextByteOffset — the
// exact resume point for the next window, with no gap or overlap.

interface WindowStreamResult {
  cardPaymentMethods: SfRecord[];
  orders: SfRecord[];
  payments: SfRecord[];
  rowsInWindow: number;
  filteredOut: number; // rows skipped by the 2-year Registration_Date scope filter
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
}

async function streamAndMapWindow(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  logger: { info: (m: string) => void; warn: (m: string) => void },
): Promise<WindowStreamResult> {
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
    // For byteOffset > 0 this window has no header row — use knownHeaders
    // passed down from the previous window via the self-invoke payload.
    let headers: string[] = knownHeaders.length > 0 ? [...knownHeaders] : [];
    const cardPaymentMethods: SfRecord[] = [];
    const orders: SfRecord[] = [];
    const payments: SfRecord[] = [];
    let rowsInWindow = 0;
    let filteredOut = 0;
    let aborted = false;
    // Byte position after the last fully-processed row. Updated BEFORE the
    // maxRows check below, so when we abort, this already points to the
    // start of the first unprocessed row — the correct resume point.
    let lastCompletedCursor = byteOffset;
    let stepCount = 0;

    papaParse(byteTracker as unknown as NodeJS.ReadableStream, {
      delimiter: "\t",
      quoteChar: "\0",
      header: false,
      // Must be false so step() fires for every raw line (including empty
      // ones), keeping stepCount in sync with lineEndBytes.
      skipEmptyLines: false,

      step: (result: ParseResult<string[]>, parser: Parser) => {
        if (aborted) return;

        const raw = result.data as unknown as string[];
        const rowEndByte =
          lineEndBytes[stepCount] ?? byteOffset + totalBytesReceived;
        stepCount++;

        if (raw.length === 0 || raw.every((c) => c.replace(/\r/g, "") === "")) {
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (headers.length === 0) {
          headers = raw.map(
            (h, i) =>
              h.replace(/^﻿/, "").replace(/\r/g, "").trim() || `__blank_${i}`,
          );
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (rowsInWindow >= maxRows) {
          aborted = true;
          parser.abort();
          return;
        }

        const row: RawRegistrationRow = {};
        headers.forEach((h, i) => {
          if (!h.startsWith("__blank_")) {
            row[h] = (raw[i] ?? "").replace(/\r/g, "");
          }
        });

        if (!str(row.ID).trim()) {
          lastCompletedCursor = rowEndByte;
          return;
        }
        rowsInWindow++;

        // 2-year scope filter — per architect, Registration_Date is the
        // field to go off of; a blank/unparseable date is treated as
        // out-of-scope rather than given the benefit of the doubt (same
        // precedent as Transcript Request's TWO_YEAR_CUTOFF filter).
        const registrationDate = toDate(row.Registration_Date);
        if (!registrationDate || registrationDate < TWO_YEAR_CUTOFF) {
          filteredOut++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        const cpm = mapToCardPaymentMethod(row);
        if (cpm) cardPaymentMethods.push(cpm);

        const order = mapToOrder(row, logger);
        if (order) orders.push(order);

        const payment = mapToPayment(row);
        if (payment) payments.push(payment);

        lastCompletedCursor = rowEndByte;
      },

      complete: () =>
        resolve({
          cardPaymentMethods,
          orders,
          payments,
          rowsInWindow,
          filteredOut,
          hasMore: aborted,
          nextByteOffset: lastCompletedCursor,
          parsedHeaders: headers,
        }),
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
// Windowed execution (same model as the Student flow): each execution reads
// and processes at most MAX_ROWS rows, starting from a byte-offset cursor
// carried in the trigger payload. When more rows remain, this flow invokes
// itself via context.invokeFlow with the next byte offset, so a file with
// hundreds of thousands of rows runs as a chain of many small, well-bounded
// executions instead of one giant one. TEST_MODE stops that chain after a
// single window regardless of how much of the file remains.

export const registrationImport = flow({
  name: "Registration Import",
  stableKey: "reg22334-4556-4778-9900-aabbccddeeff",
  description:
    "Streams the Registration TSV from Google Drive one window at a time " +
    "and bulk-upserts CardPaymentMethod, Order, PaymentGroup, and Payment " +
    "records via Bulk API 2.0. Recurses until the full file has been " +
    "processed. Must run after the Student flow.",

  onTrigger: async (_context, payload) => {
    await Promise.resolve();
    return { payload };
  },

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

    // ── Read cursor (byteOffset) from trigger payload ────────────────────────
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

    logger.info(
      `[Registration Import] Starting window ${windowNumber} at byte ${byteOffset}…`,
    );

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

    // ── Build email cache + stream/map this window in parallel ───────────────
    // Neither depends on the other any more: AccountId/BillToContactId are
    // resolved by Salesforce itself (external-ID relationship syntax), so
    // the only thing the cache still feeds is GroupPayee, stitched on below
    // once both finish. The cache is rebuilt every window (~190k accounts,
    // ~95 pages) — a real cost of windowing, traded off against MAX_ROWS
    // being wide enough (25,000) that it's only paid a few dozen times for
    // the whole file rather than once per tiny window.
    logger.info(
      `[Registration Import] Window ${windowNumber}: building Student email ` +
        "cache and streaming this window of the Registration TSV in parallel…",
    );
    const [emailCache, windowResult] = await Promise.all([
      buildStudentEmailCache(sfBase, sfToken, logger),
      streamAndMapWindow(
        fileId,
        gdToken,
        byteOffset,
        MAX_ROWS,
        knownHeaders,
        logger,
      ),
    ]);
    logger.info(
      `[Registration Import] Email cache — students=${emailCache.size}`,
    );

    let { cardPaymentMethods, orders, payments } = windowResult;
    const {
      rowsInWindow,
      filteredOut,
      hasMore,
      nextByteOffset,
      parsedHeaders,
    } = windowResult;

    logger.info(
      `[Registration Import] Window ${windowNumber} stream complete — ` +
        `rows=${rowsInWindow}, filteredOut(2yr scope)=${filteredOut}, ` +
        `hasMore=${hasMore}, nextByte=${nextByteOffset}, ` +
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
      cardPaymentMethods = cardPaymentMethods.slice(
        TEST_SKIP,
        TEST_SKIP + TEST_LIMIT,
      );
      orders = orders.slice(TEST_SKIP, TEST_SKIP + TEST_LIMIT);
      payments = payments.slice(TEST_SKIP, TEST_SKIP + TEST_LIMIT);
      logger.info(
        `[Registration Import] TEST MODE: skipped first ${TEST_SKIP}, limited to next ${TEST_LIMIT} records per object, and won't invoke the next window.`,
      );
    }

    // ── Upsert this window's records (skipped entirely if the window was empty) ──
    if (
      cardPaymentMethods.length === 0 &&
      orders.length === 0 &&
      payments.length === 0
    ) {
      logger.info(
        `[Registration Import] Window ${windowNumber}: no records to upsert.`,
      );
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
      `[Registration Import] Window ${windowNumber} complete —` +
        `\n  Window rows:               ${rowsInWindow}` +
        `\n  CardPaymentMethod submitted: ${cardPaymentMethods.length}, processed: ${cpmProcessed}, failed: ${cpmFailed}` +
        `\n  Order submitted:             ${orders.length}, processed: ${orderProcessed}, failed: ${orderFailed}` +
        `\n  PaymentGroup submitted:      ${paymentGroups.length}, processed: ${pgProcessed}, failed: ${pgFailed}` +
        `\n  Payment submitted:           ${payments.length}, processed: ${paymentProcessed}, failed: ${paymentFailed}`,
    );

    // ── Invoke next window if more rows remain ───────────────────────────────
    if (TEST_MODE && hasMore) {
      logger.info(
        "[Registration Import] TEST MODE: more rows remain, but stopping here " +
          "— set TEST_MODE = false to process the full file.",
      );
    } else if (hasMore) {
      logger.info(
        `[Registration Import] More rows remain — invoking window ${windowNumber + 1} ` +
          `at byte ${nextByteOffset}…`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Registration Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
      });
    } else {
      logger.info(
        `[Registration Import] All rows processed — import complete after ${windowNumber} window(s).`,
      );
    }

    return {
      data: {
        windowNumber,
        byteOffset,
        nextByteOffset,
        hasMore,
        rowsInWindow,
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
