/**
 * Stanford CSP Migration – Alert (RecordAlert) flow.
 *
 * Reads the Alert TSV from Google Drive, maps each row to a standard
 * Salesforce RecordAlert record, and upserts via Bulk API 2.0. Recurses via
 * context.invokeFlow until the full file is processed — same streaming
 * shape as every other flow in this project (see courseOtherCostFlow.ts).
 *
 * Field mapping (per "CSP Data Migration Mapping Workbook - Alert.pdf",
 * field API names confirmed against the RecordAlert object describe):
 *
 *   ID                          → SourceSystemIdentifier     (Upsert / external ID)
 *     Standard field, type "External Lookup (Unique)" — confirmed with SA.
 *
 *   Student_ID / Associate_ID / Instructor_ID
 *                                → WhatId ONLY — ParentId intentionally
 *                                  skipped (see Q10)        (Lookup → PersonAccount)
 *     Conversion: whichever of Student_ID, Associate_ID, Instructor_ID is
 *     populated and not "0" identifies the PersonAccount to look up (by
 *     Student_ID_4D__c / Associate_ID_4D__c / Instructor_ID_4D__c respectively).
 *     SA confirmed by reviewing the full source data: exactly one of the three
 *     is ever populated per row, never two — so the Student→Associate→
 *     Instructor priority order in code is a defensive fallback only, not
 *     something expected to actually trigger. The workbook maps this same
 *     Account Id to BOTH ParentId and WhatId (two identical Lookup rows, one
 *     labelled "Parent Record", one "What Record"), but RecordAlert has a
 *     validation rule in the target org forbidding ParentId === WhatId —
 *     confirmed by a live test run. Since both would always resolve to the
 *     exact same Account Id, there's no case where they'd ever legitimately
 *     differ, so ParentId is skipped entirely and WhatId (the required one)
 *     gets the value. Decided with SA — see Q10.
 *
 *     WhatId fallback for course-only alerts — see Q11: when a row has NO
 *     Student_ID/Associate_ID/Instructor_ID at all (course/logistics alerts
 *     like "Libby reserved studio 301..."), WhatId falls back to the
 *     CourseOffering's parent LearningCourse (plain CourseOffering is NOT an
 *     allowed WhatId type — confirmed via live test and object describe —
 *     but its parent LearningCourse is), resolved via a new live
 *     LearningCourse Id lookup (loadLearningCourseIdMap) keyed by the same
 *     "base course code" Course flow already uses to link CourseOffering →
 *     LearningCourse. Rows with neither a person nor a resolvable
 *     course/LearningCourse are left with WhatId blank and will fail
 *     REQUIRED_FIELD_MISSING — same as any other unmatched lookup, it shows
 *     up in the failed-records sheet rather than being silently skipped
 *     before upload.
 *
 *   Title                        → Subject                    (Direct Map)
 *
 *   Category                     → RecordAlertCategoryId       (Lookup → RecordAlertCategory)
 *     Conversion: Behavioral→Behavioral, Payment→Payment, STAP→STAP,
 *     Grades→Grades, anything else→Other. Looked up against
 *     RecordAlertCategory.MasterLabel in the target org (NOT .Name — see Q7).
 *     Lookup field name is "RecordAlertCategoryId" — confirmed via a live
 *     object describe against the org (NOT "RecordAlertCategory" as an
 *     earlier Setup screenshot suggested; the describe call is authoritative
 *     and overrides that — see Q8).
 *
 *   Course_RecID                 → Course_Offering__c          (Lookup → CourseOffering)
 *     Field name confirmed via Setup → Object Manager → Record Alert → Fields.
 *     Conversion: Course_RecID identifies the CourseOffering this alert is
 *     tied to. Resolved the same way as every other Course_RecID lookup in
 *     this project — via the Course TSV RecID→ID crosswalk (loadCourseRecIdMap)
 *     — then set on the CSV upload using Salesforce's relationship/external-ID
 *     syntax (Course_Offering__r.External_ID_4D__c), same pattern as
 *     courseAssociateInstructorFlow.ts and textbookFlow.ts. Requires the same
 *     "Course File ID" config var those flows use. Skipped (left blank, not
 *     row-dropped) when Course_RecID is "0"/blank or isn't found in the
 *     crosswalk — unlike courseOtherCostFlow.ts, this flow does NOT drop the
 *     whole Alert record just because its course falls outside the 2-year
 *     migration window, since the alert is still meaningful people-data
 *     without a course link.
 *
 *   Description                  → Description                (Direct Map)
 *     _4DNL_ tokens from the 4D export are converted to real newlines, same
 *     handling as every other flow in this project (e.g. Notes__c in
 *     courseAssociateInstructorFlow.ts). TRUNCATED TO 255 CHARS — the live
 *     object describe shows this field is a plain 255-char Text field, NOT
 *     the "Text Area (Long)" the workbook says. See Q9 — this silently
 *     discards most of the content for any long Description (e.g. the
 *     multi-paragraph email-thread sample record in the workbook itself).
 *
 *   Created_Date + Created_Time  → EffectiveDate                (Conversion)
 *     The two source columns are combined into a single Date/Time value.
 *     Field API name is "EffectiveDate" — confirmed via a live object
 *     describe against the org. Its UI label is "Effective Start Date" (which
 *     is why the workbook calls it that and an earlier version of this flow
 *     used "EffectiveStartDate" as the field name, which Bulk API rejected —
 *     see Q8).
 *
 *   (no source field)            → Severity = "Info"           (Conversion)
 *     Workbook: "Default Map to 'Info'" — hardcoded, not read from the source.
 *
 *   Created_Date                 → ValidUntilDate               (Conversion)
 *     Set to Created_Date + 50 years. ValidUntilDate is a Date/Time field, so
 *     the time-of-day is copied from EffectiveDate (i.e. the alert's own
 *     Created_Time) rather than defaulting to midnight — confirmed correct.
 *
 * Do Not Map (per workbook): Created_By, Last_Modified_Date, Last_Modified_Time,
 *   Last_Modified_By, Student_ID_Previous.
 *
 * ── OPEN QUESTIONS FOR ARCHITECT (none of these are resolved by this flow —
 *    each is called out again inline at the relevant code) ────────────────────
 *
 *   Q1. RESOLVED (workbook v2) — Course_RecID now maps to a "Course Offering"
 *       lookup, Field API Course_Offering__c. Implemented below as
 *       COURSE_LOOKUP_RELATIONSHIP_FIELD. Field name confirmed — see Q5.
 *
 *   Q2. RESOLVED — SA reviewed the full source data and confirmed exactly one
 *       of Student_ID / Associate_ID / Instructor_ID is ever populated per
 *       row, never two at once. The Student→Associate→Instructor priority
 *       order in mapAlertRow() is kept as a defensive fallback only.
 *
 *   Q3. ANSWERED — "SELECT Id, MasterLabel FROM RecordAlertCategory" returned
 *       zero rows in a live test on 2026-10-05: there are currently NO
 *       RecordAlertCategory records in the target org. This flow only looks
 *       them up, it does not create them, so until 5 records exist with
 *       MasterLabel = Behavioral / Payment / STAP / Grades / Other, every
 *       Alert's Category will come through blank (not an error — logged as a
 *       warning and the rest of the record still uploads). BLOCKS Category
 *       from actually populating until those 5 records are created.
 *
 *   Q4. RESOLVED — confirmed the time-of-day should follow the source data
 *       (i.e. reuse Created_Time / EffectiveDate's time), not default to
 *       midnight. Matches what this flow already does.
 *
 *   Q5. RESOLVED — SA confirmed via Setup → Object Manager → Record Alert →
 *       Fields that the Course Offering lookup's real API name is
 *       "Course_Offering__c", matching this flow's assumption exactly. CSV
 *       upload uses "Course_Offering__r.External_ID_4D__c" (relationship
 *       name + external-ID field) — see COURSE_LOOKUP_RELATIONSHIP_FIELD.
 *
 *   Q6. RESOLVED — "only add last two years." Implemented as TWO_YEAR_CUTOFF
 *       ("2024-10-06", a fixed date — same convention as Transcript Request's
 *       and Registration's TWO_YEAR_CUTOFF, not dynamically computed from the
 *       run date, so scope stays stable across re-runs). Rows with
 *       Created_Date before the cutoff, or blank/unparseable, are skipped
 *       entirely (counted in skippedCount) rather than uploaded.
 *
 *   Q7. RESOLVED — RecordAlertCategory has no "Name" field; a live test run
 *       on 2026-10-05 confirmed "No such column 'Name'" (errorCode
 *       INVALID_FIELD). Switched to "MasterLabel" and confirmed correct via a
 *       direct test query ("SELECT Id, MasterLabel FROM RecordAlertCategory"
 *       ran with no error — see Q3, it just returned zero rows because no
 *       category records exist yet).
 *
 *   Q8. RESOLVED — a live test run on 2026-10-05 (after the 5
 *       RecordAlertCategory records were created) got past Category and all
 *       the way to the Bulk API upload, which then failed with "InvalidBatch
 *       : Field name not found : EffectiveStartDate". A direct object
 *       describe against the org (sf sobject describe RecordAlert) confirmed
 *       two real field-name bugs at once:
 *         1. EffectiveStartDate doesn't exist — the real API name is
 *            "EffectiveDate" (its UI label is "Effective Start Date", which
 *            is where the wrong name came from). Fixed.
 *         2. RecordAlertCategoryId, not RecordAlertCategory, is the real
 *            lookup field for Category — this contradicts the Setup
 *            screenshot Q3/earlier assumed was authoritative. The live
 *            describe call is the ground truth and overrides that
 *            screenshot. Fixed — see CATEGORY_LOOKUP_FIELD.
 *       This run had not yet re-tested the upload with both fixes in place.
 *
 *   Q9. NEEDS ARCHITECT INPUT (not just a naming slip — a real data-loss
 *       question) — after Q8's fixes, a live run on 2026-10-05 got all the
 *       way to the Bulk API upload, but ALL 1000 records failed with
 *       "STRING_TOO_LONG:Description ... max length=255". A direct object
 *       describe confirms RecordAlert.Description is a plain 255-character
 *       Text field (type "string", length 255) — NOT the "Text Area (Long)"
 *       the workbook describes. This flow now truncates to 255 chars to
 *       unblock the upload, but the workbook's own sample record is a
 *       multi-paragraph email thread thousands of characters long — meaning
 *       for real rows like that, 255 characters keeps only a small fragment
 *       of the actual alert history and silently drops the rest. Questions
 *       for the architect: (a) is 255-char truncation acceptable, or (b)
 *       should Description be migrated into a different field/related
 *       object that actually supports long text (e.g. a Note or a different
 *       field on RecordAlert), or (c) does the RecordAlert.Description field
 *       need to be changed to a true Long Text Area before this can migrate
 *       the real content? Not something this flow can decide on its own.
 *
 *   Q10. RESOLVED. After Q9's fix, a live run on 2026-10-06 showed nearly
 *        every row failing with "FIELD_CUSTOM_VALIDATION_EXCEPTION: WhatId
 *        and ParentId cannot be the same." RecordAlert has a validation rule
 *        in the target org that directly contradicts the workbook: the
 *        workbook's Student_ID/Associate_ID/Instructor_ID rows say to set
 *        BOTH ParentId and WhatId to the same looked-up PersonAccount, but
 *        the org won't allow that.
 *
 *        Decided with SA: since the same source field drives both ParentId
 *        and WhatId, there's no scenario where they'd ever legitimately
 *        differ — so ParentId is skipped entirely (not set at all) and
 *        WhatId, which is required, gets the resolved Account Id. The old
 *        "set ParentId too" line is kept as a commented-out reference in
 *        mapAlertRow() rather than deleted outright.
 *
 *   Q11. RESOLVED. After Q10's fix, a live run on 2026-10-06 (after the
 *        2-year filter from Q6) showed 260 of 1000 rows failing with
 *        "REQUIRED_FIELD_MISSING: Required fields are missing: [WhatId]".
 *        Every failed row had Student_ID = Associate_ID = Instructor_ID = 0
 *        — these are course/logistics alerts, not about any person (e.g.
 *        "Libby reserved studio 301...", "Instructor reserved CCSR 0240"),
 *        so the Q10 person-lookup path never set WhatId for them, and
 *        WhatId being required meant every one of these rows failed outright.
 *
 *        First attempt (WRONG, reverted): fall back to the CourseOffering
 *        itself for WhatId. A live test run showed
 *        "FIELD_INTEGRITY_EXCEPTION: id value of incorrect type" — a direct
 *        describe of WhatId's referenceTo list confirmed plain CourseOffering
 *        is NOT an allowed target type for WhatId (or ParentId) at all, no
 *        matter what — this isn't a config gap, it's a hard schema limit.
 *        Confirmed independently via Setup → Record Alert → Fields → What
 *        Record, which lists every allowed type and plain "Course Offering"
 *        is genuinely absent (several of its CHILD objects — Course Offering
 *        Schedule, Course Offering Participant, etc. — ARE listed, just not
 *        CourseOffering itself).
 *
 *        Working fix (SA's idea): every CourseOffering has exactly one
 *        parent LearningCourse (the "base course," stripped of quarter and
 *        section — e.g. CourseOffering "20241_BUS 62" → LearningCourse
 *        "BUS 62" — this is literally how Course flow already links them,
 *        see courseFlow.ts's `baseCode`/`stripSectionSuffix`). LearningCourse
 *        IS an allowed WhatId type. So for course-only rows, WhatId now
 *        resolves to that CourseOffering's parent LearningCourse instead —
 *        confirmed working against the org (verified the LearningCourse for
 *        "BUS 62", the course on one of the actually-failed rows, exists
 *        with a real Id). New live lookup: loadLearningCourseIdMap().
 *
 *        Remaining edge case (not blocking, just worth knowing about): a row
 *        with NEITHER a person NOR a resolvable course/LearningCourse has
 *        nothing valid to put in WhatId (which is required at the schema
 *        level — nillable: false — so it can't just be left blank). This
 *        flow leaves WhatId blank for those and lets Salesforce reject them
 *        with REQUIRED_FIELD_MISSING, same as any other unmatched lookup —
 *        they'll show up in the failed-records sheet rather than being
 *        silently dropped before upload. Logged separately via
 *        missingWhatIdCount so it's distinguishable from "had a person ID
 *        but no Account match."
 */

import { flow } from "@prismatic-io/spectral";
import axios from "axios";
import { parse, type ParseResult, type Parser } from "papaparse";
import { Transform } from "stream";
import {
  str,
  toDate,
  getAccessToken,
  getSfInstanceUrl,
  runBulkJob,
  resolveAccountIdsByField,
  loadCourseRecIdMap,
  SF_API_VERSION,
} from "./utils";
import { createResultsSheetFromContacts } from "./reportResults";

// ── Constants ──────────────────────────────────────────────────────────────────

const MAX_ROWS = 1000;

// Confirmed against the RecordAlert object describe — see header comment.
const EXTERNAL_ID_FIELD = "SourceSystemIdentifier";
const CATEGORY_LOOKUP_FIELD = "RecordAlertCategoryId";

// Q5 RESOLVED — confirmed against the RecordAlert object describe (see header).
const COURSE_LOOKUP_RELATIONSHIP_FIELD = "Course_Offering__r.External_ID_4D__c";

// Q6 RESOLVED — "only add last two years." Same pattern as Transcript
// Request's and Registration's TWO_YEAR_CUTOFF: a fixed date, not
// dynamically computed from the run date, so the migration scope stays
// stable across re-runs. Created_Date is the field to go off of; a
// blank/unparseable date is treated as out-of-scope rather than given the
// benefit of the doubt — same precedent as those two flows.
const TWO_YEAR_CUTOFF = "2024-10-06";

const VALID_UNTIL_YEARS = 50;

type ParentLookupField =
  "Student_ID_4D__c" | "Associate_ID_4D__c" | "Instructor_ID_4D__c";

// ── Raw TSV shape ──────────────────────────────────────────────────────────────

interface RawAlertRecord {
  ID?: string;
  Student_ID?: string;
  Title?: string;
  Category?: string;
  Associate_ID?: string;
  Instructor_ID?: string;
  Course_RecID?: string; // → Course_Offering__r.External_ID_4D__c (Q5)
  Description?: string;
  Created_Date?: string;
  Created_Time?: string;
  [key: string]: string | undefined;
}

// ── Salesforce RecordAlert target shape ────────────────────────────────────────

interface RecordAlertUpload {
  ParentId?: string;
  WhatId?: string;
  Subject?: string;
  Description?: string;
  EffectiveDate?: string;
  Severity?: string;
  ValidUntilDate?: string;
  // Also holds EXTERNAL_ID_FIELD and CATEGORY_LOOKUP_FIELD (dynamic keys).
  [key: string]: unknown;
}

/** A parsed row whose Parent/What/Category lookups haven't been resolved yet. */
interface PendingAlertRow {
  record: RecordAlertUpload;
  externalId: string;
  parentLookupField: ParentLookupField | null;
  parentLookupValue: string;
  categoryName: string;
  // Q11 (see header) — Course External_ID_4D__c for this row, when resolved
  // via the Course_RecID crosswalk. Used as the WhatId fallback when there's
  // no Student/Associate/Instructor (course/logistics-only alerts).
  courseExternalId: string;
}

interface StreamResult {
  rows: PendingAlertRow[];
  hasMore: boolean;
  nextByteOffset: number;
  parsedHeaders: string[];
  firstId: string;
  lastId: string;
  skippedCount: number;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

/** Combine a 4D Date column and a 4D Time column into one Date/Time ISO string. */
function combineDateTime(
  dateStr: string | undefined,
  timeStr: string | undefined,
): string {
  const d = toDate(dateStr);
  if (!d) return "";

  const t = str(timeStr);
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?/.exec(t);
  const hh = (m ? m[1] : "0").padStart(2, "0");
  const mm = m ? m[2] : "00";
  const ss = m ? (m[3] ?? "00") : "00";

  return `${d}T${hh}:${mm}:${ss}Z`;
}

/** Pulls the Salesforce error body (status + errorCode/message) out of an axios error. */
function describeSfError(err: unknown): string {
  const e = err as { response?: { status?: number; data?: unknown } };
  return (
    `Status: ${e.response?.status ?? "unknown"}\n` +
    `Details: ${JSON.stringify(e.response?.data, null, 2)}`
  );
}

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

/**
 * Add N years to an ISO Date/Time string produced by combineDateTime().
 * Handles the Feb 29 leap-day edge case: if the source date is Feb 29 and
 * the target year isn't a leap year, rolls back to Feb 28 (standard
 * "add years" calendar behavior) instead of emitting an invalid date like
 * "2075-02-29", which Salesforce would reject for that row.
 */
function addYears(isoDateTime: string, years: number): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(.+)$/.exec(isoDateTime);
  if (!m) return "";
  const [, y, mo, da, rest] = m;
  const newYear = parseInt(y, 10) + years;
  const day = mo === "02" && da === "29" && !isLeapYear(newYear) ? "28" : da;
  return `${newYear}-${mo}-${day}T${rest}`;
}

/**
 * 4D Category picklist → RecordAlertCategory.MasterLabel.
 * Behavioral = Behavioral, Payment = Payment, STAP = STAP, Grades = Grades,
 * all other values (including blank) = Other.
 */
function mapCategoryName(raw: string | undefined): string {
  const v = str(raw).toLowerCase();
  if (v === "behavioral") return "Behavioral";
  if (v === "payment") return "Payment";
  if (v === "stap") return "STAP";
  if (v === "grades") return "Grades";
  return "Other";
}

// Q3 (see header) — assumes Behavioral/Payment/STAP/Grades/Other already
// exist as RecordAlertCategory records; this flow only looks them up.
//
// Q7 — RecordAlertCategory has no "Name" field (confirmed by a live run:
// "No such column 'Name' on entity 'RecordAlertCategory'", errorCode
// INVALID_FIELD). Switched to "MasterLabel", the field Salesforce uses for
// the display label on "setup/configuration"-style standard objects like
// this one (same pattern as RecordType, BusinessProcess, etc.) — this is a
// strong convention match, not yet confirmed via the object describe the way
// Q1/Q5 were. Please verify via Setup → Object Manager → Record Alert
// Category → Fields before the next run, same as before.
/** Loads the full RecordAlertCategory table as Map<MasterLabel, Id>. Small reference table. */
async function loadRecordAlertCategoryMap(
  instanceUrl: string,
  accessToken: string,
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const headers = { Authorization: `Bearer ${accessToken}` };
  let done = false;
  let nextUrl: string | null = null;

  const fetchPage = async (url: string | null) => {
    const { data } = await axios.get<{
      records: { Id: string; MasterLabel: string }[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      url
        ? { headers }
        : {
            params: { q: "SELECT Id, MasterLabel FROM RecordAlertCategory" },
            headers,
          },
    );
    for (const rec of data.records) {
      map.set(rec.MasterLabel, rec.Id);
    }
    done = data.done;
    nextUrl = data.nextRecordsUrl
      ? `${instanceUrl}${data.nextRecordsUrl}`
      : null;
  };

  await fetchPage(null);
  while (!done && nextUrl) await fetchPage(nextUrl);

  return map;
}

// Q11 (see header) — "course-only" rows (no Student/Associate/Instructor)
// need a real CourseOffering Salesforce Id for WhatId, since WhatId is a
// polymorphic field and can't use the external-ID relationship CSV trick
// Course_Offering__c uses. Same query shape already used in
// enrollmentFlow.ts's buildCourseOfferingCache — loads the full table once
// rather than chunking by value, since the course catalog is small relative
// to Account/Enrollment volume.
/** Loads the full CourseOffering table as Map<External_ID_4D__c, Id>. */
async function loadCourseOfferingIdMap(
  instanceUrl: string,
  accessToken: string,
): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  const headers = { Authorization: `Bearer ${accessToken}` };
  let done = false;
  let nextUrl: string | null = null;

  const fetchPage = async (url: string | null) => {
    const { data } = await axios.get<{
      records: { Id: string; External_ID_4D__c: string }[];
      done: boolean;
      nextRecordsUrl?: string;
    }>(
      url ?? `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      url
        ? { headers }
        : {
            params: {
              q: "SELECT Id, External_ID_4D__c FROM CourseOffering WHERE External_ID_4D__c != null",
            },
            headers,
          },
    );
    for (const rec of data.records) {
      if (rec.External_ID_4D__c) map.set(rec.External_ID_4D__c, rec.Id);
    }
    done = data.done;
    nextUrl = data.nextRecordsUrl
      ? `${instanceUrl}${data.nextRecordsUrl}`
      : null;
  };

  await fetchPage(null);
  while (!done && nextUrl) await fetchPage(nextUrl);

  return map;
}

// ── Field mapping (per row, before Parent/What/Category lookups) ──────────────

function mapAlertRow(
  raw: RawAlertRecord,
  courseRecIdMap: Map<string, string>,
  missingCourseRecIds: Set<string>,
): PendingAlertRow | null {
  const id = str(raw.ID);
  if (!id || !/^[1-9]\d*$/.test(id)) return null;

  // 2-year scope filter (Q6) — Created_Date is the field to go off of; a
  // blank/unparseable date is treated as out-of-scope, not given the
  // benefit of the doubt (see TWO_YEAR_CUTOFF above).
  const createdDate = toDate(raw.Created_Date);
  if (!createdDate || createdDate < TWO_YEAR_CUTOFF) return null;

  const record: RecordAlertUpload = {};
  record[EXTERNAL_ID_FIELD] = id;

  // Course_RecID → Course_Offering lookup. Left blank (not row-dropped) when
  // "0"/blank or not found in the crosswalk — see header comment.
  const courseRecId = str(raw.Course_RecID);
  let courseExternalId = "";
  if (courseRecId && courseRecId !== "0") {
    const courseId = courseRecIdMap.get(courseRecId);
    if (courseId) {
      record[COURSE_LOOKUP_RELATIONSHIP_FIELD] = courseId;
      courseExternalId = courseId;
    } else {
      missingCourseRecIds.add(courseRecId);
    }
  }

  const title = str(raw.Title);
  if (title) record.Subject = title;

  // Q9 (see header) — Description is capped at 255 chars on this object
  // (confirmed via live describe), not a true Long Text Area as the workbook
  // implied. Truncating here is what unblocks the upload, but it silently
  // discards most of the content for any long Description (see Q9 for why
  // that's a real concern, not just a technicality).
  const description = str(raw.Description)
    .replace(/_4DNL_/g, "\n")
    .trim()
    .slice(0, 255);
  if (description) record.Description = description;

  // Severity has no source column — always "Info" per workbook.
  record.Severity = "Info";

  const effectiveStart = combineDateTime(raw.Created_Date, raw.Created_Time);
  if (effectiveStart) {
    record.EffectiveDate = effectiveStart;
    // Q4 RESOLVED — time-of-day follows the source data (EffectiveDate),
    // not midnight.
    const validUntil = addYears(effectiveStart, VALID_UNTIL_YEARS);
    if (validUntil) record.ValidUntilDate = validUntil;
  }

  // Parent/What lookup — Student_ID, then Associate_ID, then Instructor_ID,
  // whichever is populated and not "0". Q2 RESOLVED: SA confirmed only one
  // is ever populated per row, so this priority order is a defensive
  // fallback that isn't expected to actually trigger.
  let parentLookupField: ParentLookupField | null = null;
  let parentLookupValue = "";

  const studentId = str(raw.Student_ID);
  const associateId = str(raw.Associate_ID);
  const instructorId = str(raw.Instructor_ID);

  if (studentId && studentId !== "0") {
    parentLookupField = "Student_ID_4D__c";
    parentLookupValue = studentId;
  } else if (associateId && associateId !== "0") {
    parentLookupField = "Associate_ID_4D__c";
    parentLookupValue = associateId;
  } else if (instructorId && instructorId !== "0") {
    parentLookupField = "Instructor_ID_4D__c";
    parentLookupValue = instructorId;
  }

  return {
    record,
    externalId: id,
    parentLookupField,
    parentLookupValue,
    categoryName: mapCategoryName(raw.Category),
    courseExternalId,
  };
}

// ── TSV streaming ──────────────────────────────────────────────────────────────

async function streamAndParseTsv(
  fileId: string,
  accessToken: string,
  byteOffset: number,
  maxRows: number,
  knownHeaders: string[],
  courseRecIdMap: Map<string, string>,
  missingCourseRecIds: Set<string>,
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
    const rows: PendingAlertRow[] = [];
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

        const raw: Record<string, string> = {};
        parsedHeaders.forEach((header, i) => {
          raw[header] = row[i] ?? "";
        });

        const mapped = mapAlertRow(
          raw as RawAlertRecord,
          courseRecIdMap,
          missingCourseRecIds,
        );
        if (!mapped) {
          skippedCount++;
          lastCompletedCursor = rowEndByte;
          return;
        }

        if (!firstId) firstId = mapped.externalId;
        lastId = mapped.externalId;
        rows.push(mapped);
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

export const alertImport = flow({
  name: "Alert Import",
  stableKey: "a1e2r3t4-9999-4a1e-8b2c-556677889900",
  description:
    "Reads the Alert TSV from Google Drive, applies the 2-year scope filter " +
    "(Created_Date >= 2024-10-06), maps each row to a RecordAlert record " +
    "(resolving WhatId to a PersonAccount, the Category picklist to a " +
    "RecordAlertCategory lookup, and Course_RecID to a CourseOffering " +
    "lookup), and upserts via Bulk API 2.0. Recurses until the full file is " +
    "processed.",

  onTrigger: async (_context, payload) => ({ payload }),

  onExecution: async (context, params) => {
    const { logger, configVars } = context;

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

    logger.info(`[Alert] Starting at byte offset ${byteOffset}`);

    const gdConn = configVars["Google Drive Connection"];
    const sfConn = configVars["Salesforce Connection"];
    const fileId = (configVars as Record<string, unknown>)[
      "Alert File ID"
    ] as string;
    const failedFolderId = configVars["Failed Records Folder ID"] as
      string | undefined;

    if (!fileId) throw new Error("Alert File ID config var is empty.");

    const gdToken = getAccessToken(gdConn);
    const sfToken = getAccessToken(sfConn);
    const sfInstanceUrl = getSfInstanceUrl(sfConn);

    // Course_RecID → Course_Offering lookup crosswalk (same file/helper every
    // other Course_RecID-resolving flow in this project uses).
    const courseFileId = (configVars as Record<string, unknown>)[
      "Course File ID"
    ] as string;
    if (!courseFileId) {
      throw new Error(
        "Course File ID config var is required for Alert Import (used to resolve Course_RecID).",
      );
    }
    const courseRecIdMap = await loadCourseRecIdMap(courseFileId, gdToken);
    logger.info(
      `[Alert] Loaded ${courseRecIdMap.size} course RecID→ID mappings`,
    );
    const missingCourseRecIds = new Set<string>();

    logger.info(
      `[Alert] Streaming from byte ${byteOffset} of Drive file ${fileId}…`,
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
      courseRecIdMap,
      missingCourseRecIds,
    );

    if (missingCourseRecIds.size > 0) {
      logger.warn(
        `[Alert] ${missingCourseRecIds.size} Course_RecID(s) not found in crosswalk: ` +
          `${[...missingCourseRecIds].join(", ")} — Course_Offering left blank on those rows.`,
      );
    }

    logger.info(
      `[Alert] Parsed ${rows.length} records` +
        ` (hasMore=${hasMore}, nextByte=${nextByteOffset}, skipped=${skippedCount},` +
        ` firstId=${firstId}, lastId=${lastId})`,
    );

    let nextSheetId = sheetId;
    let finalRecords: RecordAlertUpload[] = [];

    if (rows.length > 0) {
      // Resolve Category (RecordAlertCategory), Course (CourseOffering), and
      // Parent/What (PersonAccount) lookups in batch before uploading.
      let categoryMap: Map<string, string>;
      let courseOfferingIdMap: Map<string, string>;
      try {
        [categoryMap, courseOfferingIdMap] = await Promise.all([
          loadRecordAlertCategoryMap(sfInstanceUrl, sfToken),
          loadCourseOfferingIdMap(sfInstanceUrl, sfToken),
        ]);
      } catch (err: unknown) {
        logger.error(
          `[Alert] Failed to query RecordAlertCategory/CourseOffering records.\n${describeSfError(err)}`,
        );
        throw err;
      }

      const studentIds = [
        ...new Set(
          rows
            .filter((r) => r.parentLookupField === "Student_ID_4D__c")
            .map((r) => r.parentLookupValue),
        ),
      ];
      const associateIds = [
        ...new Set(
          rows
            .filter((r) => r.parentLookupField === "Associate_ID_4D__c")
            .map((r) => r.parentLookupValue),
        ),
      ];
      const instructorIds = [
        ...new Set(
          rows
            .filter((r) => r.parentLookupField === "Instructor_ID_4D__c")
            .map((r) => r.parentLookupValue),
        ),
      ];

      let studentMap: Map<string, string>;
      let associateMap: Map<string, string>;
      let instructorMap: Map<string, string>;
      try {
        [studentMap, associateMap, instructorMap] = await Promise.all([
          resolveAccountIdsByField(
            sfInstanceUrl,
            sfToken,
            "Student_ID_4D__c",
            studentIds,
          ),
          resolveAccountIdsByField(
            sfInstanceUrl,
            sfToken,
            "Associate_ID_4D__c",
            associateIds,
          ),
          resolveAccountIdsByField(
            sfInstanceUrl,
            sfToken,
            "Instructor_ID_4D__c",
            instructorIds,
          ),
        ]);
      } catch (err: unknown) {
        logger.error(
          `[Alert] Failed to resolve PersonAccount IDs ` +
            `(studentIds=${studentIds.length}, associateIds=${associateIds.length}, ` +
            `instructorIds=${instructorIds.length}).\n${describeSfError(err)}`,
        );
        throw err;
      }

      let missingParentCount = 0;
      let missingCategoryCount = 0;
      let missingWhatIdCount = 0;

      finalRecords = rows.map((row) => {
        const record = row.record;

        if (row.parentLookupField) {
          const map =
            row.parentLookupField === "Student_ID_4D__c"
              ? studentMap
              : row.parentLookupField === "Associate_ID_4D__c"
                ? associateMap
                : instructorMap;
          const accountId = map.get(row.parentLookupValue);
          if (accountId) {
            // Q10 (see header) — workbook maps this same Account Id to BOTH
            // ParentId and WhatId, but RecordAlert has a validation rule in
            // the target org forbidding ParentId === WhatId. Since both
            // would always be set from the identical Student_ID/Associate_ID/
            // Instructor_ID value on a row, there's no case where they'd
            // ever come out different — so ParentId is skipped entirely and
            // WhatId (required) gets the value.
            // record.ParentId = accountId; // intentionally not set — see Q10
            record.WhatId = accountId;
          } else {
            missingParentCount++;
          }
        } else if (row.courseExternalId) {
          // Q11 (see header) — "course-only" alert (no Student/Associate/
          // Instructor on this row): WhatId falls back to the CourseOffering
          // this alert is about, since WhatId is a required field.
          const courseSfId = courseOfferingIdMap.get(row.courseExternalId);
          if (courseSfId) {
            record.WhatId = courseSfId;
          } else {
            missingWhatIdCount++;
          }
        } else {
          // Q11 (see header), Scenario 3 — no person AND no resolvable
          // course on this row, so there's nothing valid to put in the
          // required WhatId field. Left blank on purpose; Salesforce will
          // reject the row for REQUIRED_FIELD_MISSING and it'll show up in
          // the failed-records sheet, same as any other unmatched lookup.
          missingWhatIdCount++;
        }

        const categoryId = categoryMap.get(row.categoryName);
        if (categoryId) {
          record[CATEGORY_LOOKUP_FIELD] = categoryId;
        } else {
          missingCategoryCount++;
        }

        return record;
      });

      if (missingWhatIdCount > 0) {
        logger.warn(
          `[Alert] ${missingWhatIdCount} row(s) had no person AND no resolvable ` +
            `course — WhatId left blank, these rows will fail REQUIRED_FIELD_MISSING.`,
        );
      }

      if (missingParentCount > 0) {
        logger.warn(
          `[Alert] ${missingParentCount} row(s) had a Student/Associate/Instructor ` +
            `ID with no matching PersonAccount — ParentId/WhatId left blank.`,
        );
      }
      if (missingCategoryCount > 0) {
        logger.warn(
          `[Alert] ${missingCategoryCount} row(s) had a Category with no matching ` +
            `RecordAlertCategory record (checked target org for "${CATEGORY_LOOKUP_FIELD}") — ` +
            `left blank.`,
        );
      }

      const jobResult = await runBulkJob(
        sfInstanceUrl,
        sfToken,
        "RecordAlert",
        EXTERNAL_ID_FIELD,
        finalRecords as Record<string, unknown>[],
        logger,
        "[Alert]",
      );

      try {
        const sheet = await createResultsSheetFromContacts({
          flowName: "Alert Import",
          objectName: "RecordAlert",
          contacts: finalRecords as Record<string, unknown>[],
          externalIdField: EXTERNAL_ID_FIELD,
          failedExternalIds: jobResult.failedExternalIds,
          successfulCsv: jobResult.successfulCsv,
          failedCsv: jobResult.failedCsv,
          accessToken: gdToken,
          folderId: failedFolderId,
          spreadsheetId: sheetId,
        });
        nextSheetId = sheet.spreadsheetId;
        logger.info(`[Alert] Results sheet: ${sheet.url}`);
      } catch (err: unknown) {
        logger.warn(`[Alert] Could not update results sheet: ${String(err)}`);
      }
    } else {
      logger.info(`[Alert] Window contained no records; skipping upload.`);
    }

    if (hasMore) {
      logger.info(
        `[Alert] More rows remain — invoking next iteration at byte ${nextByteOffset}`,
      );
      await (
        context as unknown as {
          invokeFlow(name: string, payload: unknown): Promise<void>;
        }
      ).invokeFlow("Alert Import", {
        byteOffset: nextByteOffset,
        headers: parsedHeaders,
        windowNumber: windowNumber + 1,
        sheetId: nextSheetId,
      });
    } else {
      logger.info(`[Alert] All rows processed — import complete.`);
    }

    return {
      data: {
        byteOffset,
        rowsProcessed: finalRecords.length,
        hasMore,
      },
    };
  },
});

export default [alertImport];
