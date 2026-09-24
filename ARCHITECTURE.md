# Stanford CSP Migration — Architecture & Flow Reference

**Project:** Stanford Continuing Studies Program (CSP) — 4D → Salesforce Education Cloud  
**Tech Stack:** TypeScript (Node.js), Salesforce Bulk API 2.0, Google Sheets (source data), Google Drive (result reporting)  
**Last Updated:** July 2026

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Salesforce Object Model & Relationships](#2-salesforce-object-model--relationships)
3. [Flow Execution Order & Dependencies](#3-flow-execution-order--dependencies)
4. [Flow-by-Flow Reference](#4-flow-by-flow-reference)
   - [Quarter Flow](#41-quarter-flow)
   - [Department Flow](#42-department-flow)
   - [Student Flow](#43-student-flow)
   - [Instructor Flow](#44-instructor-flow)
   - [Associate Flow](#45-associate-flow)
   - [Course Flow](#46-course-flow)
   - [Course Associate & Instructor Staff Detail Flow](#47-course-associate--instructor-staff-detail-flow)
   - [Course Other Cost Flow](#48-course-other-cost-flow)
   - [Course Timecard Flow](#49-course-timecard-flow)
   - [Coursework (Syllabus) Flow](#410-coursework-syllabus-flow)
   - [Enrollment Flow](#411-enrollment-flow)
   - [Enrollment Waiver Flow](#412-enrollment-waiver-flow)
   - [Transcript Request Flow](#413-transcript-request-flow)
   - [Tuition Rule Flow](#414-tuition-rule-flow)
   - [Unit Rule Flow](#415-unit-rule-flow)
   - [Textbook Flow](#416-textbook-flow)
5. [Cross-Cutting Patterns](#5-cross-cutting-patterns)
6. [External ID Strategy](#6-external-id-strategy)
7. [Known Open Items & Decisions](#7-known-open-items--decisions)

---

## 1. High-Level Architecture

```
Google Sheets (4D Source TSV)
        │
        ▼
 TypeScript Flow (Node.js)
  ├─ Reads source rows via HTTP Range streaming
  ├─ Normalizes / validates / deduplicates
  ├─ Resolves parent lookups (SOQL queries or External ID refs)
  ├─ Calls Salesforce Bulk API 2.0 (upsert)
  └─ Writes result sheets to Google Drive
        │
        ▼
 Salesforce Education Cloud (target org)
```

**Core design principles:**
- Every record has a stable `External_ID_4D__c` (or equivalent field) so re-runs are idempotent upserts, not duplicates.
- Parent lookups use Salesforce External ID relationship syntax (`Parent__r.External_ID_4D__c`) wherever possible, so child records do not need the parent's SF Id to be resolved first.
- Large datasets are streamed in byte-range windows (e.g., 2000 rows at a time) so the flow never loads an entire sheet into memory.
- Result CSVs (successes + failures) are uploaded to Google Drive for review after every Bulk API job.

---

## 2. Salesforce Object Model & Relationships

The diagram below shows every SF object created by the migration and how they relate. Arrow direction = child → parent.

```
AcademicYear
    └── AcademicTerm (AcademicYear.External_ID_4D__c)
            └── AcademicSession (AcademicTerm.Code__c)

Account [non-person] — Department categories & departments
    └── Account (Parent.External_ID_4D__c)   ← self-referential; category → sub-category → dept

Account [person] — Students / Instructors / Associates
    └── PersonEmployment (RelatedPersonId / AccountId)   ← uses SF Id (no ext ID on Contact)

Learning
    └── LearningCourse (Learning.External_ID_4D__c)
            └── CourseOffering (LearningCourse, AcademicSession, Dept Account, Tuition_Rule__c)
                    ├── CourseOfferingSchedule (CourseOfferingId)
                    ├── Course_Department__c  (Course_Offering__r, Child_Dept, Parent_Dept)
                    ├── CourseOfferingParticipant — Instructor / Associate (CourseOffering + Contact)
                    ├── CourseOfferingParticipant — Student Enrollment (CourseOffering + Contact + Account)
                    ├── Course_Offering_Staff_Detail__c (CourseOffering, Instructor/Associate Contact, PersonEmployment)
                    │       └── Course_Offering_Staff_Timecard__c (Staff_Detail__r.External_ID_4D__c)
                    ├── CourseOfferingExpense__c (CourseOffering__r.External_ID_4D__c)
                    ├── Textbook_Association__c (CourseOffering__r, Textbook__r)
                    └── Syllabus_Association__c (Course_ID__r, Syllabus__r)

CourseOfferingParticipant [Student]
    └── Course_Offering_Participant_Waiver__c (Enrollment_ID__c → CourseOfferingParticipant)

Textbook__c  (standalone; linked via Textbook_Association__c)
Syllabus__c  (standalone; linked via Syllabus_Association__c)
Tuition_Rule__c  (standalone; referenced by CourseOffering)
Unit_Rule__c     (standalone)
CSP_Transcript_Request__c  (standalone)
```

### Object-level relationship table

| Child Object | Lookup Field | Parent Object | How Linked |
|---|---|---|---|
| AcademicTerm | `AcademicYear.External_ID_4D__c` | AcademicYear | External ID ref |
| AcademicSession | `AcademicTerm.Code__c` | AcademicTerm | External ID ref |
| Account (dept) | `Parent.External_ID_4D__c` | Account (category) | External ID ref |
| PersonEmployment | `RelatedPersonId` / `AccountId` | Account (person) | **SF Id** (resolved post-upsert) |
| LearningCourse | `Learning.External_ID_4D__c` | Learning | External ID ref |
| CourseOffering | `LearningCourse.External_ID_4D__c` | LearningCourse | External ID ref |
| CourseOffering | `AcademicSession.Abbreviation__c` | AcademicSession | External ID ref |
| CourseOffering | `Department__r.External_ID_4D__c` | Account (dept) | External ID ref |
| CourseOffering | `Tuition_Rule__r.External_ID_4D__c` | Tuition_Rule__c | External ID ref |
| CourseOffering | `PrimaryFacultyId` | Contact | **SF Id** (resolved via SOQL) |
| CourseOffering | `Coordinator__c` | Contact | **SF Id** (resolved via SOQL) |
| CourseOffering | `ProviderId` | Account (dept) | **SF Id** (resolved via SOQL) |
| CourseOfferingSchedule | `CourseOfferingId` | CourseOffering | **SF Id** (resolved same batch) |
| Course_Department__c | `Course_Offering__r.External_ID_4D__c` | CourseOffering | External ID ref |
| Course_Department__c | `Child_Department__r.External_ID_4D__c` | Account (dept) | External ID ref |
| Course_Department__c | `Parent_Department__r.External_ID_4D__c` | Account (category) | External ID ref |
| CourseOfferingParticipant (instr) | `CourseOfferingId` | CourseOffering | **SF Id** |
| CourseOfferingParticipant (instr) | `ParticipantContactId` | Contact | **SF Id** |
| CourseOfferingParticipant (student) | `CourseOfferingId` | CourseOffering | **SF Id** (cached) |
| CourseOfferingParticipant (student) | `ParticipantContactId` | Contact | **SF Id** (PersonContactId cached) |
| CourseOfferingParticipant (student) | `ParticipantAccountId` | Account (person) | **SF Id** (cached) |
| CourseOfferingParticipant (student) | `AcademicTermEnrollment.External_ID_4D__c` | AcademicTermEnrollment | External ID ref |
| Course_Offering_Staff_Detail__c | `Course_Offering__r.External_ID_4D__c` | CourseOffering | External ID ref |
| Course_Offering_Staff_Detail__c | `Staff_Member__r.Instructor_ID_4D__c` | Contact (instructor) | External ID ref |
| Course_Offering_Staff_Detail__c | `Staff_Member__r.Associate_ID_4D__c` | Contact (associate) | External ID ref |
| Course_Offering_Staff_Detail__c | `Person_Employment__r.External_ID_4D__c` | PersonEmployment | External ID ref |
| Course_Offering_Staff_Timecard__c | `Course_Offering_Staff_Detail__r.External_ID_4D__c` | Course_Offering_Staff_Detail__c | External ID ref |
| CourseOfferingExpense__c | `CourseOffering__r.External_ID_4D__c` | CourseOffering | External ID ref |
| Textbook_Association__c | `CourseOffering__r.External_ID_4D__c` | CourseOffering | External ID ref |
| Textbook_Association__c | `Textbook__r.External_ID_4D__c` | Textbook__c | External ID ref |
| Syllabus_Association__c | `Course_ID__r.External_ID_4D__c` | CourseOffering | External ID ref |
| Syllabus_Association__c | `Syllabus__r.External_ID_4D__c` | Syllabus__c | External ID ref |
| Course_Offering_Participant_Waiver__c | `Enrollment_ID__c` | CourseOfferingParticipant | **SF Id** (SOQL by ENR-{id}) |

> **Note on SF Id usage:** Fields marked **SF Id** require a pre-flight SOQL query to resolve the Salesforce record Id before the Bulk API job. This is because the target lookup field does not support an External ID relationship reference. All other parents use the `Relationship__r.External_ID_4D__c` pattern so Salesforce resolves them during the Bulk API job itself.

---

## 3. Flow Execution Order & Dependencies

Flows must run in this order. A flow in a later group depends on records from all earlier groups.

```
Group 1 — No dependencies (run in any order or in parallel)
  ├── Quarter Flow        → AcademicYear, AcademicTerm, AcademicSession
  ├── Department Flow     → Account (categories + departments)
  ├── Student Flow        → Account (person / student)
  ├── Instructor Flow     → Account (person / instructor) + PersonEmployment
  ├── Associate Flow      → Account (person / associate)  + PersonEmployment
  ├── Tuition Rule Flow   → Tuition_Rule__c
  └── Unit Rule Flow      → Unit_Rule__c

Group 1b — Depends on Student Flow (Group 1)
  └── Account Login Flow  → User (portal login, resolved via Account.Student_ID_4D__c)

Group 2 — Depends on Group 1
  └── Course Flow         → Learning, LearningCourse, Location, CourseOffering,
                            CourseOfferingSchedule, Course_Department__c,
                            CourseOfferingParticipant (instructors + associates)

Group 3 — Depends on Group 2 (CourseOffering must exist)
  ├── Course Assoc/Instr Staff Detail Flow → Course_Offering_Staff_Detail__c
  ├── Course Other Cost Flow               → CourseOfferingExpense__c
  ├── Coursework (Syllabus) Flow           → Syllabus__c + Syllabus_Association__c
  └── Textbook Flow                        → Textbook__c + Textbook_Association__c

Group 4 — Depends on Groups 1+2+3
  ├── Course Timecard Flow    → Course_Offering_Staff_Timecard__c  (needs Staff Detail)
  └── Enrollment Flow         → CourseOfferingParticipant (students)

Group 5 — Depends on Group 4
  └── Enrollment Waiver Flow  → Course_Offering_Participant_Waiver__c (needs Enrollment)

Group 6 — Standalone (can run any time after Group 1)
  └── Transcript Request Flow → CSP_Transcript_Request__c
```

---

## 4. Flow-by-Flow Reference

---

### 4.1 Quarter Flow

**File:** `src/quarterFlow.ts`  
**Source:** Quarter Google Sheet (TSV)  
**Purpose:** Seeds the academic calendar — years, terms, and sessions — that every course and enrollment references.

#### Objects Created

**AcademicYear**
| Field | Value / Source |
|---|---|
| `External_ID_4D__c` | From source ID |
| `Name` | Format: `"2025-2026"` |
| `Year` | 4-digit academic start year |

**AcademicTerm** (parent: AcademicYear)
| Field | Value / Source |
|---|---|
| `External_ID_4D__c` | From source |
| `Code__c` | Upsert key |
| `Name` | `"Fall 2025"`, `"Winter 2026"`, etc. |
| `Season` | Normalized: `Fall` / `Winter` / `Spring` / `Summer` |
| `IsActive` | Boolean from source |
| `RegistrationOpenDate` | ISO date (optional) |
| `Abbreviation__c` | Short code, e.g. `su23` |
| **`AcademicYear.External_ID_4D__c`** | **Parent link — external ID ref** |

**AcademicSession** (parent: AcademicTerm)
| Field | Value / Source |
|---|---|
| `External_ID_4D__c` | From source |
| `Code__c` | Upsert key |
| `Name` | From source |
| `Season`, `IsActive`, `Abbreviation__c` | Same as AcademicTerm |
| `Web_Launch_Date__c` | ISO datetime |
| `ClassStartDate`, `ClassEndDate` | ISO datetime |
| **`AcademicTerm.Code__c`** | **Parent link — external ID ref** |

**Academic Year Logic:**
- Fall quarter (e.g. `fa26`) defines year `"2026"` → `"2026-2027"`.
- Winter/Spring/Summer that follow (e.g. `wi27`, `sp27`, `su27`) belong to academic year `"2026"` (not 2027).

---

### 4.2 Department Flow

**File:** `src/departmentFlow.ts`  
**Source:** Department Google Sheet (TSV)  
**Purpose:** Creates the org-chart of departments as non-person Accounts with a parent-child hierarchy.

#### Objects Created

**Account — Category (parent departments)**
| Field | Value / Source |
|---|---|
| `External_ID_4D__c` | Format: `dept_cat_{id}` (special: `dept_cat_0` = "Default Department") |
| `Name` | From source |
| `Parent.External_ID_4D__c` | Optional; links to parent category (external ID ref) |

**Account — Department (leaf departments)**
| Field | Value / Source |
|---|---|
| `External_ID_4D__c` | Format: `dept_{id}` |
| `Name` | From source |
| `Parent.External_ID_4D__c` | Links to category Account (external ID ref) |

**Processing order:** Category accounts are upserted first, then departments (because departments reference categories as their Parent).

---

### 4.3 Student Flow

**File:** `src/studentFlow.ts`  
**Source:** Student Google Sheet (TSV) — streamed in 2,000-row windows  
**Purpose:** Creates one Person Account per student with all demographic and preference fields.

#### Objects Created

**Account (Person Account)**  
Upsert key: `Student_ID_4D__c`

| Field | Notes |
|---|---|
| `Student_ID_4D__c` | Primary external ID |
| `Student_ID_4D__pc` | Person account copy |
| `LastName`, `FirstName`, `MiddleName` | |
| `PersonBirthdate` | MM/DD/YYYY → ISO; `"00/00/00"` → null |
| `PersonEmail` | Validated; invalid → `""` |
| `PersonHasOptedOutOfEmail` | Inverse of `Opt_In_Email` |
| `PersonOtherPhone` | Evening phone; fallback to daytime |
| `PersonMailingStreet/City/State/Country/PostalCode` | State/Country normalized to full names |
| `Legal_Sex__pc` | Picklist: `M`, `F`, `D`, `N` |
| `Highest_Degree__pc`, `TA_Discount_Type__pc` | Normalized picklists |
| `University_ID__pc`, `SAA_Number__pc`, `SBSAA_Number__pc` | |
| `Do_Not_Enroll__pc`, `Alert_Flag__pc` | Boolean |
| `Certificate_Issued__pc`, `Certificate_Issued_Date__pc` | |
| `FERPA_Directory_Consent__pc`, `Discount_Lifetime__pc` | Boolean |
| `Total_Enrollments__pc`, `First_Quarter__pc`, `Most_Recent_Quarter__pc` | |
| `Description` | Notes; `_4DNL_` → newline |

**Excluded fields (architect decision):** `US_Citizen__pc`, `Stanford_Alumnus__pc`, `Ethnicity__pc`

---

### 4.3a Account Login Flow

**File:** `src/accountLoginFlow.ts`
**Source:** Account Login TSV (4D) — streamed in 2,000-row windows
**Purpose:** Creates one Salesforce `User` per portal login credential, for the Student community/portal profile. **Must run after Student Flow** — every row is resolved against the Account created there.
**Reference:** "CSP Data Migration Mapping Workbook - Account Login.pdf"

#### Objects Created

**User**
Upsert key: `FederationIdentifier`

| Field | Source | Notes |
|---|---|---|
| `FederationIdentifier` | `Student_ID` | Direct map |
| `ContactId` | `Student_ID` | Conversion — lookup via `Account.Student_ID_4D__c` → `PersonContactId` |
| `IsActive` | `Inactive` | **Inverted** — see note below |
| `Username`, `Email` | — | Conversion — the matched Contact's email (not the source `Username` column) |
| `FirstName`, `LastName` | — | Conversion — from the matched Contact |
| `Alias` | — | Conversion — `LOWER(LEFT(FirstName,1) & LEFT(LastName,7))` |
| `ProfileId` | — | Direct map, default `"Student_Profile"` |
| `LocaleSidKey` | — | Direct map, default `"en_US"` |
| `LanguageLocaleKey` | — | Direct map, default `"en_US"` |
| `TimeZoneSidKey` | — | Direct map, default `"America/Los_Angeles"` |
| `EmailEncodingKey` | — | Direct map, default `"UTF-8"` |

**Do Not Map:** `Instructor_ID`, `Associate_ID`, `ID`, `Last_Login_Date`, `Password_Hash_Char`, `Created_Date/Time/By`, `Last_Modified_Date/Time/By`, `Student_ID_Previous`, `Override_Hash_Storage/Start_Date/Start_Time/History`, `Previous_Hash_Chars`.

**Row-skip rules:** no `Student_ID` or `Student_ID = 0`; no matching Account found for `Student_ID`; matched Contact has no usable email.

**Open item — Active/Inactive semantics:** the mapping doc calls this "Direct Map" from source `Inactive` to target `Active`, noting "records are only marked as inactive if they are duplicate." A literal same-value copy would leave every normal (non-duplicate) record `IsActive = false`, so this flow inverts the value (`IsActive = !Inactive`) to match the note's evident intent. **Needs architect confirmation before go-live.**

**Prerequisites:** a Profile named `Student_Profile` must exist in the org; `User.FederationIdentifier` is used as the Bulk API 2.0 upsert key (Salesforce natively supports this for User).

---

### 4.4 Instructor Flow

**File:** `src/instructorFlow.ts`  
**Source:** Instructor Google Sheet (TSV) — streamed in 3,000-row windows  
**Purpose:** Creates Person Account for each instructor, with optional PersonEmployment for bio/contract data. Deduplicates against existing Student and Associate accounts.

#### Pre-Match Deduplication Logic
Before upsert, each instructor row is queried against existing SF Accounts by email:
- Email + name (forward or swapped) matches a **Student** → upsert using `Student_ID_4D__c` as key.
- Email + name matches an **Associate** → upsert using `Associate_ID_4D__c` as key.
- No match → upsert using `Instructor_ID_4D__c` as key (new or existing instructor).

This ensures one Person Account per real person, even if they appear in all three source systems.

#### Objects Created

**Account (Person Account)**  
Upsert key: `Instructor_ID_4D__c` (unless deduped; see above)

| Field | Notes |
|---|---|
| `Instructor_ID_4D__c` | Always set, even when upsert key is Student/Associate ID |
| `Instructor_ID_4D__pc` | Person account copy |
| `LastName`, `FirstName`, `MiddleName`, `Suffix` | |
| `PersonTitle` | Max 80 chars |
| `PersonDepartment` | |
| `PersonEmail`, `Phone`, `PersonHomePhone`, `PersonMobilePhone` | |
| `PersonMailingStreet/City/State/Country/PostalCode` | |
| `SUNet_Id__pc`, `Username_4D__pc` | |
| `Active__pc`, `Do_Not_Contact__pc`, `Alert_Flag__pc` | Boolean |
| `University_ID__pc`, `Student_ID__pc` | |
| `First_Quarter__pc`, `Most_Recent_Quarter__pc` | |
| `Description` | Notes; `_4DNL_` → newline |

**PersonEmployment** (if bio or employment data present)  
Upsert key: `External_ID_4D__c` = `instructor_{id}`

| Field | Notes |
|---|---|
| `External_ID_4D__c` | `instructor_{id}` |
| `Name` | Formatted name |
| `RelatedPersonId` | **SF Id** — resolved from Account after Account upsert |
| `AccountId` | **SF Id** — same as RelatedPersonId |
| `Instructor_Bio__c` | `_4DNL_` → newline |
| `Legal_Name__c` | |
| `Out_Of_State_Fee__c`, `AC_Pay__c` | Boolean |
| `Email_for_Students__c` | |
| `Harassment_Training__c` | Date |
| `Salary_Category__c` | Normalized (`"Contigent"` → `"Cont."`) |
| `Cont_Hourly_Rate__c`, `Cont_Estimated_Hours__c` | Numeric |
| `Total_Courses__c` | Integer |
| `Contract_Note__c`, `Coordinator_Status__c` | |

> **Why SF Id for PersonEmployment:** `PersonEmployment.RelatedPersonId` and `AccountId` are standard lookup fields with no external ID relationship support, so the Account upsert must complete first, then the returned SF Id is injected before the PersonEmployment Bulk API job.

---

### 4.5 Associate Flow

**File:** `src/associateFlow.ts`  
**Source:** Associate Google Sheet (TSV) — streamed in 3,000-row windows  
**Purpose:** Same structure as Instructor flow — creates Person Account + optional PersonEmployment for associates (staff who assist instructors).

#### Pre-Match Deduplication
Same logic as instructor: check by email against existing Student → Instructor → fallback to Associate ID as upsert key.

#### Objects Created

**Account (Person Account)** — Upsert key: `Associate_ID_4D__c` (unless deduped)

All address/contact/boolean fields same as Instructor. Additional fields:

| Field | Notes |
|---|---|
| `Associate_ID_4D__c` | Always set |
| `Associate_ID_4D__pc` | |
| `Stanford_Mail_Code__pc` | |
| `Course_Solicitations__pc` | Boolean |
| `Email_Address_For_Students__pc` | |
| `Total_Courses__pc` | Integer; excluded if 0 |
| `Salary_Category__pc`, `Harassment_Training_Complete__pc` | |

**PersonEmployment** — Upsert key: `External_ID_4D__c` = `associate_{id}`  
Same pattern as instructor. Only created if `Salary_Category` has a value.

---

### 4.6 Course Flow

**File:** `src/courseFlow.ts`  
**Source:** Course Google Sheet (TSV) — streamed in 20–100 row windows (REST API, row by row)  
**Purpose:** Most complex flow. Creates the full course catalog: Learning → LearningCourse → CourseOffering → Schedule, Departments, Participants.

**Quarter filter:** Only processes rows whose quarter code falls within `ANCHOR_QUARTER ± YEARS_BACK` (default: `wi25 ± 2 years`).

#### Objects Created (in order within each row)

**1. Learning**  
Upsert key: `External_ID_4D__c` = base course code (e.g. `OWC 303`)

| Field | Notes |
|---|---|
| `Name` | Max 255 chars |
| `Type` | Hardcoded: `"LearningCourse"` |
| `IsActive` | `true` |
| `External_ID_4D__c` | Base code |
| `ProviderId` | **SF Id** — Account Id for department (resolved by SOQL using dept external ID) |

**2. LearningCourse**  
Upsert key: `External_ID_4D__c` = base course code

| Field | Notes |
|---|---|
| `Name` | |
| `CourseNumber` | Base code with spaces removed |
| `Catalog_Notes__c` | |
| `Description` | Max 32,000 chars |
| `Textbooks__c` | Max 255 chars |
| `Learning.External_ID_4D__c` | Parent link — external ID ref |

**3. Location** (if building/room data exists)  
Deduplication by name before creation.

| Field | Notes |
|---|---|
| `Name` | `"{building} - Rm {room}"` or just `"{building}"` |

**4. CourseOffering**  
Upsert key: `External_ID_4D__c` = source row ID (e.g. `20253_OWC 303 A`)

| Field | Notes |
|---|---|
| `LearningCourse.External_ID_4D__c` | Parent — external ID ref |
| `AcademicSession.Abbreviation__c` | Parent — external ID ref; pre-2013 quarters remapped to `fa13`/`wi13` |
| `Department__r.External_ID_4D__c` | Offering-specific dept — external ID ref |
| `Tuition_Rule__r.External_ID_4D__c` | External ID ref |
| `PrimaryFacultyId` | **SF Id** — Contact Id of primary instructor |
| `Coordinator__c` | **SF Id** — Contact Id (tries instructor lookup first, then associate) |
| `Name`, `SectionNumber` | |
| `Enrollment_Status__c` | Normalized |
| `StartDate`, `EndDate`, `Drop_By_Date__c` | Dates |
| `EnrollmentCapacity`, `MaxEnrollments__c` | Integer |
| `Units__c`, `Duration_Value__c`, `Duration_Unit__c` | |
| `Tuition_Amount__c`, `Additional_Fee__c`, `Other_Costs__c` | Currency |
| `Format__c` | Derived from Format/Hybrid columns or section suffix |
| `Zoom_URL__c`, `Zoom_Password__c` | |
| `Staff_Notes__c`, `Instructor_Notes__c`, `Catalog_Notes__c` | |
| `Grade_Restriction__c`, `Recording__c`, `Canvas_Publish__c` | |
| Many more offering-level flags | (see source file for full list) |

**5. CourseOfferingSchedule** (if weekday/time present)

| Field | Notes |
|---|---|
| `CourseOfferingId` | **SF Id** — from CourseOffering upsert result |
| `Monday__c` … `Sunday__c` | Boolean weekday flags |
| `StartTime`, `EndTime` | Time of day; EndTime > StartTime required |
| `LocationId` | **SF Id** — from Location lookup |

**6. Course_Department__c** (junction)  
Upsert key: `External_ID_4D__c` = `CDEPT-{id}`. Only rows where `IsPrimary = true`.

| Field | Notes |
|---|---|
| `Course_Offering__r.External_ID_4D__c` | External ID ref |
| `Child_Department__r.External_ID_4D__c` | External ID ref |
| `Parent_Department__r.External_ID_4D__c` | External ID ref (uses parent dept if available) |
| `IsPrimary__c` | Boolean |

**7. CourseOfferingParticipant — Instructors & Associates**  
One record per instructor/associate junction row, deduplicated by (CourseOfferingId + ContactId + Affiliation).

| Field | Notes |
|---|---|
| `CourseOfferingId` | **SF Id** |
| `ParticipantContactId` | **SF Id** — resolved Contact Id |
| `ParticipantAffiliation` | `"Instructor"` or `"Associate"` |
| `IsPrimary__c` | Boolean |
| `External_ID_4D__c` | `CINST-{id}` or `CASSOC-{id}` (optional) |

---

### 4.7 Course Associate & Instructor Staff Detail Flow

**File:** `src/courseAssociateInstructorFlow.ts`  
**Source:** Course_Instructor and Course_Associate Google Sheets  
**Purpose:** Creates `Course_Offering_Staff_Detail__c` — the pay/contract record linking a person's employment to a specific course offering. Prerequisite: Course Flow (CourseOfferings and PersonEmployments must exist).

**Course Associate prerequisite:** Requires the Course file to build a `Course_RecID → Course_ID` crosswalk since Course_Associate rows reference courses by RecID not by source ID.

#### Object Created

**Course_Offering_Staff_Detail__c**

| Field | Notes |
|---|---|
| `External_ID_4D__c` | `CINST-SD-{id}` (instructor) or `CASSOC-SD-{id}` (associate) |
| `Course_Offering__r.External_ID_4D__c` | External ID ref (CourseOffering) |
| `Staff_Member__r.Instructor_ID_4D__c` | External ID ref (Instructor Contact) |
| `Staff_Member__r.Associate_ID_4D__c` | External ID ref (Associate Contact) |
| `Person_Employment__r.External_ID_4D__c` | External ID ref (`instructor_{id}` or `associate_{id}`) |
| `Estimated_Comp__c`, `Gross_Pay__c`, `Hi_Enroll_Bonus__c` | Currency |
| `Classroom_Hours__c`, `Cont_Hourly_Rate__c`, `Cont_Estimated_Hours__c` | Numeric |
| `Salary_Category__c` | Normalized |
| `Additional_Comp_Other__c` | From `True_Up_Bonus` |
| `Contract_Note__c` | |
| `Hire_Date__c`, `Term_Date__c`, `Offer_Letter_Received__c` | Dates |
| `Notes__c`, `Job_Record_Number__c` | |
| `Speaking_Date__c` | Associates only |

---

### 4.8 Course Other Cost Flow

**File:** `src/courseOtherCostFlow.ts`  
**Source:** Course_Other_Cost Google Sheet — streamed in 1,000-row windows  
**Purpose:** Creates expense records for miscellaneous per-offering costs.

**Prerequisite:** Course file for `Course_RecID → Course_ID` crosswalk.

#### Object Created

**CourseOfferingExpense__c**

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source ID |
| `CourseOffering__r.External_ID_4D__c` | External ID ref |
| `Actual_Cost__c` | Currency |
| `Description__c` | Text |

---

### 4.9 Course Timecard Flow

**File:** `src/courseTimecardFlow.ts`  
**Source:** Timecard Google Sheet — streamed in 1,000-row windows  
**Purpose:** Creates weekly timecard records for instructor/associate hours against a specific staff detail record.

**Filter:** Only rows with a valid 4D quarter code (format `YYYYD`, D = 1–4).

#### Object Created

**Course_Offering_Staff_Timecard__c**

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source ID |
| `Course_Offering_Staff_Detail__r.External_ID_4D__c` | `CINST-SD-{id}` or `CASSOC-SD-{id}` — external ID ref |
| `Pay_Period_End__c` | Date (MM/DD/YYYY → ISO) |
| `Job_Number__c` | Text; `0` = primary |
| `Hours_Regular__c`, `Hours_Overtime__c`, `Hours_Double_OT__c`, `Hours_Sick__c` | Numeric |

---

### 4.10 Coursework (Syllabus) Flow

**File:** `src/courseworkFlow.ts`  
**Source:** Coursework Google Sheet — streamed in 1,000-row windows  
**Filter:** `Syllabus = true` AND Course_ID prefix within 2-year quarter window.

#### Objects Created (in order)

**Syllabus__c** (upserted first)

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source ID |
| `Name` | Normalized (typos `"syllbaus"`, `"syllaubs"` → `"Syllabus"`) |
| `URL__c` | Optional |

**Syllabus_Association__c** (after Syllabus upsert)

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source ID |
| `Course_ID__r.External_ID_4D__c` | External ID ref (CourseOffering) |
| `Syllabus__r.External_ID_4D__c` | External ID ref |

---

### 4.11 Enrollment Flow

**File:** `src/enrollmentFlow.ts`  
**Source:** Enrollment Google Sheet (TSV) — single-pass streaming  
**Purpose:** Creates one `CourseOfferingParticipant` per student enrollment with financial and grade data.

#### Deduplication Strategy
Rows are streamed and deduplicated in memory by `(Registration_ID, Student_ID, Course_ID)`. When duplicates exist, the row with the highest **status rank** wins:

| Rank | Status |
|---|---|
| 10 | Drop w/ Refund variants |
| 9 | Drop No Refund |
| 8 | Drop - Pending |
| 7 | Drop Transferred |
| 5 | Drop |
| 4 | UnEnrolled |
| 1 | Enrolled / Wait List |
| 0 | New — **excluded entirely** (abandoned carts per business decision) |

**Excluded OPEN statuses (pending decisions):** `Adjustment`, `DupEnrollment`, `Course cancel`, `Cancelled - Refunded`, `Cancelled - Pending`

#### Pre-Flight Caches Built Before Bulk Job
- **Student cache:** `Student_ID_4D__c → { PersonContactId, Account.Id }` — SOQL query at startup.
- **CourseOffering cache:** `External_ID_4D__c → CourseOffering.Id` — SOQL query at startup.

#### Object Created

**CourseOfferingParticipant (Student Enrollment)**  
Upsert key: `External_ID_4D__c` = `ENR-{enrollment_id}`

| Field | Notes |
|---|---|
| `External_ID_4D__c` | `ENR-{id}` |
| `AcademicTermEnrollment.External_ID_4D__c` | Registration ID — **external ID ref** |
| `ParticipantContactId` | **SF Id** (PersonContactId from student cache) |
| `ParticipantAccountId` | **SF Id** (Account.Id from student cache) |
| `CourseOfferingId` | **SF Id** (from CourseOffering cache) |
| `ParticipantAffiliation` | Hardcoded: `"Student"` |
| `ParticipationStatus` | `"Enrolled"`, `"Waitlisted"`, `"Dropped"`, etc. |
| `Grade_Option__c` | `"NGR"`, `"Credit/No Credit"`, `"Letter Grade"`, `"Inst S/NC"` |
| `Grade__c` | Normalized (`"AUD"` → `"AU"`, `"CRA"` → `"CR"`, etc.) |
| `Grade_Entered_Date__c` | |
| `Tuition__c`, `Fee__c`, `Extension__c` | Currency; zero preserved |
| `TA_Discount__c` | Currency; stored as negative per architect decision |
| `STAP_Applied__c` | Currency; zero → null |
| `Add_Drop__c` | `"Add"`, `"Drop"`, `"Adjustment"` |
| `Enrollment_Date__c`, `RegistrationDateTime` | |
| `Survey_Response_Date__c`, `Survey_Response_Time__c` | |
| `Summary` | Notes; max 32,000 chars |
| `CreatedDate`, `LastModifiedDate` | Requires "Set Audit Fields" org permission |

---

### 4.12 Enrollment Waiver Flow

**File:** `src/enrollmentWaiverFlow.ts`  
**Source:** Enrollment_Waiver Google Sheet — streamed in 500-row windows  
**Optional:** Waiver Reference file (name/ID lookup table, ~15 entries)

#### Object Created

**Course_Offering_Participant_Waiver__c**  
Upsert key: `External_ID_4D__c` = source ID

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source ID |
| `Enrollment_ID__c` | **SF Id** — resolved by SOQL lookup on `CourseOfferingParticipant` using `ENR-{id}` prefix |
| `Historical_Waiver__c` | `"{name} ({id})"` if reference loaded; else just the ID |
| `Date_Time_New__c` | Parsed from `YYYYMMDDHHMMSS` → ISO DateTime |
| `Verification__c` | Loaded as-is (may contain IP addresses) |
| `Option__c` | Text; default `"ACCEPT"` |

---

### 4.13 Transcript Request Flow

**File:** `src/transcriptRequestFlow.ts`  
**Source:** Transcript_Request Google Sheet — single-pass streaming  
**Filter:** `Request_Date >= 2024-07-07` (2-year window)

#### Object Created

**CSP_Transcript_Request__c** — Standalone; no parent lookups.  
Upsert key: `External_ID_4D__c` = source ID

| Field | Notes |
|---|---|
| `Request_Date__c` | Date/time; midnight UTC |
| `Student_ID__c`, `Last_Name__c`, `First_Name__c` | |
| `Email_Address__c` | Cleaned/validated |
| `Student_ID_Longint__c`, `Transaction_ID__c` | |
| `Transcript_Format__c` | |
| `Entry_1__c` – `Entry_5__c` | Text(255); `_4DNL_` → newline |
| `Quantity_1__c` – `Quantity_5__c`, `Quantity_Total__c` | Text(255) |
| `Unfullfilled_Quantity__c`, `Unfullfilled_Notes__c` | |
| `Sent_Date__c`, `Transaction_ID_Date_Entered__c` | |
| `Amount__c` | Currency (near 0% populated in 2-year window) |

---

### 4.14 Tuition Rule Flow

**File:** `src/tuitionruleFlow.ts`  
**Source:** Tuition Rule Google Sheet — single-pass fetch  
**Purpose:** Seeds the tuition pricing matrix referenced by CourseOfferings.

#### Object Created

**Tuition_Rule__c** — Standalone (CourseOffering references this).  
Upsert key: `External_ID_4D__c` = source ID

| Field | Notes |
|---|---|
| `Name` | Required |
| `External_ID_4D__c` | Source ID |
| `Status__c` | `"Active"` or `"Inactive"` (inferred from name containing "INACTIVE") |
| `Rate_Type__c` | |
| `Base_Amount__c`, `Rate_Per_Unit__c` | Currency |
| `Limit_Surcharge_31_39__c` … `Limit_Surcharge_18_Under__c` | Currency |
| `Tuition_Cap__c` | Currency |
| `Comments__c` | `_4DNL_` → newline |

---

### 4.15 Unit Rule Flow

**File:** `src/unitRuleFlow.ts`  
**Source:** Unit Rule Google Sheet — single-pass fetch

#### Object Created

**Unit_Rule__c** — Standalone.  
Upsert key: `External_ID_4D__c` = source ID

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source ID |
| `Rule_Type__c` | |
| `Priority__c`, `Maximum_Threshold__c`, `Units_Awarded__c` | Numeric |

**Intentionally not mapped:** `Format`, `Department_ID` (per architect/Liz decision, May 2026)

---

### 4.16 Textbook Flow

**File:** `src/textbookFlow.ts`  
**Source:** Textbook Google Sheet — single-pass streaming  
**Filters:**
- Course_ID prefix within 2-year quarter window.
- Exclude ID = `"1"` (junk seed row).
- Exclude titles matching: `"Course Reader"`, `"No Required Textbook"`, `"No Textbook"`.

#### Textbook Deduplication
Across all rows, textbooks are deduplicated so one `Textbook__c` per unique book:
- **Primary key:** ISBN (if present).
- **Fallback:** `title_slug + author_slug` (for ~0.8% of rows with no ISBN).

#### Objects Created (in order)

**Textbook__c** (upserted first, batched)  
Upsert key: `External_ID_4D__c` = ISBN or `NOISBN_{title_slug}_{author_slug}`

| Field | Notes |
|---|---|
| `External_ID_4D__c` | See above |
| `Name` | Title; max 80 chars |
| `Title__c` | Full title |
| `Author__c` | `"(Required)"`/`"(Recommended)"` prefixes stripped |
| `ISBN__c` | Cleaned; trailing `_4DNL_` stripped |

**Textbook_Association__c** (after Textbook upsert)  
Upsert key: `External_ID_4D__c` = source row ID

| Field | Notes |
|---|---|
| `External_ID_4D__c` | Source row ID |
| `CourseOffering__r.External_ID_4D__c` | External ID ref |
| `Textbook__r.External_ID_4D__c` | External ID ref |
| `Required_Optional__c` | `"Required"` / `"Optional"` / omitted |

---

## 5. Cross-Cutting Patterns

### 5.1 Streaming (HTTP Range Windows)
Large source sheets are read in byte-range windows (not loaded fully into memory). Each flow invocation processes one window and then calls itself recursively with `hasMore = true` to fetch the next window. Window sizes vary by flow (500 – 3,000 rows).

### 5.2 Bulk API 2.0
All SF writes go through Salesforce Bulk API 2.0 (`runBulkJob` in `utils.ts`). This means:
- Records are sent as CSV batches.
- Partial success is allowed (failed rows are captured for re-run).
- Result CSVs (success + failure) are uploaded to Google Drive after each job.

### 5.3 Idempotency
Every object has a stable external ID derived from the source system's own ID. Re-running a flow upserts (not duplicates) existing records. Safe to re-run after failures.

### 5.4 Field Normalization (shared across all flows)
| Data Type | Normalization Rule |
|---|---|
| Newlines | `_4DNL_` token → `\n` |
| Dates | `MM/DD/YYYY` → `YYYY-MM-DD` ISO; `"00/00/00"` → null |
| Booleans | `"true"`, `"1"`, `"Y"` → `true`; absent/empty → `false` or `#N/A` |
| Country | Full country name (US state codes mapped to full names only when country = US) |
| Currency | Zero preserved where meaningful; `TA_Discount__c` stored negative |
| Picklists | `"Contigent"` → `"Cont."` (salary typo); gender normalization; enrollment status normalization |

---

## 6. External ID Strategy

Every object has a `External_ID_4D__c` custom field as the upsert key. The naming convention makes it easy to trace any SF record back to its 4D source:

| Prefix / Format | Object | Example |
|---|---|---|
| (raw 4D ID) | AcademicYear, AcademicTerm, AcademicSession | `"12345"` |
| `dept_cat_{id}` | Account (department category) | `dept_cat_7` |
| `dept_cat_0` | Account ("Default Department") | `dept_cat_0` |
| `dept_{id}` | Account (department leaf) | `dept_42` |
| `Student_ID_4D__c` | Account (student) | Person account field |
| `Instructor_ID_4D__c` | Account (instructor) | Person account field |
| `Associate_ID_4D__c` | Account (associate) | Person account field |
| `instructor_{id}` | PersonEmployment | `instructor_1001` |
| `associate_{id}` | PersonEmployment | `associate_2002` |
| Base course code | Learning, LearningCourse | `OWC 303` |
| Source row ID | CourseOffering | `20253_OWC 303 A` |
| `CINST-{id}` | CourseOfferingParticipant (instructor) | `CINST-9001` |
| `CASSOC-{id}` | CourseOfferingParticipant (associate) | `CASSOC-9002` |
| `CINST-SD-{id}` | Course_Offering_Staff_Detail__c (instructor) | `CINST-SD-5001` |
| `CASSOC-SD-{id}` | Course_Offering_Staff_Detail__c (associate) | `CASSOC-SD-5002` |
| `CDEPT-{id}` | Course_Department__c | `CDEPT-3001` |
| `ENR-{id}` | CourseOfferingParticipant (student enrollment) | `ENR-80001` |
| `NOISBN_{slug}` | Textbook__c (no ISBN) | `NOISBN_intro-to-python_smith` |
| (raw 4D `Student_ID`) | User (`FederationIdentifier` — not `External_ID_4D__c`) | Account Login |
| (raw 4D ID) | All other objects | `"6001"` |

---

## 7. Known Open Items & Decisions

| # | Item | Status |
|---|---|---|
| 1 | **Enrollment — OPEN statuses** (`Adjustment` 6,456 / `DupEnrollment` 3,173 / `Course cancel` 4,859 / `Cancelled - Refunded` 3,430 / `Cancelled - Pending` 18) | Excluded from current run; pending business decision |
| 2 | **Enrollment — `New` status** | Excluded (abandoned cart; per Amy) |
| 3 | **Enrollment Waiver — `Verification__c` field** may contain raw IP addresses | Loading as-is; business review pending |
| 4 | **Enrollment audit fields** (`CreatedDate`, `LastModifiedDate`) | Require "Set Audit Fields upon Record Creation" org permission to be enabled |
| 5 | **CourseOffering.ProviderId / PrimaryFacultyId / Coordinator__c** | Using SF Ids (SOQL pre-resolve) rather than external ID refs — these lookup fields do not support external ID relationship syntax |
| 6 | **PersonEmployment.RelatedPersonId / AccountId** | Using SF Ids (resolved post-Account upsert) — no external ID relationship support on these standard fields |
| 7 | **Unit_Rule__c — Format / Department_ID** | Intentionally not mapped (per Liz, May 2026) |
| 8 | **Grades — CourseOfferingPtcpResult** | Grade data not yet in CourseOfferingParticipant; separate later flow planned |
| 9 | **Legacy_Enrollment_Status__c** (drop/refund detail preservation) | Division of responsibility between Holly/Amy TBD |
| 10 | **Enrollment `TA_Discount__c`** | Stored as negative value per architect decision; confirm with business |
| 11 | **Account Login — `Inactive`→`Active` mapping** | ~~Doc says "Direct Map" but names are semantic opposites~~ **Resolved:** confirmed with architect — source `Inactive` flag IS the duplicate marker (4D only sets it true for duplicates), so `IsActive = !Inactive` is correct; no separate dedup logic needed |
| 12 | **Account Login — `Student_Profile` Profile must exist** | Flow throws if not found; confirm exact Profile name/API value with org admin |
| 13 | **Account Login — scope of `Instructor_ID`/`Associate_ID` rows** | Source table carries these columns (marked "Do Not Map") suggesting non-student logins may exist; current flow only processes rows with a usable `Student_ID` and silently skips the rest — confirm instructor/associate logins are out of scope for this table |
| 14 | **Account Login — duplicate `Student_ID` rows** | If more than one Account Login row shares a `Student_ID`, whichever streams last wins the upsert with no tie-break; confirm this is acceptable or define a tie-break rule (e.g. `Last_Modified_Date`) |
| 15 | **Account Login — `Student_Profile` license type** | Unconfirmed whether this is a standard internal-user Profile or an Experience Cloud/Community license Profile; the latter needs additional Contact/community-membership setup beyond `ProfileId` for the login to actually work |
| 16 | **Account Login — Username global uniqueness** | Username is set to the Contact's raw email; Salesforce Usernames must be globally unique across all orgs, so sandbox/UAT runs may collide — confirm whether an environment suffix convention is needed |
| 17 | **Account Login — welcome/password-reset email on User insert** | Bulk-creating `User` records normally triggers Salesforce's welcome email; confirm whether this should be suppressed during the migration load |
