import { configPage, configVar } from "@prismatic-io/spectral";
import { googleDriveOauth2 } from "./manifests/google-drive/connections/oauth2";
import { salesforceOauth2 } from "./manifests/salesforce/connections/oauth2";

export const configPages = {
  Connections: configPage({
    tagline: "Authenticate with Google Drive and Salesforce",
    elements: {
      "Google Drive Connection": googleDriveOauth2("google-drive-connection", {
        clientId: {
          value: "",
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
        },
        clientSecret: {
          value: "",
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
        },
      }),
      "Salesforce Connection": salesforceOauth2("salesforce-connection", {
        clientId: {
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
          value: "",
        },
        clientSecret: {
          value: "",
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
        },
        tokenUrl: {
          value: "",
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
        },
        authorizeUrl: {
          value: "",
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
        },
        revokeUrl: {
          value: "",
          permissionAndVisibilityType: "organization",
          visibleToOrgDeployer: true,
        },
      }),
    },
  }),

  "Quarter & Department Configuration": configPage({
    tagline: "Identify the Google Sheets that contain quarter and department records",
    elements: {
      "Quarter File ID": configVar({
        stableKey: "b2c3d4e5-2222-4b2c-9d3e-bbccdd002222",
        dataType: "string",
        description:
          "Google Drive file ID of the Google Sheet that contains quarter/academic term records.",
      }),
      "Department File ID": configVar({
        stableKey: "a1b2c3d4-1111-4a1b-8c2d-aabbcc001111",
        dataType: "string",
        description:
          "Google Drive file ID of the Google Sheet that contains department records.",
      }),
    },
  }),

  "Student, Instructor & Associate Configuration": configPage({
    tagline:
      "Identify the Google Drive files that contain student, instructor, and associate records",
    elements: {
      "Student File ID": configVar({
        stableKey: "c3d4e5f6-3333-4c7d-0e1f-ccddee001122",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains student records.",
      }),
      "Student Correction Enabled": configVar({
        stableKey: "sc001122-3344-4556-7788-aabbccddeeff",
        dataType: "boolean",
        default: "false",
        description:
          "When true, Student Import only processes records found in the Correction Sheet and applies address overrides. Set to false to process all student records normally.",
      }),
      "Instructor File ID": configVar({
        stableKey: "d4e5f6a7-4444-4d8e-1f2a-ddeeff112233",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains instructor records.",
      }),
      "Instructor Correction Enabled": configVar({
        stableKey: "ic001122-3344-4556-7788-aabbccddeeff",
        dataType: "boolean",
        default: "false",
        description:
          "When true, Instructor Import only processes records found in the Correction Sheet and applies address overrides. Set to false to process all instructor records normally.",
      }),
      "Associate File ID": configVar({
        stableKey: "f6a7b8c9-6666-4f9f-3c4d-ffee11334455",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains associate records.",
      }),
      "Associate Correction Enabled": configVar({
        stableKey: "ac001122-3344-4556-7788-aabbccddeeff",
        dataType: "boolean",
        default: "false",
        description:
          "When true, Associate Import only processes records found in the Correction Sheet and applies address overrides. Set to false to process all associate records normally.",
      }),
      "Correction Sheet ID": configVar({
        stableKey: "b6c7d8e9-f0a1-4b2c-9d3e-f4a5b6c7d8e9",
        dataType: "string",
        default: "",
        description:
          "Google Sheets spreadsheet ID of the shared correction sheet. Used when Student, Instructor, or Associate correction is enabled. Leave blank when no correction is running. Must contain a 'Student', 'Instructor', and/or 'Associate' tab with an 'ID' column plus columns for each corrected field.",
      }),
    },
  }),

  "Unit Rule & Tuition Rule Configuration": configPage({
    tagline:
      "Identify the Google Drive files that contain unit rule and tuition rule records",
    elements: {
      "Unit Rule File ID": configVar({
        stableKey: "b7c8d9e0-f1a2-4b3c-5d6e-7f8a9b0c1d2e",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains unit rule records.",
      }),
      "Tuition Rule File ID": configVar({
        stableKey: "a9b8c7d6-e5f4-4a3b-2c1d-0e9f8a7b6c5d",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains tuition rule records.",
      }),
    },
  }),

  "Course Configuration": configPage({
    tagline:
      "Identify the Google Drive files for course records and everything tied to a course offering (staff detail, other costs, timecards, coursework, and textbooks)",
    elements: {
      "Course File ID": configVar({
        stableKey: "d7e8f9a0-7777-4d0e-4f5a-00112233aabb",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains course records.",
      }),
      "Course Instructor File ID": configVar({
        stableKey: "c8d9e0f1-8888-4c1d-5e6f-11223344ccdd",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV junction table linking Course_ID to Instructor_ID.",
      }),
      "Course Department File ID": configVar({
        stableKey: "f1a2b3c4-9999-4f2e-6a7b-22334455ddee",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV junction table linking Course_ID to Department_ID (Course_Department.txt).",
      }),
      "Course Submission File ID": configVar({
        stableKey: "e2f3a4b5-ccdd-4e5f-7a8b-33445566eeff",
        dataType: "string",
        description:
          "Google Drive file ID of the Course_Submission TSV containing Occurrences, Hours_Per_Occurrence, Total_Hours, Tuition_Adjustment, Tuition_Adjustment_Note, and Async_Hours fields.",
      }),
      "Course Associate File ID": configVar({
        stableKey: "f3a4b5c6-ddee-4f6a-8b9c-445566778899",
        dataType: "string",
        description:
          "Google Drive file ID of the Course_Associate TSV (junction table linking Associate_ID to Course_RecID). Used by the Course Associate Staff Detail flow.",
      }),
      "Course Other Cost File ID": configVar({
        stableKey: "e4f5a6b7-ffee-4e7b-9c0d-556677889900",
        dataType: "string",
        description:
          "Google Drive file ID of the Course_Other_Cost TSV containing expense records (ID, Course_RecID, Cost, Description).",
      }),
      "Coursework File ID": configVar({
        stableKey: "cw001122-3344-4556-7788-99aabbccddff",
        dataType: "string",
        description:
          "Google Drive file ID of the Coursework TSV. " +
          "Only rows where Syllabus = True within the 2-year quarter window are migrated.",
      }),
      "Timecard File ID": configVar({
        stableKey: "f5a6b7c8-0011-4f8c-0d1e-667788990011",
        dataType: "string",
        description:
          "Google Drive file ID of the Timecard TSV containing Course_Offering_Staff_Time_Card__c records.",
      }),
      "Textbook File ID": configVar({
        stableKey: "tb001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Textbook records. " +
          "Migrates Textbook__c (deduplicated by ISBN) and Textbook_Association__c records " +
          "for courses within the 2-year window (ANCHOR_QUARTER wi25).",
      }),
    },
  }),

  "Enrollment Configuration": configPage({
    tagline:
      "Identify the Google Drive files that contain enrollment and enrollment waiver records",
    elements: {
      "Enrollment File ID": configVar({
        stableKey: "en001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Enrollment records. " +
          "Must run after Student, Course, and Registration flows.",
      }),
      "Enrollment Waiver File ID": configVar({
        stableKey: "ew001122-3344-4556-7788-99aabbccddee",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Enrollment_Waiver records.",
      }),
      "Waiver Reference File ID": configVar({
        stableKey: "wr001122-3344-4556-7788-99aabbccddee",
        dataType: "string",
        default: "",
        description:
          "Google Drive file ID of the tab-delimited Waiver reference table (columns: ID, Name). " +
          "Used to denormalise waiver names onto Historical_Waiver__c. The Waiver source table is not migrated.",
      }),
    },
  }),

  "Certificate Configuration": configPage({
    tagline:
      "Identify the Google Drive files for certificate program, certificate course, and student certificate records",
    elements: {
      "Certificate Program File ID": configVar({
        stableKey: "cp001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Certificate_Program records. " +
          "Migrates LearningProgram records.",
      }),
      "Certificate Course File ID": configVar({
        stableKey: "cc001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Certificate_Course records. " +
          "Migrates LearningProgramPlanRequirement records, linking a LearningProgram " +
          "(via Certificate_ID) to a LearningCourse (via Course_Code). Must run after " +
          "the Certificate Program and Course flows.",
      }),
      "Student Certificate File ID": configVar({
        stableKey: "532243e5-6a63-482c-ae81-0776110f6f77",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Student_Certificate records. " +
          "Migrates LearnerProgram and PersonAcademicCredential records. Only rows " +
          "whose Deadline_Quarter falls in the 2-year window (ANCHOR_QUARTER wi25) " +
          "are migrated. Must run after the Student and Certificate Program flows.",
      }),
    },
  }),

  "Additional Configuration": configPage({
    tagline:
      "Identify the Google Drive files for transcript request, registration, alert, and account login records, plus the failed-records output folder",
    elements: {
      "Transcript Request File ID": configVar({
        stableKey: "tr001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Transcript_Request records. " +
          "Only rows with Request_Date >= 2024-07-07 (2-year window) are migrated.",
      }),
      "Registration File ID": configVar({
        stableKey: "rg001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Registration records. " +
          "Migrates CardPaymentMethod, Order, and Payment records. Must run after Student flow.",
      }),
      "Alert File ID": configVar({
        stableKey: "al001122-3344-4556-7788-aabbccddeeff",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Alert records. " +
          "Migrates RecordAlert records; ParentId/WhatId resolve to a PersonAccount " +
          "via Student_ID, Associate_ID, or Instructor_ID, so Student/Associate/" +
          "Instructor flows must run first. Course_RecID resolves to a " +
          "CourseOffering lookup via the 'Course File ID' config var (Course " +
          "Configuration page), so the Course flow must also run first.",
      }),
      "Account Login File ID": configVar({
        stableKey: "9e8d7c6b-5a4f-4b3c-8d2e-1f0a9b8c7d6e",
        dataType: "string",
        description:
          "Google Drive file ID of the TSV that contains Account Login records. " +
          "Must run after Student Import — each row is matched to its Account via Student_ID.",
      }),
      "Failed Records Folder ID": configVar({
        stableKey: "e5f6a7b8-5555-4e8f-3b4c-eeff00223344",
        dataType: "string",
        description:
          "Google Drive folder ID where failed-record CSV reports will be uploaded after each execution.",
      }),
    },
  }),
};
