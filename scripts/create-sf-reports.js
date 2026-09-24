#!/usr/bin/env node
'use strict';

/**
 * create-sf-reports.js
 *
 * Creates two custom report types and two reports in Salesforce:
 *   • Stanford CSP – Student Migration
 *   • Stanford CSP – Instructor Migration
 *
 * Prerequisites:
 *   Option A — sf CLI:   sf org login web   (or --alias <org>)
 *   Option B — env vars: SF_INSTANCE_URL=https://... SF_ACCESS_TOKEN=... node scripts/create-sf-reports.js
 *
 * Usage:
 *   node scripts/create-sf-reports.js             # create everything
 *   node scripts/create-sf-reports.js --dry-run   # discover relationships only
 */

const axios = require('axios');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

const SF_API_VERSION  = '60.0';
const STUDENT_RT      = 'Stanford_CSP_Student_Migration';
const INSTRUCTOR_RT   = 'Stanford_CSP_Instructor_Migration';
const FOLDER_LABEL    = 'Stanford CSP Reports';
const FOLDER_DEV_NAME = 'Stanford_CSP_Reports';

// ── Credentials ────────────────────────────────────────────────────────────────

function getCredentials(orgAlias) {
  if (process.env.SF_INSTANCE_URL && process.env.SF_ACCESS_TOKEN) {
    return {
      instanceUrl: process.env.SF_INSTANCE_URL.replace(/\/$/, ''),
      accessToken: process.env.SF_ACCESS_TOKEN,
      alias: orgAlias ?? 'env',
    };
  }
  try {
    const flag = orgAlias ? `--target-org ${orgAlias}` : '';
    const json = execSync(`sf org display ${flag} --json 2>/dev/null`, { encoding: 'utf8' });
    const { result } = JSON.parse(json);
    if (!result?.instanceUrl || !result?.accessToken) throw new Error('Missing fields');
    return {
      instanceUrl: result.instanceUrl.replace(/\/$/, ''),
      accessToken: result.accessToken,
      alias: result.alias ?? orgAlias ?? 'default',
    };
  } catch {
    throw new Error(
      'No Salesforce credentials found.\n' +
      '  Option A: node scripts/create-sf-reports.js stanford-devmain\n' +
      '  Option B: SF_INSTANCE_URL=https://... SF_ACCESS_TOKEN=... node scripts/create-sf-reports.js'
    );
  }
}

// ── SF REST helper ─────────────────────────────────────────────────────────────

function sfGet(instanceUrl, accessToken, path, params) {
  return axios.get(`${instanceUrl}/services/data/v${SF_API_VERSION}${path}`, {
    params,
    headers: { Authorization: `Bearer ${accessToken}` },
  }).then(r => r.data);
}

function sfPost(instanceUrl, accessToken, path, body) {
  return axios.post(`${instanceUrl}/services/data/v${SF_API_VERSION}${path}`, body, {
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
  }).then(r => r.data);
}

function sfPatch(instanceUrl, accessToken, path, body) {
  return axios.patch(`${instanceUrl}/services/data/v${SF_API_VERSION}${path}`, body, {
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
  }).then(r => r.data);
}

// ── Discover relationship names ────────────────────────────────────────────────

async function discoverRelationships(instanceUrl, accessToken) {
  const [contactDesc, accountDesc] = await Promise.all([
    sfGet(instanceUrl, accessToken, '/sobjects/Contact/describe'),
    sfGet(instanceUrl, accessToken, '/sobjects/Account/describe'),
  ]);

  const migHistRel = contactDesc.childRelationships.find(
    r => r.childSObject === 'Migration_History__c'
  )?.relationshipName ?? 'Migration_Histories__r';

  // PersonEmployment may be a direct child of Account (Person Account orgs) or
  // a child of Contact. Check both; Account wins if both exist.
  const personEmpFromAccount = accountDesc.childRelationships.find(
    r => r.childSObject === 'PersonEmployment'
  );
  const personEmpFromContact = contactDesc.childRelationships.find(
    r => r.childSObject === 'PersonEmployment'
  );

  // Prefer the Contact path because it allows nesting PersonEmployment inside
  // the single Contacts <join> (Salesforce only allows one top-level <join>).
  const personEmpRel = personEmpFromContact?.relationshipName
    ?? personEmpFromAccount?.relationshipName
    ?? 'PersonEmploymentDetails';

  const personEmpViaContact = !!personEmpFromContact;

  console.log(`   Contact → Migration_History__c : ${migHistRel}`);
  console.log(`   PersonEmployment relationship  : ${personEmpRel} (via ${personEmpViaContact ? 'Contact' : 'Account'})`);

  return { migHistRel, personEmpRel, personEmpViaContact };
}

// ── Field definitions ──────────────────────────────────────────────────────────
// In Person Account report types, __pc fields are Contact-side fields and must
// use their Contact API name (without __pc, without Person prefix) under the
// Account.Contacts table. Only fields that belong directly to the Account object
// (standard Account fields, Account-level custom fields like External_ID_4D__c)
// should appear in the Account table section.

// Shared Account-table fields (both student and instructor)
const BASE_ACCOUNT_FIELDS = [
  { field: 'Name',             label: 'Full Name' },
  { field: 'External_ID_4D__c', label: 'External ID (4D)' },
  { field: 'FirstName',        label: 'First Name' },
  { field: 'LastName',         label: 'Last Name' },
  { field: 'MiddleName',       label: 'Middle Name' },
  { field: 'Description',      label: 'Notes' },
];

// Instructor-only Account-table fields
const INSTRUCTOR_ACCOUNT_EXTRA = [
  { field: 'Suffix', label: 'Suffix' },
  { field: 'Phone',  label: 'Work Phone' },
  { field: 'Fax',    label: 'Fax' },
];

// Shared Contact-table fields (both student and instructor)
// Field names are Contact API names (no Person prefix, __c not __pc).
const BASE_CONTACT_FIELDS = [
  { field: 'External_ID_4D__c',            label: 'Contact External ID (4D)' },
  { field: 'Birthdate',                    label: 'Date of Birth' },
  { field: 'Email',                        label: 'Email' },
  { field: 'HasOptedOutOfEmail',           label: 'Opt Out of Email' },
  { field: 'OtherPhone',                   label: 'Other Phone' },
  { field: 'MailingStreet',                label: 'Mailing Street' },
  { field: 'MailingCity',                  label: 'Mailing City' },
  { field: 'MailingState',                 label: 'Mailing State' },
  { field: 'MailingPostalCode',            label: 'Mailing Postal Code' },
  { field: 'MailingCountry',               label: 'Mailing Country' },
  { field: 'Legal_Sex__c',                 label: 'Legal Sex' },
  { field: 'Opt_In_Physical_Mail__c',      label: 'Opt In Physical Mail' },
  { field: 'US_Citizen__c',                label: 'US Citizen' },
  { field: 'Stanford_Alumnus__c',          label: 'Stanford Alumnus' },
  { field: 'Highest_Degree__c',            label: 'Highest Degree' },
  { field: 'Ethnicity__c',                 label: 'Ethnicity' },
  { field: 'TA_Discount_Type__c',          label: 'TA Discount Type' },
  { field: 'University_ID__c',             label: 'University ID' },
  { field: 'SAA_Number__c',                label: 'SAA Number' },
  { field: 'Do_Not_Enroll__c',             label: 'Do Not Enroll' },
  { field: 'First_Quarter__c',             label: 'First Quarter' },
  { field: 'Total_Enrollments__c',         label: 'Total Enrollments' },
  { field: 'Discount_Verified__c',         label: 'Discount Verified' },
  { field: 'Discount_Verification_Date__c', label: 'Discount Verification Date' },
  { field: 'Alert_Flag__c',                label: 'Alert Flag' },
  { field: 'Most_Recent_Quarter__c',       label: 'Most Recent Quarter' },
  { field: 'Certificate_Issued__c',        label: 'Certificate Issued' },
  { field: 'Certificate_Issued_Date__c',   label: 'Certificate Issued Date' },
  { field: 'SBSAA_Number__c',              label: 'SBSAA Number' },
  { field: 'FERPA_Directory_Consent__c',   label: 'FERPA Directory Consent' },
  { field: 'Discount_Lifetime__c',         label: 'Lifetime Discount' },
];

// Instructor-only Contact-table fields
const INSTRUCTOR_CONTACT_EXTRA = [
  { field: 'Title',                label: 'Title' },
  { field: 'Department',           label: 'Department' },
  { field: 'HomePhone',            label: 'Home Phone' },
  { field: 'MobilePhone',          label: 'Mobile Phone' },
  { field: 'SUNet_Id__c',          label: 'SUNet ID' },
  { field: 'Username_4D__c',       label: 'Username' },
  { field: 'Active__c',            label: 'Active' },
  { field: 'Student_ID__c',        label: 'Student ID' },
  { field: 'Stanford_Mail_Code__c', label: 'Stanford Mail Code' },
  { field: 'Benefit_Status__c',    label: 'Benefit Status' },
  { field: 'Out_Of_State_Fee__c',  label: 'Out of State Fee' },
  { field: 'Legal_Name__c',        label: 'Legal Name' },
];

const PERSON_EMPLOYMENT_FIELDS = [
  { field: 'Name',                        label: 'Employment Name' },
  { field: 'External_ID_4D__c',           label: 'External ID (4D)' },
  { field: 'AC_Pay__c',                   label: 'AC Pay' },
  { field: 'Email_for_Students__c',       label: 'Email for Students' },
  { field: 'Harassment_Training__c',      label: 'Harassment Training' },
  { field: 'Total_Courses__c',            label: 'Total Courses' },
  { field: 'Coordinator_Status__c',       label: 'Coordinator Status' },
  { field: 'Salary_Category__c',          label: 'Salary Category' },
  { field: 'Online_Teaching_Training__c', label: 'Online Teaching Training' },
  { field: 'Zoom_Training__c',            label: 'Zoom Training' },
  { field: 'Canvas_Training__c',          label: 'Canvas Training' },
];

const MIGRATION_HISTORY_FIELDS = [
  { field: 'Description__c',    label: 'Migration Description' },
  { field: 'Skipped_Fields__c', label: 'Skipped Fields' },
  { field: 'CreatedDate',       label: 'Migrated At' },
];

// ── XML builder ────────────────────────────────────────────────────────────────

function columnsXml(fields, table, indent = '      ') {
  return fields.map(f =>
    `${indent}<columns>\n${indent}  <checkedByDefault>true</checkedByDefault>\n${indent}  <displayNameOverride>${f.label}</displayNameOverride>\n${indent}  <field>${f.field}</field>\n${indent}  <table>${table}</table>\n${indent}</columns>`
  ).join('\n');
}

function buildStudentReportTypeXml(migHistRel) {
  const contactTable = 'Account.Contacts';
  const migHistTable = `${contactTable}.${migHistRel}`;

  // <join> at the ReportType root is REQUIRED — without it Salesforce only
  // recognises 'Account' and rejects every other table path.
  // The nested inner join represents Migration_Histories__r from Contact;
  // the outer join represents Contacts from Account.
  return `<?xml version="1.0" encoding="UTF-8"?>
<ReportType xmlns="http://soap.sforce.com/2006/04/metadata">
    <baseObject>Account</baseObject>
    <category>accounts</category>
    <deployed>true</deployed>
    <description>Stanford CSP Student Migration — Account with Contact and Migration History</description>
    <join>
        <join>
            <outerJoin>false</outerJoin>
            <relationship>${migHistRel}</relationship>
        </join>
        <outerJoin>false</outerJoin>
        <relationship>Contacts</relationship>
    </join>
    <label>Stanford CSP Student Migration</label>
    <sections>
${columnsXml(BASE_ACCOUNT_FIELDS, 'Account')}
      <masterLabel>Accounts</masterLabel>
    </sections>
    <sections>
${columnsXml(BASE_CONTACT_FIELDS, contactTable)}
      <masterLabel>Contacts</masterLabel>
    </sections>
    <sections>
${columnsXml(MIGRATION_HISTORY_FIELDS, migHistTable)}
      <masterLabel>Migration Histories</masterLabel>
    </sections>
</ReportType>`;
}

function buildInstructorReportTypeXml(migHistRel) {
  const contactTable = 'Account.Contacts';
  const migHistTable = `${contactTable}.${migHistRel}`;

  const instructorContactFields = [
    ...BASE_CONTACT_FIELDS,
    ...INSTRUCTOR_CONTACT_EXTRA,
  ].filter((f, i, arr) => arr.findIndex(x => x.field === f.field) === i);

  return `<?xml version="1.0" encoding="UTF-8"?>
<ReportType xmlns="http://soap.sforce.com/2006/04/metadata">
    <baseObject>Account</baseObject>
    <category>accounts</category>
    <deployed>true</deployed>
    <description>Stanford CSP Instructor Migration — Account with Contact and Migration History</description>
    <join>
        <join>
            <outerJoin>false</outerJoin>
            <relationship>${migHistRel}</relationship>
        </join>
        <outerJoin>false</outerJoin>
        <relationship>Contacts</relationship>
    </join>
    <label>Stanford CSP Instructor Migration</label>
    <sections>
${columnsXml([...BASE_ACCOUNT_FIELDS, ...INSTRUCTOR_ACCOUNT_EXTRA], 'Account')}
      <masterLabel>Accounts</masterLabel>
    </sections>
    <sections>
${columnsXml(instructorContactFields, contactTable)}
      <masterLabel>Contacts</masterLabel>
    </sections>
    <sections>
${columnsXml(MIGRATION_HISTORY_FIELDS, migHistTable)}
      <masterLabel>Migration Histories</masterLabel>
    </sections>
</ReportType>`;
}

// ── Deploy report types via temp SFDX project ─────────────────────────────────
// The SOAP upsertMetadata API cannot add related-object joins to custom report
// types. We work around this by writing proper .reportType-meta.xml files into
// a throwaway SFDX project and deploying via sf CLI, which handles the full
// metadata schema including relationship paths.

function deployReportTypesSfdx(orgAlias, studentXml, instructorXml) {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'csp-reports-'));
  try {
    // Minimal SFDX project
    fs.writeFileSync(
      path.join(tmpDir, 'sfdx-project.json'),
      JSON.stringify({ packageDirectories: [{ path: 'force-app', default: true }], sourceApiVersion: SF_API_VERSION })
    );

    const rtDir = path.join(tmpDir, 'force-app', 'main', 'default', 'reportTypes');
    fs.mkdirSync(rtDir, { recursive: true });

    fs.writeFileSync(path.join(rtDir, `${STUDENT_RT}.reportType-meta.xml`), studentXml);
    fs.writeFileSync(path.join(rtDir, `${INSTRUCTOR_RT}.reportType-meta.xml`), instructorXml);

    let raw;
    try {
      raw = execSync(
        `sf project deploy start --source-dir force-app --target-org ${orgAlias} --json 2>&1`,
        { cwd: tmpDir, encoding: 'utf8' }
      );
    } catch (e) {
      raw = e.stdout ?? e.output?.join('') ?? String(e);
    }

    // Strip any warning lines before the JSON object
    const jsonStart = raw.indexOf('{');
    const jsonStr = jsonStart >= 0 ? raw.slice(jsonStart) : raw;

    let parsed;
    try { parsed = JSON.parse(jsonStr); } catch { throw new Error(`Deploy output not JSON:\n${raw}`); }

    if (parsed.status !== 0) {
      const failures = parsed.result?.details?.componentFailures ?? [];
      const msg = failures.map(f => `  ${f.fullName}: ${f.problem}`).join('\n') || JSON.stringify(parsed, null, 2);
      throw new Error(`Deploy failed:\n${msg}`);
    }
    return parsed;
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

// ── Report folder ──────────────────────────────────────────────────────────────

async function findOrCreateFolder(instanceUrl, accessToken) {
  const qry = await sfGet(instanceUrl, accessToken, '/query', {
    q: `SELECT Id FROM Folder WHERE DeveloperName = '${FOLDER_DEV_NAME}' AND Type = 'Report'`,
  });

  if (qry.records.length > 0) {
    console.log(`   Folder already exists (${qry.records[0].Id})`);
    return qry.records[0].Id;
  }

  const res = await sfPost(instanceUrl, accessToken, '/sobjects/Folder', {
    Name: FOLDER_LABEL,
    DeveloperName: FOLDER_DEV_NAME,
    Type: 'Report',
    AccessType: 'Public',
  });
  console.log(`   Folder created (${res.id})`);
  return res.id;
}

// ── Discover report type columns (after creation) ─────────────────────────────

async function getReportTypeColumns(instanceUrl, accessToken, reportTypeName) {
  try {
    const data = await sfGet(instanceUrl, accessToken, `/analytics/reportTypes/${reportTypeName}`);
    // Returns flat list of { name, label, dataType, ... }
    return (data.columns ?? []).map(c => c.name);
  } catch {
    return null;
  }
}

// ── Upsert report via Analytics REST API ──────────────────────────────────────

async function findExistingReport(instanceUrl, accessToken, name) {
  const qry = await sfGet(instanceUrl, accessToken, '/query', {
    q: `SELECT Id FROM Report WHERE Name = '${name.replace(/'/g, "\\'")}'`,
  });
  return qry.records[0]?.Id ?? null;
}

async function upsertReport(instanceUrl, accessToken, folderId, metadata) {
  const body = { reportMetadata: { ...metadata, folderId } };
  const existingId = await findExistingReport(instanceUrl, accessToken, metadata.name, folderId);

  if (existingId) {
    await sfPatch(instanceUrl, accessToken, `/analytics/reports/${existingId}`, body);
    console.log(`   Updated: "${metadata.name}" (${existingId})`);
  } else {
    const res = await sfPost(instanceUrl, accessToken, '/analytics/reports', body);
    console.log(`   Created: "${metadata.name}" (${res.id})`);
  }
}

// ── Report metadata builders ───────────────────────────────────────────────────

function buildStudentReportMeta(folderId, availableColumns) {
  // availableColumns is the list of column IDs returned by the Analytics API
  // after the report type is deployed.  When null, omit detailColumns so SF
  // includes all columns by default.
  const descColumn = availableColumns?.find(c =>
    c.toLowerCase().includes('description__c') && c.toLowerCase().includes('migration')
  );

  const meta = {
    name: 'Stanford CSP - Student Migration',
    reportType: { type: STUDENT_RT },
    reportFormat: 'TABULAR',
    scope: 'organization',
    showDetails: true,
    description: 'All student records imported by the Student Import flow',
    folderId,
  };
  if (availableColumns) meta.detailColumns = availableColumns;
  if (descColumn) {
    meta.reportFilters = [{
      column: descColumn,
      isRunPageEditable: true,
      operator: 'contains',
      value: 'Student Import',
    }];
  }
  return meta;
}

function buildInstructorReportMeta(folderId, availableColumns) {
  const descColumn = availableColumns?.find(c =>
    c.toLowerCase().includes('description__c') && c.toLowerCase().includes('migration')
  );

  const meta = {
    name: 'Stanford CSP - Instructor Migration',
    reportType: { type: INSTRUCTOR_RT },
    reportFormat: 'TABULAR',
    scope: 'organization',
    showDetails: true,
    description: 'All instructor records imported by the Instructor Import flow',
    folderId,
  };
  if (availableColumns) meta.detailColumns = availableColumns;
  if (descColumn) {
    meta.reportFilters = [{
      column: descColumn,
      isRunPageEditable: true,
      operator: 'contains',
      value: 'Instructor Import',
    }];
  }
  return meta;
}

// ── Main ───────────────────────────────────────────────────────────────────────

async function main() {
  const dryRun  = process.argv.includes('--dry-run');
  const orgAlias = process.argv.find(a => !a.startsWith('-') && a !== process.argv[0] && a !== process.argv[1]) ?? 'stanford-devmain';

  console.log('\nStanford CSP Migration — Salesforce Report Setup');
  console.log('=================================================\n');

  console.log('1. Getting Salesforce credentials...');
  const { instanceUrl, accessToken, alias } = getCredentials(orgAlias);
  console.log(`   Instance : ${instanceUrl}`);
  console.log(`   Org alias: ${alias}\n`);

  console.log('2. Discovering object relationships...');
  const { migHistRel, personEmpRel, personEmpViaContact } = await discoverRelationships(instanceUrl, accessToken);
  console.log();

  if (dryRun) {
    console.log('Dry-run complete — use without --dry-run to create reports.\n');
    return;
  }

  console.log('3. Deploying custom report types via sf CLI...');
  const studentXml    = buildStudentReportTypeXml(migHistRel);
  const instructorXml = buildInstructorReportTypeXml(migHistRel);
  deployReportTypesSfdx(alias, studentXml, instructorXml);
  console.log(`   ✓ ${STUDENT_RT}`);
  console.log(`   ✓ ${INSTRUCTOR_RT}\n`);

  console.log('4. Resolving report folder...');
  const folderId = await findOrCreateFolder(instanceUrl, accessToken);
  console.log();

  console.log('5. Discovering report type columns...');
  const studentCols    = await getReportTypeColumns(instanceUrl, accessToken, STUDENT_RT);
  const instructorCols = await getReportTypeColumns(instanceUrl, accessToken, INSTRUCTOR_RT);
  console.log(`   Student columns available:    ${studentCols?.length ?? 'unknown (will use defaults)'}`);
  console.log(`   Instructor columns available: ${instructorCols?.length ?? 'unknown (will use defaults)'}\n`);

  console.log('6. Creating reports...');
  await upsertReport(instanceUrl, accessToken, folderId, buildStudentReportMeta(folderId, studentCols));
  await upsertReport(instanceUrl, accessToken, folderId, buildInstructorReportMeta(folderId, instructorCols));
  console.log();

  console.log('Done! In Salesforce Reports, open the "Stanford CSP Reports" folder.\n');
}

main().catch(err => {
  const detail = err.response?.data;
  console.error('\nError:', err.message);
  if (detail) console.error('Detail:', JSON.stringify(detail, null, 2));
  process.exit(1);
});
