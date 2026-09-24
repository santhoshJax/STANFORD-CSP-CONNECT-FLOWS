const fs = require("fs");
const path = require("path");
const Papa = require("papaparse");

const ROOT = path.resolve(__dirname, "..");
const INPUT_FILES = [
  "/Users/jax/Downloads/Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors.csv",
  "/Users/jax/Downloads/Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors (2).csv",
];
const ADDRESS_SETTINGS =
  "/Users/jax/Downloads/Address.settings-meta.xml";
const OUTPUT_DIR = path.join(ROOT, "reports", "address-error-triage");

const CATEGORY_CODE = "Fixable by Code Update";
const CATEGORY_CONFIG = "Fixable by Salesforce Config";
const CATEGORY_CLIENT = "Needs Client Clarification";
const CATEGORY_OTHER = "Not State/Country Error";

function readTag(block, tag) {
  const match = block.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
  return match ? match[1].trim() : "";
}

function normalize(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, "AND")
    .replace(/[^A-Z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function parseAddressSettings(xmlPath) {
  const xml = fs.readFileSync(xmlPath, "utf8");
  const countries = [];

  for (const countryMatch of xml.matchAll(
    /<countries>([\s\S]*?)<\/countries>/g,
  )) {
    const countryBlock = countryMatch[1];
    const country = {
      iso: readTag(countryBlock, "isoCode"),
      integrationValue: readTag(countryBlock, "integrationValue"),
      label: readTag(countryBlock, "label"),
      active: readTag(countryBlock, "active") === "true",
      states: [],
    };

    for (const stateMatch of countryBlock.matchAll(
      /<states>([\s\S]*?)<\/states>/g,
    )) {
      const stateBlock = stateMatch[1];
      country.states.push({
        iso: readTag(stateBlock, "isoCode"),
        integrationValue: readTag(stateBlock, "integrationValue"),
        label: readTag(stateBlock, "label"),
        active: readTag(stateBlock, "active") === "true",
      });
    }

    countries.push(country);
  }

  return countries;
}

const countries = parseAddressSettings(ADDRESS_SETTINGS);
const countryByIso = new Map(countries.map((country) => [country.iso, country]));
const countryLookup = new Map();

for (const country of countries) {
  for (const key of [country.iso, country.integrationValue, country.label]) {
    if (key) countryLookup.set(normalize(key), country.iso);
  }
}

const countryAliases = {
  RUSSIA: "RU",
  TURKEY: "TR",
  TURKIYE: "TR",
  "KOREA REPUBLIC OF SOUTH": "KR",
  "KOREA REPUBLIC OF": "KR",
  "SOUTH KOREA": "KR",
  "KOREA SOUTH": "KR",
  KOREA: "KR",
  "VIET NAM": "VN",
  VIETNAM: "VN",
  "CZECH REPUBLIC": "CZ",
  UK: "GB",
  "U K": "GB",
  "UNITED KINGDON": "GB",
  "ENGLAND UK": "GB",
  UAE: "AE",
  "U S A": "US",
  USA: "US",
  "U S": "US",
  "U S A REALLY YOU NEED THIS": "US",
  "USA N": "US",
  "US OF A": "US",
  "UNITES STATES": "US",
  "UINITED STATES": "US",
  "UNITED STATED": "US",
  "UNITED STATE": "US",
  "UNITIED STATES": "US",
  "UNITESD STATES": "US",
  "THE UNITED STATES OF AMERICA": "US",
  "ESTADOS UNIDOS": "US",
  "COUNTRY US": "US",
  BOLIVIA: "BO",
  TANZANIA: "TZ",
  VENEZUELA: "VE",
  LIBANON: "LB",
  "SULTANATE OF OMAN": "OM",
  "REPUBLIC OF MOLDOVA": "MD",
  "THE NETHERLANDS": "NL",
  NETHERLAND: "NL",
  ROMANI: "RO",
  RO: "RO",
  "PEOPLE S REPUBLIC OF CHINA": "CN",
  "TAIWAN R O C": "TW",
  CANANDA: "CA",
  ASTRALIA: "AU",
  BRASIL: "BR",
  CURACAO: "CW",
  "COTE DIVOIRE": "CI",
  MACAU: "MO",
  LAOS: "LA",
  "COLOMBIA COLOMBIA": "CO",
};

for (const [alias, iso] of Object.entries(countryAliases)) {
  countryLookup.set(alias, iso);
}

const missingOrInactiveCountryValues = new Set([
  "IRAN",
  "SUDAN",
  "SYRIA",
  "PALESTINIAN TERRITORIES",
  "CUBA",
  "KOREA DEMOCRATIC PEOPLE S REP",
  "VIRGIN ISLANDS U S",
  "GUAM",
  "PUERTO RICO",
]);

const clientDecisionCountryValues = new Set([
  "HONG KONG",
  "HONGKONG",
  "HONG KONG SAR",
  "HONG KONG SAR CHINA",
  "SAN MATEO",
  "SANTA CLARA",
  "PALM BEACH",
  "NORTH AMERICA",
  "MEXICO DF",
]);

const stateAliasesByCountry = {
  BR: {
    "SAO PAULO": "SP",
  },
  MX: {
    CDMX: "DF",
    "CIUDAD DE MEXICO": "DF",
    "MEXICO CITY": "DF",
  },
  IN: {
    TAMILNADU: "TN",
    "NEW DELHI": "DL",
  },
  US: {
    "CA CALIFORNIA": "CA",
    "CALIFORNIA CA": "CA",
    CALIF: "CA",
    CALIFONIA: "CA",
    CALIFORONIA: "CA",
    "NORTHERN CALIFORNIA": "CA",
    "CALIFORNIA NORTH": "CA",
  },
};

function normalizeCountryCode(value) {
  return countryLookup.get(normalize(value)) || "";
}

function activeStateCode(countryIso, value) {
  const country = countryByIso.get(countryIso);
  if (!country) return "";

  const normalized = normalize(value);
  for (const state of country.states) {
    if (!state.active) continue;
    const keys = [state.iso, state.integrationValue, state.label].map(normalize);
    if (keys.includes(normalized)) return state.iso;
  }

  return stateAliasesByCountry[countryIso]?.[normalized] || "";
}

function inactiveStateMatch(countryIso, value) {
  const country = countryByIso.get(countryIso);
  if (!country) return false;

  const normalized = normalize(value);
  return country.states.some((state) => {
    if (state.active) return false;
    const keys = [state.iso, state.integrationValue, state.label].map(normalize);
    return keys.includes(normalized);
  });
}

function classifyCountry(row) {
  const rawCountry = row.PersonMailingCountry || "";
  const normalizedCountry = normalize(rawCountry);

  if (!normalizedCountry) {
    return {
      category: CATEGORY_CODE,
      suggestedCountryCode: "",
      suggestedStateCode: "",
      detail:
        "Country is blank. Script should omit/null PersonMailingCountryCode instead of sending an invalid value.",
    };
  }

  if (clientDecisionCountryValues.has(normalizedCountry)) {
    if (normalizedCountry.startsWith("HONG KONG")) {
      return {
        category: CATEGORY_CLIENT,
        suggestedCountryCode: "CN",
        suggestedStateCode: "91",
        detail:
          "Client decision needed. Salesforce metadata has Hong Kong as a China subdivision (CountryCode CN, StateCode 91), not as a standalone country. Confirm whether to map to CN/91 or change Salesforce country config.",
      };
    }

    return {
      category: CATEGORY_CLIENT,
      suggestedCountryCode: "",
      suggestedStateCode: "",
      detail:
        "Country value is not a country or contains mixed location data. Client/source-data owner must confirm the correct country and whether the value belongs in city, state, county, or another address field.",
    };
  }

  if (missingOrInactiveCountryValues.has(normalizedCountry)) {
    return {
      category: CATEGORY_CONFIG,
      suggestedCountryCode: "",
      suggestedStateCode: "",
      detail:
        "Country/territory is not active as a country in the provided Salesforce Address.settings metadata. Salesforce admin must add/activate the country or decide an allowed replacement mapping.",
    };
  }

  const countryIso = normalizeCountryCode(rawCountry);
  const country = countryByIso.get(countryIso);
  if (countryIso && country?.active) {
    return {
      category: CATEGORY_CODE,
      suggestedCountryCode: countryIso,
      suggestedStateCode: "",
      detail: `Script can normalize country value '${rawCountry}' to PersonMailingCountryCode '${countryIso}'.`,
    };
  }

  return {
    category: CATEGORY_CLIENT,
    suggestedCountryCode: countryIso,
    suggestedStateCode: "",
    detail:
      "Country value is not recognized by the current normalization rules or Salesforce metadata. Client/source-data owner must confirm the intended country.",
  };
}

function classifyState(row) {
  const rawState = row.PersonMailingState || "";
  const rawCountry = row.PersonMailingCountry || "";
  const normalizedState = normalize(rawState);

  if (!normalizedState || ["NA", "N A", "NONE", "NULL"].includes(normalizedState)) {
    return {
      category: CATEGORY_CODE,
      suggestedCountryCode: normalizeCountryCode(rawCountry),
      suggestedStateCode: "",
      detail:
        "State is blank/NA. Script should omit/null PersonMailingStateCode instead of sending a literal placeholder.",
    };
  }

  const countryIso = normalizeCountryCode(rawCountry);
  const stateCode = activeStateCode(countryIso, rawState);
  if (countryIso && stateCode) {
    return {
      category: CATEGORY_CODE,
      suggestedCountryCode: countryIso,
      suggestedStateCode: stateCode,
      detail: `Script can normalize state '${rawState}' with country '${rawCountry}' to PersonMailingCountryCode '${countryIso}' and PersonMailingStateCode '${stateCode}'.`,
    };
  }

  if (countryIso && inactiveStateMatch(countryIso, rawState)) {
    return {
      category: CATEGORY_CONFIG,
      suggestedCountryCode: countryIso,
      suggestedStateCode: "",
      detail:
        "State/territory exists in Salesforce Address.settings for this country but is inactive. Salesforce admin must activate it or approve a replacement mapping.",
    };
  }

  return {
    category: CATEGORY_CLIENT,
    suggestedCountryCode: countryIso,
    suggestedStateCode: "",
    detail:
      "State cannot be deterministically mapped for the current row. It may conflict with the country, be a city/county/country value in the state field, or require Salesforce metadata expansion. Client/source-data owner must confirm the intended address.",
  };
}

function classifyRow(row) {
  const message = row["Error Message"] || "";

  if (message.includes("PersonMailingCountry")) {
    return classifyCountry(row);
  }

  if (message.includes("PersonMailingState")) {
    return classifyState(row);
  }

  return {
    category: CATEGORY_OTHER,
    suggestedCountryCode: "",
    suggestedStateCode: "",
    detail:
      "Original Salesforce failure is not a Mailing State/Country picklist error. Handle in a separate migration remediation track.",
  };
}

function countBy(rows, field) {
  const counts = new Map();
  for (const row of rows) {
    const key = row[field] || "<blank>";
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return [...counts.entries()].sort((a, b) => b[1] - a[1]);
}

function renderTable(rows) {
  return [
    "| Value | Count |",
    "|---|---:|",
    ...rows.map(([value, count]) => `| ${String(value).replace(/\|/g, "\\|")} | ${count} |`),
  ].join("\n");
}

function aggregateAddressRows(rows) {
  const groups = new Map();

  for (const row of rows) {
    const message = row["Error Message"] || "";
    if (!message.includes("PersonMailingState") && !message.includes("PersonMailingCountry")) {
      continue;
    }

    const field = message.includes("PersonMailingState")
      ? "PersonMailingState"
      : "PersonMailingCountry";
    const keyParts = [
      row.State_Country_Triage_Category,
      field,
      row.PersonMailingState || "",
      row.PersonMailingCountry || "",
      row.Suggested_PersonMailingCountryCode || "",
      row.Suggested_PersonMailingStateCode || "",
      row.State_Country_Triage_Detail || "",
    ];
    const key = JSON.stringify(keyParts);

    if (!groups.has(key)) {
      groups.set(key, {
        State_Country_Triage_Category: row.State_Country_Triage_Category,
        Failed_Field: field,
        PersonMailingState: row.PersonMailingState || "",
        PersonMailingCountry: row.PersonMailingCountry || "",
        Suggested_PersonMailingCountryCode:
          row.Suggested_PersonMailingCountryCode || "",
        Suggested_PersonMailingStateCode:
          row.Suggested_PersonMailingStateCode || "",
        State_Country_Triage_Detail: row.State_Country_Triage_Detail || "",
        Record_Count: 0,
      });
    }

    groups.get(key).Record_Count += 1;
  }

  return [...groups.values()].sort((a, b) => {
    if (b.Record_Count !== a.Record_Count) return b.Record_Count - a.Record_Count;
    return `${a.Failed_Field}${a.PersonMailingState}${a.PersonMailingCountry}`.localeCompare(
      `${b.Failed_Field}${b.PersonMailingState}${b.PersonMailingCountry}`,
    );
  });
}

function renderAggregatedTable(rows, limit) {
  const limitedRows = rows.slice(0, limit);
  return [
    "| Category | Field | State Value | Country Value | Suggested CountryCode | Suggested StateCode | Count |",
    "|---|---|---|---|---|---|---:|",
    ...limitedRows.map((row) =>
      [
        row.State_Country_Triage_Category,
        row.Failed_Field,
        row.PersonMailingState || "",
        row.PersonMailingCountry || "",
        row.Suggested_PersonMailingCountryCode || "",
        row.Suggested_PersonMailingStateCode || "",
        row.Record_Count,
      ]
        .map((value, index) =>
          index === 6
            ? String(value)
            : String(value).replace(/\|/g, "\\|"),
        )
        .join(" | "),
    ).map((line) => `| ${line} |`),
  ].join("\n");
}

function writeAnnotatedCsv(inputFile) {
  const parsed = Papa.parse(fs.readFileSync(inputFile, "utf8"), {
    header: true,
    skipEmptyLines: true,
  });
  if (parsed.errors.length) {
    throw new Error(
      `CSV parse failed for ${inputFile}: ${JSON.stringify(parsed.errors.slice(0, 5))}`,
    );
  }

  const originalFields = parsed.meta.fields || Object.keys(parsed.data[0] || {});
  const addedFields = [
    "State_Country_Triage_Category",
    "State_Country_Triage_Detail",
    "Suggested_PersonMailingCountryCode",
    "Suggested_PersonMailingStateCode",
  ];

  const annotatedRows = parsed.data.map((row) => {
    const triage = classifyRow(row);
    return {
      ...row,
      State_Country_Triage_Category: triage.category,
      State_Country_Triage_Detail: triage.detail,
      Suggested_PersonMailingCountryCode: triage.suggestedCountryCode,
      Suggested_PersonMailingStateCode: triage.suggestedStateCode,
    };
  });

  const basename = path.basename(inputFile, ".csv");
  const outputFile = path.join(OUTPUT_DIR, `${basename} - Annotated.csv`);
  fs.writeFileSync(
    outputFile,
    Papa.unparse(annotatedRows, {
      columns: [...originalFields, ...addedFields],
      newline: "\n",
    }),
  );

  return { inputFile, outputFile, rows: annotatedRows };
}

function buildSummary(results) {
  const allRows = results.flatMap((result) => result.rows);
  const aggregatedRows = aggregateAddressRows(allRows);
  const addressRows = allRows.filter((row) => {
    const message = row["Error Message"] || "";
    return message.includes("PersonMailingState") || message.includes("PersonMailingCountry");
  });
  const stateRows = addressRows.filter((row) =>
    (row["Error Message"] || "").includes("PersonMailingState"),
  );
  const countryRows = addressRows.filter((row) =>
    (row["Error Message"] || "").includes("PersonMailingCountry"),
  );

  const categoryCounts = new Map();
  const categoryFieldCounts = new Map();

  for (const row of addressRows) {
    const category = row.State_Country_Triage_Category;
    const field = (row["Error Message"] || "").includes("PersonMailingState")
      ? "State"
      : "Country";
    categoryCounts.set(category, (categoryCounts.get(category) || 0) + 1);
    const key = `${category}|${field}`;
    categoryFieldCounts.set(key, (categoryFieldCounts.get(key) || 0) + 1);
  }

  const byFileRows = results.map((result) => {
    const rows = result.rows;
    const address = rows.filter((row) => {
      const message = row["Error Message"] || "";
      return message.includes("PersonMailingState") || message.includes("PersonMailingCountry");
    });
    const counts = new Map();
    for (const row of address) {
      const category = row.State_Country_Triage_Category;
      counts.set(category, (counts.get(category) || 0) + 1);
    }
    const stateCount = address.filter((row) =>
      (row["Error Message"] || "").includes("PersonMailingState"),
    ).length;
    const countryCount = address.length - stateCount;
    return `| ${path.basename(result.inputFile)} | ${rows.length} | ${address.length} | ${stateCount} | ${countryCount} | ${counts.get(CATEGORY_CODE) || 0} | ${counts.get(CATEGORY_CONFIG) || 0} | ${counts.get(CATEGORY_CLIENT) || 0} |`;
  });

  const topCode = countBy(
    addressRows.filter((row) => row.State_Country_Triage_Category === CATEGORY_CODE),
    "State_Country_Triage_Detail",
  ).slice(0, 15);
  const topConfig = countBy(
    addressRows.filter((row) => row.State_Country_Triage_Category === CATEGORY_CONFIG),
    "State_Country_Triage_Detail",
  ).slice(0, 10);
  const topClient = countBy(
    addressRows.filter((row) => row.State_Country_Triage_Category === CATEGORY_CLIENT),
    "State_Country_Triage_Detail",
  ).slice(0, 10);

  const aggregationFile = path.join(
    OUTPUT_DIR,
    "state-country-error-triage-aggregation.csv",
  );
  fs.writeFileSync(
    aggregationFile,
    Papa.unparse(aggregatedRows, {
      columns: [
        "State_Country_Triage_Category",
        "Failed_Field",
        "PersonMailingState",
        "PersonMailingCountry",
        "Suggested_PersonMailingCountryCode",
        "Suggested_PersonMailingStateCode",
        "Record_Count",
        "State_Country_Triage_Detail",
      ],
      newline: "\n",
    }),
  );

  const codeGroups = aggregatedRows.filter(
    (row) => row.State_Country_Triage_Category === CATEGORY_CODE,
  );
  const configGroups = aggregatedRows.filter(
    (row) => row.State_Country_Triage_Category === CATEGORY_CONFIG,
  );
  const clientGroups = aggregatedRows.filter(
    (row) => row.State_Country_Triage_Category === CATEGORY_CLIENT,
  );

  const lines = [
    "# Student Account State/Country Error Triage",
    "",
    `Generated from:`,
    ...results.map((result) => `- ${result.inputFile}`),
    `- ${ADDRESS_SETTINGS}`,
    "",
    "## Scope",
    "",
    `The two CSVs contain ${allRows.length} total failed Account rows. This report classifies only Mailing State/Country picklist failures; non-address failures remain in the annotated CSVs as '${CATEGORY_OTHER}'.`,
    "",
    "## Address Error Summary",
    "",
    "| Category | Records | State | Country |",
    "|---|---:|---:|---:|",
    `| ${CATEGORY_CODE} | ${categoryCounts.get(CATEGORY_CODE) || 0} | ${categoryFieldCounts.get(`${CATEGORY_CODE}|State`) || 0} | ${categoryFieldCounts.get(`${CATEGORY_CODE}|Country`) || 0} |`,
    `| ${CATEGORY_CONFIG} | ${categoryCounts.get(CATEGORY_CONFIG) || 0} | ${categoryFieldCounts.get(`${CATEGORY_CONFIG}|State`) || 0} | ${categoryFieldCounts.get(`${CATEGORY_CONFIG}|Country`) || 0} |`,
    `| ${CATEGORY_CLIENT} | ${categoryCounts.get(CATEGORY_CLIENT) || 0} | ${categoryFieldCounts.get(`${CATEGORY_CLIENT}|State`) || 0} | ${categoryFieldCounts.get(`${CATEGORY_CLIENT}|Country`) || 0} |`,
    `| **Total Address Errors** | **${addressRows.length}** | **${stateRows.length}** | **${countryRows.length}** |`,
    "",
    "## By File",
    "",
    "| File | Total Failed Rows | Address Errors | State Errors | Country Errors | Code | Salesforce Config | Client Clarification |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
    ...byFileRows,
    "",
    "## Triage Rules",
    "",
    `- ${CATEGORY_CODE}: the row can be deterministically normalized by script to Salesforce country/state code fields.`,
    `- ${CATEGORY_CONFIG}: the value is missing or inactive in Salesforce Address.settings metadata and requires Salesforce admin configuration or an approved org-level mapping.`,
    `- ${CATEGORY_CLIENT}: the row has conflicting country/state data, a city/county/country in the wrong field, or a value that requires a client decision before code or config should change it.`,
    `- ${CATEGORY_OTHER}: the row failed for a non-address reason and is outside this state/country triage.`,
    "",
    "## Output CSV Columns Added",
    "",
    "- `State_Country_Triage_Category`",
    "- `State_Country_Triage_Detail`",
    "- `Suggested_PersonMailingCountryCode`",
    "- `Suggested_PersonMailingStateCode`",
    "",
    "## Common Code-Fix Details",
    "",
    renderTable(topCode),
    "",
    "## Top Code-Fix Values",
    "",
    renderAggregatedTable(codeGroups, 30),
    "",
    "## Common Salesforce-Config Details",
    "",
    renderTable(topConfig),
    "",
    "## Top Salesforce-Config Values",
    "",
    renderAggregatedTable(configGroups, 30),
    "",
    "## Common Client-Clarification Details",
    "",
    renderTable(topClient),
    "",
    "## Top Client-Clarification Values",
    "",
    renderAggregatedTable(clientGroups, 30),
    "",
    "## Generated Files",
    "",
    ...results.map((result) => `- ${result.outputFile}`),
    `- ${aggregationFile}`,
  ];

  const summaryFile = path.join(OUTPUT_DIR, "state-country-error-triage-summary.md");
  fs.writeFileSync(summaryFile, `${lines.join("\n")}\n`);
  return summaryFile;
}

fs.mkdirSync(OUTPUT_DIR, { recursive: true });
const results = INPUT_FILES.map(writeAnnotatedCsv);
const summaryFile = buildSummary(results);

console.log(`Wrote ${results.length} annotated CSV files.`);
for (const result of results) console.log(result.outputFile);
console.log(summaryFile);
