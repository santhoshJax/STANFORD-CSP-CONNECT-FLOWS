/**
 * Shared helpers used by both Student Import and Instructor Import flows.
 */

import axios from "axios";
import Papa, { parse, type ParseResult } from "papaparse";
import type { Connection } from "@prismatic-io/spectral";

// ── Constants ──────────────────────────────────────────────────────────────────

export const POLL_INTERVAL_MS = 2_000;
export const SF_API_VERSION = "v60.0";

// ── Lookup maps ────────────────────────────────────────────────────────────────

export const US_STATES: Record<string, string> = {
  // Military "state" codes — added Aug 2026. Verified active in Stanford_UAT
  // Address Settings under United States; previously missing here, so these
  // three passed straight through unnormalized (e.g. "AP" instead of
  // "Armed Forces Pacific").
  AA: "Armed Forces Americas",
  AE: "Armed Forces Europe",
  AP: "Armed Forces Pacific",
  AL: "Alabama",
  AK: "Alaska",
  AZ: "Arizona",
  AR: "Arkansas",
  CA: "California",
  CO: "Colorado",
  CT: "Connecticut",
  DE: "Delaware",
  FL: "Florida",
  GA: "Georgia",
  HI: "Hawaii",
  ID: "Idaho",
  IL: "Illinois",
  IN: "Indiana",
  IA: "Iowa",
  KS: "Kansas",
  KY: "Kentucky",
  LA: "Louisiana",
  ME: "Maine",
  MD: "Maryland",
  MA: "Massachusetts",
  MI: "Michigan",
  MN: "Minnesota",
  MS: "Mississippi",
  MO: "Missouri",
  MT: "Montana",
  NE: "Nebraska",
  NV: "Nevada",
  NH: "New Hampshire",
  NJ: "New Jersey",
  NM: "New Mexico",
  NY: "New York",
  NC: "North Carolina",
  ND: "North Dakota",
  OH: "Ohio",
  OK: "Oklahoma",
  OR: "Oregon",
  PA: "Pennsylvania",
  RI: "Rhode Island",
  SC: "South Carolina",
  SD: "South Dakota",
  TN: "Tennessee",
  TX: "Texas",
  UT: "Utah",
  VT: "Vermont",
  VA: "Virginia",
  WA: "Washington",
  WV: "West Virginia",
  WI: "Wisconsin",
  WY: "Wyoming",
  DC: "District of Columbia",
  PR: "Puerto Rico",
  GU: "Guam",
  VI: "US Virgin Islands",
  AS: "American Samoa",
  MP: "Northern Mariana Islands",
};

export const COUNTRIES: Record<string, string> = {
  US: "United States",
  CA: "Canada",
  MX: "Mexico",
  GB: "United Kingdom",
  AU: "Australia",
  NZ: "New Zealand",
  IN: "India",
  CN: "China",
  JP: "Japan",
  // Label matches the org's Address Settings picklist exactly (verified against
  // Stanford_UAT, Aug 2026) — "South Korea" was the old label and would not have
  // matched the picklist. Existing aliases (SOUTH KOREA, KOREA, etc. below) still
  // resolve here, so old input text keeps working.
  KR: "Korea, Republic of",
  DE: "Germany",
  FR: "France",
  IT: "Italy",
  ES: "Spain",
  PT: "Portugal",
  NL: "Netherlands",
  BE: "Belgium",
  CH: "Switzerland",
  AT: "Austria",
  SE: "Sweden",
  NO: "Norway",
  DK: "Denmark",
  FI: "Finland",
  IE: "Ireland",
  PL: "Poland",
  // Label matches the org's Address Settings picklist exactly (verified against
  // Stanford_UAT, Aug 2026) — "Russia" was the old label. Existing aliases
  // (RUSSIA, RUSSIAN FEDERATION below) still resolve here.
  RU: "Russian Federation",
  BR: "Brazil",
  AR: "Argentina",
  CL: "Chile",
  CO: "Colombia",
  PE: "Peru",
  // Label matches the org's Address Settings picklist exactly (verified against
  // Stanford_UAT, Aug 2026) — "Venezuela" was the old label. Unlike KR/RU above,
  // there was no explicit alias for the old name, so one is added below
  // (VENEZUELA -> VE) to keep old input text working after this label change.
  VE: "Venezuela, Bolivarian Republic of",
  ZA: "South Africa",
  NG: "Nigeria",
  KE: "Kenya",
  EG: "Egypt",
  IL: "Israel",
  SA: "Saudi Arabia",
  AE: "United Arab Emirates",
  SG: "Singapore",
  // Not used directly for HK input — this org has no top-level "Hong Kong"
  // country entry; Hong Kong exists only as a subdivision of China (isoCode
  // 91). See resolveCountryAndState() below, which redirects HK/Hong Kong
  // input to Country="China", State="Hong Kong" before this table is even
  // consulted. Kept here only so normalizeCountryName() still returns a
  // sensible value if ever called directly, outside that redirect.
  HK: "Hong Kong",
  TW: "Taiwan",
  TH: "Thailand",
  PH: "Philippines",
  MY: "Malaysia",
  ID: "Indonesia",
  PK: "Pakistan",
  BD: "Bangladesh",
  LK: "Sri Lanka",
  NP: "Nepal",
  // Added Aug 2026 — active in Stanford_UAT Address Settings but previously
  // missing here, so these codes (and any full-name variant of them) were
  // passing straight through unnormalized. Labels verified directly against
  // the org, not just the plain English name (several differ, e.g. Turkey →
  // "Türkiye", Russia-style ", Republic of"/", Plurinational State of" forms).
  TR: "Türkiye",
  VN: "Vietnam",
  IR: "Iran",
  CZ: "Czechia",
  BO: "Bolivia, Plurinational State of",
  TZ: "Tanzania, United Republic of",
  CD: "Congo, the Democratic Republic of the",
  CU: "Cuba",
  MD: "Moldova, Republic of",
  PS: "Palestine",
  AW: "Aruba",
  BY: "Belarus",
  CG: "Congo",
  CI: "Cote d'Ivoire",
  CW: "Curaçao",
  CY: "Cyprus",
  DZ: "Algeria",
  GA: "Gabon",
  GE: "Georgia",
  GH: "Ghana",
  GR: "Greece",
  GT: "Guatemala",
  HR: "Croatia",
  IS: "Iceland",
  JO: "Jordan",
  KI: "Kiribati",
  KZ: "Kazakhstan",
  LA: "Lao People's Democratic Republic",
  LB: "Lebanon",
  LT: "Lithuania",
  LV: "Latvia",
  MK: "North Macedonia",
  MO: "Macao",
  RO: "Romania",
  SD: "Sudan",
  SV: "El Salvador",
  UA: "Ukraine",
  // Full-text / abbreviated aliases (keys are always matched uppercased)
  USA: "United States",
  "U.S.A.": "United States",
  "U.S.": "United States",
  "UNITED STATES OF AMERICA": "United States",
  "UNITED STATES": "United States",
  "HONG KONG": "Hong Kong",
  "REPUBLIC OF KOREA": "South Korea",
  "KOREA, REPUBLIC OF": "South Korea",
};

// ── String / value helpers ─────────────────────────────────────────────────────

/** Strip \r from a string value. */
export function stripCr(v: string): string {
  return v.replace(/\r/g, "");
}

/** Return a clean string from a possibly-undefined raw value. */
export function str(v: string | undefined): string {
  return v !== undefined ? stripCr(v).trim() : "";
}

/**
 * Clean an email value from the source TSV:
 * - Multiple comma-separated emails → blank (can't pick one, migrate without email)
 * - "Name: email@domain" format → extract the email portion only
 * - Trailing period → stripped
 * - Anything that doesn't resolve to a valid single email → blank
 */
export function cleanEmail(v: string | undefined): string {
  const val = str(v);
  if (!val) return "";
  if (val.includes(",")) return "";
  const match = val.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/);
  if (!match) return "";
  const email = match[0].replace(/\.$/, "");
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : "";
}

/** Returns true when the raw source string has a recognisable value (non-blank). */
export function hasValue(v: string | undefined): boolean {
  return (v ?? "").trim().length > 0;
}

/**
 * Returns a shallow copy of `obj` containing only the listed keys whose
 * value is not undefined. Used to build the fixed "overlay" field set
 * merged onto an existing Student/Instructor/Associate record when a
 * cross-match is found, without touching any other field on that record.
 */
export function pickFields<T extends Record<string, unknown>>(
  obj: T,
  fields: (keyof T)[],
): Partial<T> {
  const out: Partial<T> = {};
  for (const f of fields) {
    if (obj[f] !== undefined) out[f] = obj[f];
  }
  return out;
}

// ── Salesforce state/country code helpers ─────────────────────────────────────

function addressCodeKey(v: string | undefined): string {
  return str(v)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, "AND")
    .replace(/[^A-Z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

const COUNTRY_CODE_ALIASES: Record<string, string> = {
  USA: "US",
  "U S A": "US",
  "U S": "US",
  "U S A REALLY YOU NEED THIS": "US",
  "USA N": "US",
  "US OF A": "US",
  "UNITED STATES OF AMERICA": "US",
  "THE UNITED STATES OF AMERICA": "US",
  "UNITED STATES": "US",
  "UNITED STA": "US",
  "UNITED STA OF AM": "US",
  "UNITES STATES": "US",
  "UNITESD STATES": "US",
  "UINITED STATES": "US",
  "UNITIED STATES": "US",
  "UNITED STATED": "US",
  "UNITED STATE": "US",
  "ESTADOS UNIDOS": "US",
  "COUNTRY US": "US",
  UK: "GB",
  "U K": "GB",
  "UNITED KINGDON": "GB",
  "ENGLAND UK": "GB",
  UAE: "AE",
  "HONG KONG": "HK",
  RUSSIA: "RU",
  "RUSSIAN FEDERATION": "RU",
  VENEZUELA: "VE",
  "VENEZUELA, BOLIVARIAN REPUBLIC OF": "VE",
  TURKEY: "TR",
  TURKIYE: "TR",
  "VIET NAM": "VN",
  VIETNAM: "VN",
  "SOUTH KOREA": "KR",
  "KOREA SOUTH": "KR",
  "KOREA REPUBLIC OF SOUTH": "KR",
  "KOREA REPUBLIC OF": "KR",
  "REPUBLIC OF KOREA": "KR",
  KOREA: "KR",
  "CZECH REPUBLIC": "CZ",
  CZECHIA: "CZ",
  BOLIVIA: "BO",
  TANZANIA: "TZ",
  "TANZANIA UNITED REPUBLIC OF": "TZ",
  IND: "IN",
  IRAN: "IR",
  SUDAN: "SD",
  CANANDA: "CA",
  ASTRALIA: "AU",
  BRASIL: "BR",
  "PEOPLE S REPUBLIC OF CHINA": "CN",
  "TAIWAN R O C": "TW",
  "THE NETHERLANDS": "NL",
  NETHERLAND: "NL",
  ROMANI: "RO",
  LIBANON: "LB",
  "SULTANATE OF OMAN": "OM",
  "REPUBLIC OF MOLDOVA": "MD",
  CURACAO: "CW",
  MACAU: "MO",
  LAOS: "LA",
  "COTE DIVOIRE": "CI",
  "COLOMBIA COLOMBIA": "CO",
};

const COUNTRY_NAME_TO_CODE = Object.entries(COUNTRIES).reduce<
  Record<string, string>
>((acc, [code, name]) => {
  if (!/^[A-Z]{2}$/.test(code)) return acc;

  acc[addressCodeKey(code)] = code;
  acc[addressCodeKey(name)] = code;
  return acc;
}, {});

export function toSalesforceCountryCode(v: string | undefined): string {
  const raw = str(v);
  if (!raw) return "";

  const key = addressCodeKey(raw);
  return COUNTRY_CODE_ALIASES[key] ?? COUNTRY_NAME_TO_CODE[key] ?? raw;
}

/**
 * Normalizes common country name variants to a proper full country name.
 * e.g. "USA", "U.S.A.", "united states of america" → "United States"
 *      "IN", "india" → "India"
 * Returns the original value unchanged if no match is found.
 */
export function normalizeCountryName(v: string | undefined): string {
  const raw = str(v);
  if (!raw) return "";

  const key = addressCodeKey(raw);
  const code = COUNTRY_CODE_ALIASES[key] ?? COUNTRY_NAME_TO_CODE[key];
  if (code) return COUNTRIES[code] ?? raw;

  return raw;
}

export function normalizeStateName(v: string | undefined): string {
  const raw = str(v).trim();
  if (!raw) return "";
  const code = expandState(v);
  return US_STATES[code.toUpperCase()] ?? raw;
}

/** True when the raw country input represents Hong Kong (code or full name). */
function isHongKongCountry(v: string | undefined): boolean {
  const key = str(v).trim().toUpperCase();
  return key === "HK" || key === "HONG KONG";
}

/**
 * True for literal placeholder text ("N/A", "NA", etc.) that sometimes shows
 * up in a State column but was never meant as a real state value. Exported
 * so correction-mode overlay code (which sets State independently of
 * Country) can apply the same check without duplicating the literal list.
 */
export function isBlankStatePlaceholder(v: string): boolean {
  const key = v.trim().toUpperCase();
  return key === "N/A" || key === "NA" || key === "N A" || key === "N.A.";
}

/**
 * Resolves the final PersonMailingCountry / PersonMailingState pair together
 * from raw source values. This is the one place all three flows (Student,
 * Instructor, Associate) should go through for country+state, so the
 * Hong Kong and placeholder-state handling below stay consistent everywhere.
 *
 *   - Country is normalized via normalizeCountryName as usual.
 *   - State is normalized via normalizeStateName only when country resolves
 *     to "United States" (matches each flow's existing behavior).
 *   - Hong Kong special case: this org has no top-level "Hong Kong" country —
 *     Hong Kong exists only as a subdivision of China (Address Settings,
 *     verified Aug 2026). So HK/Hong Kong input redirects to
 *     Country="China", State="Hong Kong" instead of Country="Hong Kong".
 *   - "N/A"-style placeholder state values are treated as no value at all.
 */
export function resolveCountryAndState(
  rawCountry: string | undefined,
  rawState: string | undefined,
): { country: string; state: string } {
  if (isHongKongCountry(rawCountry)) {
    return { country: "China", state: "Hong Kong" };
  }

  const country = normalizeCountryName(rawCountry);
  let state =
    country === "United States"
      ? normalizeStateName(rawState)
      : !country
        ? str(rawState)
        : "";

  if (isBlankStatePlaceholder(state)) state = "";

  return { country, state };
}

function addStateCodeAliases(
  target: Record<string, string>,
  code: string,
  names: string[],
): void {
  target[addressCodeKey(code)] = code;
  for (const name of names) target[addressCodeKey(name)] = code;
}

function buildStateCodeMap(
  entries: [string, string[]][],
): Record<string, string> {
  const map: Record<string, string> = {};
  for (const [code, names] of entries) addStateCodeAliases(map, code, names);
  return map;
}

const US_STATE_CODES = Object.entries(US_STATES).reduce<Record<string, string>>(
  (acc, [code, name]) => {
    addStateCodeAliases(acc, code, [name]);
    return acc;
  },
  {},
);
addStateCodeAliases(US_STATE_CODES, "CA", [
  "CA - CALIFORNIA",
  "CA CALIFORNIA",
  "CA-CALIFORNIA",
  "CALIFORNIA (CA)",
  "CALIFORNIA [CA]",
  "CALIF",
  "CALIF.",
  "CALIFONIA",
  "CALIFORONIA",
  "NORTHERN CALIFORNIA",
  "CALIFORNIA NORTH",
  "CALIFORNIA - NORTH",
  "CALIFORNIA - CA",
]);

const STATE_CODES_BY_COUNTRY: Record<string, Record<string, string>> = {
  US: US_STATE_CODES,
  CA: buildStateCodeMap([
    ["AB", ["Alberta"]],
    ["BC", ["British Columbia"]],
    ["MB", ["Manitoba"]],
    ["NB", ["New Brunswick"]],
    ["NL", ["Newfoundland and Labrador"]],
    ["NS", ["Nova Scotia"]],
    ["NT", ["Northwest Territories"]],
    ["NU", ["Nunavut"]],
    ["ON", ["Ontario"]],
    ["PE", ["Prince Edward Island"]],
    ["QC", ["Quebec"]],
    ["SK", ["Saskatchewan"]],
    ["YT", ["Yukon Territories", "Yukon"]],
  ]),
  BR: buildStateCodeMap([
    ["AC", ["Acre"]],
    ["AL", ["Alagoas"]],
    ["AM", ["Amazonas"]],
    ["AP", ["Amapa", "Amapá"]],
    ["BA", ["Bahia"]],
    ["CE", ["Ceara", "Ceará"]],
    ["DF", ["Distrito Federal", "Federal District"]],
    ["ES", ["Espirito Santo", "Espírito Santo"]],
    ["GO", ["Goias", "Goiás"]],
    ["MA", ["Maranhao", "Maranhão"]],
    ["MG", ["Minas Gerais"]],
    ["MS", ["Mato Grosso do Sul"]],
    ["MT", ["Mato Grosso"]],
    ["PA", ["Para", "Pará"]],
    ["PB", ["Paraiba", "Paraíba"]],
    ["PE", ["Pernambuco"]],
    ["PI", ["Piaui", "Piauí"]],
    ["PR", ["Parana", "Paraná"]],
    ["RJ", ["Rio de Janeiro"]],
    ["RN", ["Rio Grande do Norte"]],
    ["RO", ["Rondonia", "Rondônia"]],
    ["RR", ["Roraima"]],
    ["RS", ["Rio Grande do Sul"]],
    ["SC", ["Santa Catarina"]],
    ["SE", ["Sergipe"]],
    ["SP", ["Sao Paulo", "São Paulo"]],
    ["TO", ["Tocantins"]],
  ]),
  AU: buildStateCodeMap([
    ["ACT", ["Australian Capital Territory"]],
    ["NSW", ["New South Wales"]],
    ["NT", ["Northern Territory"]],
    ["QLD", ["Queensland"]],
    ["SA", ["South Australia"]],
    ["TAS", ["Tasmania"]],
    ["VIC", ["Victoria"]],
    ["WA", ["Western Australia"]],
  ]),
  MX: buildStateCodeMap([
    ["AG", ["Aguascalientes"]],
    ["BC", ["Baja California"]],
    ["BS", ["Baja California Sur"]],
    ["CH", ["Chihuahua"]],
    ["CL", ["Colima"]],
    ["CM", ["Campeche"]],
    ["CO", ["Coahuila"]],
    ["CS", ["Chiapas"]],
    ["DF", ["Federal District", "Mexico City", "CDMX", "Ciudad de Mexico"]],
    ["DG", ["Durango"]],
    ["GR", ["Guerrero"]],
    ["GT", ["Guanajuato"]],
    ["HG", ["Hidalgo"]],
    ["JA", ["Jalisco"]],
    ["ME", ["Mexico State"]],
    ["MI", ["Michoacan", "Michoacán"]],
    ["MO", ["Morelos"]],
    ["NA", ["Nayarit"]],
    ["NL", ["Nuevo Leon", "Nuevo León"]],
    ["OA", ["Oaxaca"]],
    ["PB", ["Puebla"]],
    ["QE", ["Queretaro", "Querétaro"]],
    ["QR", ["Quintana Roo"]],
    ["SI", ["Sinaloa"]],
    ["SL", ["San Luis Potosi", "San Luis Potosí"]],
    ["SO", ["Sonora"]],
    ["TB", ["Tabasco"]],
    ["TL", ["Tlaxcala"]],
    ["TM", ["Tamaulipas"]],
    ["VE", ["Veracruz"]],
    ["YU", ["Yucatan", "Yucatán"]],
    ["ZA", ["Zacatecas"]],
  ]),
  IN: buildStateCodeMap([
    ["AN", ["Andaman and Nicobar Islands"]],
    ["AP", ["Andhra Pradesh"]],
    ["AR", ["Arunachal Pradesh"]],
    ["AS", ["Assam"]],
    ["BR", ["Bihar"]],
    ["CH", ["Chandigarh"]],
    ["CT", ["Chhattisgarh"]],
    ["DD", ["Daman and Diu"]],
    ["DL", ["Delhi", "New Delhi"]],
    ["DN", ["Dadra and Nagar Haveli"]],
    ["GA", ["Goa"]],
    ["GJ", ["Gujarat"]],
    ["HP", ["Himachal Pradesh"]],
    ["HR", ["Haryana"]],
    ["JH", ["Jharkhand"]],
    ["JK", ["Jammu and Kashmir", "Jammu", "Kashmir", "J&K"]],
    ["KA", ["Karnataka"]],
    ["KL", ["Kerala"]],
    ["LD", ["Lakshadweep"]],
    ["MH", ["Maharashtra"]],
    ["ML", ["Meghalaya"]],
    ["MN", ["Manipur"]],
    ["MP", ["Madhya Pradesh"]],
    ["MZ", ["Mizoram"]],
    ["NL", ["Nagaland"]],
    ["OR", ["Odisha", "Orissa"]],
    ["PB", ["Punjab"]],
    ["PY", ["Puducherry", "Pondicherry"]],
    ["RJ", ["Rajasthan"]],
    ["SK", ["Sikkim"]],
    ["TG", ["Telangana"]],
    ["TN", ["Tamil Nadu", "Tamilnadu"]],
    ["TR", ["Tripura"]],
    ["UP", ["Uttar Pradesh"]],
    ["UT", ["Uttarakhand", "Uttaranchal"]],
    ["WB", ["West Bengal"]],
  ]),
  JP: buildStateCodeMap([
    ["01", ["Hokkaido"]],
    ["02", ["Aomori"]],
    ["03", ["Iwate"]],
    ["04", ["Miyagi"]],
    ["05", ["Akita"]],
    ["06", ["Yamagata"]],
    ["07", ["Fukushima"]],
    ["08", ["Ibaraki"]],
    ["09", ["Tochigi"]],
    ["10", ["Gunma"]],
    ["11", ["Saitama"]],
    ["12", ["Chiba"]],
    ["13", ["Tokyo", "Tokyo Japan"]],
    ["14", ["Kanagawa"]],
    ["15", ["Niigata"]],
    ["16", ["Toyama"]],
    ["17", ["Ishikawa"]],
    ["18", ["Fukui"]],
    ["19", ["Yamanashi"]],
    ["20", ["Nagano"]],
    ["21", ["Gifu"]],
    ["22", ["Shizuoka"]],
    ["23", ["Aichi"]],
    ["24", ["Mie"]],
    ["25", ["Shiga"]],
    ["26", ["Kyoto"]],
    ["27", ["Osaka"]],
    ["28", ["Hyogo"]],
    ["29", ["Nara"]],
    ["30", ["Wakayama"]],
    ["31", ["Tottori"]],
    ["32", ["Shimane"]],
    ["33", ["Okayama"]],
    ["34", ["Hiroshima"]],
    ["35", ["Yamaguchi"]],
    ["36", ["Tokushima"]],
    ["37", ["Kagawa"]],
    ["38", ["Ehime"]],
    ["39", ["Kochi"]],
    ["40", ["Fukuoka"]],
    ["41", ["Saga"]],
    ["42", ["Nagasaki"]],
    ["43", ["Kumamoto"]],
    ["44", ["Oita"]],
    ["45", ["Miyazaki"]],
    ["46", ["Kagoshima"]],
    ["47", ["Okinawa"]],
  ]),
  DE: buildStateCodeMap([
    ["BB", ["Brandenburg"]],
    ["BE", ["Berlin"]],
    ["BW", ["Baden-Württemberg", "Baden-Wurttemberg", "Baden Wurttemberg"]],
    ["BY", ["Bavaria", "Bayern"]],
    ["HB", ["Bremen"]],
    ["HE", ["Hesse", "Hessen"]],
    ["HH", ["Hamburg"]],
    ["MV", ["Mecklenburg-Vorpommern", "Mecklenburg Vorpommern"]],
    ["NI", ["Lower Saxony", "Niedersachsen"]],
    ["NW", ["North Rhine-Westphalia", "Nordrhein-Westfalen", "NRW"]],
    ["RP", ["Rhineland-Palatinate", "Rheinland-Pfalz"]],
    ["SH", ["Schleswig-Holstein"]],
    ["SL", ["Saarland"]],
    ["SN", ["Saxony", "Sachsen"]],
    ["ST", ["Saxony-Anhalt", "Sachsen-Anhalt"]],
    ["TH", ["Thuringia", "Thüringen", "Thuringen"]],
  ]),
  GB: buildStateCodeMap([
    ["ENG", ["England"]],
    ["NIR", ["Northern Ireland"]],
    ["SCT", ["Scotland"]],
    ["WLS", ["Wales"]],
  ]),
};

export function toSalesforceStateCode(
  stateValue: string | undefined,
  countryCode: string,
): string {
  const raw = str(stateValue);
  const stateKey = addressCodeKey(raw);

  if (!stateKey || ["NA", "N A", "NONE", "NULL"].includes(stateKey)) {
    return "";
  }

  const stateMap = STATE_CODES_BY_COUNTRY[countryCode];
  if (!stateMap) return raw;

  const leading = /^([A-Z]{2,3})\s+/.exec(stateKey);
  if (leading && stateMap[leading[1]]) return leading[1];

  const bracketed = /\b([A-Z]{2,3})\b/.exec(stateKey);
  if (bracketed && stateMap[bracketed[1]]) return bracketed[1];

  return stateMap[stateKey] ?? raw;
}

/**
 * Convert truthy-ish strings ("true", "yes", "1", "y") → true,
 * everything else → false.
 */
export function toBool(v: string | undefined): boolean {
  return ["true", "yes", "1", "y"].includes((v ?? "").toLowerCase().trim());
}

/** Convert truthy-ish strings → "Yes", everything else → "No". */
export function toYesNo(v: string | undefined): "Yes" | "No" {
  return ["true", "yes", "1", "y"].includes((v ?? "").toLowerCase().trim())
    ? "Yes"
    : "No";
}

/**
 * Normalises a University ID for the "exactly 8 numeric digits" Salesforce
 * validation rule. A 7-digit value is zero-padded to 8 (a leading zero is
 * commonly dropped upstream, e.g. by Excel treating the column as a number).
 * Any other length, or a non-numeric value, is invalid — returns "".
 * Correction-mode use only (Instructor, Associate); does not affect normal
 * full-import mapping.
 */
export function normalizeUniversityId(v: string | undefined): string {
  const raw = (v ?? "").trim();
  const padded = /^\d{7}$/.test(raw) ? `0${raw}` : raw;
  return /^\d{8}$/.test(padded) ? padded : "";
}

/**
 * Normalise a date to YYYY-MM-DD.
 * Handles M/D/YYYY, MM/DD/YYYY, and ISO-8601 input.
 * Returns "" for blank or unparseable values.
 */
export function toDate(v: string | undefined): string {
  const s = (v ?? "").trim();
  if (!s) return "";

  // Junk placeholder dates ("00/00/00", "00/00/0000", "0000-00-00", etc.)
  // — every segment is all zeros. Must be checked before the M/D/YYYY regex
  // below: "00/00/0000" otherwise matches that pattern syntactically (a
  // 4-digit year of "0000") and would return the literal garbage string
  // "0000-00-00" instead of being rejected as junk. Centralised here so
  // every caller gets this for free, instead of each flow needing its own
  // local isJunkDatePlaceholder() guard before calling toDate().
  if (/^0+([./-]0+){2}$/.test(s)) return "";

  // M/D/YYYY or MM/DD/YYYY
  const mdy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    const [, m, d, y] = mdy;
    return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  }

  // Already YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // Fallback to native Date
  const parsed = new Date(s);
  return isNaN(parsed.getTime()) ? "" : parsed.toISOString().slice(0, 10);
}

// Keyed by addressCodeKey — covers abbreviations, typos, city names, and
// formatted codes not caught by the pattern-based logic in expandState.
const US_STATE_EXTRA_ALIASES: Record<string, string> = {
  // 3-letter abbreviations
  FLA: "FL",
  CAL: "CA",
  MASS: "MA",
  ORE: "OR",
  ILL: "IL",
  // Dotted abbreviations ("N.Y." → addressCodeKey → "N Y")
  "N Y": "NY",
  // City names
  NYC: "NY",
  "NEW YORK CITY": "NY",
  // "US-0-XX" format ("US-0-CA" → addressCodeKey → "US 0 CA")
  "US 0 CA": "CA",
  "US 0 PA": "PA",
  "US 0 OH": "OH",
  // California typos (in US_STATE_CODES but not caught by pattern logic)
  CALIFONIA: "CA",
  CALIFORONIA: "CA",
  // New Jersey typos
  NEWJERSEY: "NJ",
  "NEW JERESEY": "NJ",
  // Wisconsin with internal space
  "W I": "WI",
};

export function expandState(v: string | undefined): string {
  const raw = str(v).trim();
  const s = raw.toUpperCase();

  // Already a valid 2-letter code
  if (US_STATES[s]) return s;

  // Extra aliases: abbreviations, typos, city names, "US-0-XX" format, etc.
  const aliasCode = US_STATE_EXTRA_ALIASES[addressCodeKey(raw)];
  if (aliasCode) return aliasCode;

  // "CA - CALIFORNIA" or "TX-TEXAS" or "TX- TEXAS"
  const leading = s.match(/^([A-Z]{2})\s*-+\s*/);
  if (leading && US_STATES[leading[1]]) return leading[1];

  // "CALIFORNIA (CA)" or "CALIFORNIA [CA]"
  const bracketed = s.match(/[(\[]([A-Z]{2})[)\]]/);
  if (bracketed && US_STATES[bracketed[1]]) return bracketed[1];

  // "D.C." or "D.C"
  if (s === "D.C." || s === "D.C") return "DC";

  // "CALIF." or "CALIF"
  if (s === "CALIF." || s === "CALIF") return "CA";

  // Multi-state strings like "ID IDAHO, MT - MONTANA, FL" — take first valid code
  const first = s.match(/\b([A-Z]{2})\b/);
  if (first && US_STATES[first[1]]) return first[1];

  return raw;
}

/**
 * Expand a 2-letter ISO country code to its full name.
 * Returns the original value unchanged if already a full name or unrecognised.
 */
export function expandCountry(v: string | undefined): string {
  const s = str(v).trim();
  return COUNTRIES[s.toUpperCase()] ?? s;
}

// ── Prismatic connection helpers ───────────────────────────────────────────────

export function getAccessToken(conn: Connection): string {
  const token = conn.token?.access_token as string | undefined;
  if (!token) throw new Error(`Connection "${conn.key}" has no access_token.`);
  return token;
}

export function getSfInstanceUrl(sfConn: Connection): string {
  const url = (
    ((sfConn.token as Record<string, unknown> | undefined)?.instance_url as
      | string
      | undefined) ?? ""
  ).replace(/\/$/, "");
  if (!url)
    throw new Error(
      "Salesforce instance_url missing from OAuth token response.",
    );
  return url;
}

// ── Name match helper ─────────────────────────────────────────────────────────

/** Returns true if first+last match in either forward or swapped order (case-insensitive). */
export function nameMatches(
  sfFirst: string,
  sfLast: string,
  rawFirst: string,
  rawLast: string,
): boolean {
  const norm = (s: string) => s.toLowerCase().trim();
  const f = norm(rawFirst);
  const l = norm(rawLast);
  const sf = norm(sfFirst);
  const sl = norm(sfLast);
  return (sf === f && sl === l) || (sf === l && sl === f);
}

// ── Salesforce REST query helpers ─────────────────────────────────────────────

/**
 * Given a list of External_ID_4D__c values, returns a map of
 * { externalId → SF Account record Id }.
 * Used to resolve RelatedPersonId / AccountId before PersonEmployment upsert.
 */
export async function resolveAccountIdsByExternalId(
  instanceUrl: string,
  accessToken: string,
  externalIds: string[],
): Promise<Map<string, string>> {
  if (externalIds.length === 0) return new Map();

  const CHUNK_SIZE = 500;
  const map = new Map<string, string>();
  const headers = { Authorization: `Bearer ${accessToken}` };

  for (let i = 0; i < externalIds.length; i += CHUNK_SIZE) {
    const chunk = externalIds.slice(i, i + CHUNK_SIZE);
    const inClause = chunk
      .map((id) => `'${id.replace(/'/g, "\\'")}'`)
      .join(",");
    const soql = `SELECT Id, External_ID_4D__c FROM Account WHERE External_ID_4D__c IN (${inClause})`;
    const { data } = await axios.get(
      `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      { params: { q: soql }, headers },
    );
    for (const rec of data.records as Array<{
      Id: string;
      External_ID_4D__c: string;
    }>) {
      map.set(rec.External_ID_4D__c, rec.Id);
    }
  }

  return map;
}

/**
 * Generic version of resolveAccountIdsByExternalId.
 * Queries Account by any typed external-ID field and returns { fieldValue → SF Id }.
 */
export async function resolveAccountIdsByField(
  instanceUrl: string,
  accessToken: string,
  fieldName: string,
  values: string[],
): Promise<Map<string, string>> {
  if (values.length === 0) return new Map();

  const CHUNK_SIZE = 200;
  const map = new Map<string, string>();
  const headers = { Authorization: `Bearer ${accessToken}` };

  for (let i = 0; i < values.length; i += CHUNK_SIZE) {
    const chunk = values.slice(i, i + CHUNK_SIZE);
    const inClause = chunk.map((v) => `'${v.replace(/'/g, "\\'")}'`).join(",");
    const soql = `SELECT Id, ${fieldName} FROM Account WHERE ${fieldName} IN (${inClause})`;
    const { data } = await axios.get(
      `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      { params: { q: soql }, headers },
    );
    for (const rec of data.records as Array<Record<string, string>>) {
      map.set(rec[fieldName], rec.Id);
    }
  }

  return map;
}

/**
 * Given a list of External ID values, returns the subset that already exist
 * for the given object/field. Used to check whether a related record (e.g.
 * an Instructor-sourced PersonEmployment) already exists before deciding
 * whether to create a second one from a different source flow.
 */
export async function queryExistingExternalIds(
  instanceUrl: string,
  accessToken: string,
  objectName: string,
  externalIdField: string,
  values: string[],
): Promise<Set<string>> {
  if (values.length === 0) return new Set();

  const CHUNK_SIZE = 200;
  const found = new Set<string>();
  const headers = { Authorization: `Bearer ${accessToken}` };

  for (let i = 0; i < values.length; i += CHUNK_SIZE) {
    const chunk = values.slice(i, i + CHUNK_SIZE);
    const inClause = chunk.map((v) => `'${v.replace(/'/g, "\\'")}'`).join(",");
    const soql = `SELECT ${externalIdField} FROM ${objectName} WHERE ${externalIdField} IN (${inClause})`;
    const { data } = await axios.get(
      `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      { params: { q: soql }, headers },
    );
    for (const rec of data.records as Array<Record<string, string>>) {
      found.add(rec[externalIdField]);
    }
  }

  return found;
}

/**
 * Queries existing Person Accounts by email in batches of 200.
 * Returns { lowercasedEmail → { Id, FirstName, LastName, typed IDs } }.
 * Used for pre-match logic before inserting associate/instructor records.
 */
/** One Account row returned by queryAccountsByEmail. */
export interface EmailMatchAccount {
  Id: string;
  FirstName: string;
  LastName: string;
  Student_ID_4D__c: string;
  Associate_ID_4D__c: string;
  Instructor_ID_4D__c: string;
  University_ID__pc: string;
}

/**
 * PersonEmail is not guaranteed unique in this org — two unrelated people can
 * share the same email on file. Returns EVERY Account row matching each
 * email (not just one), so callers can disambiguate by name instead of
 * silently picking whichever row happened to be returned last.
 */
export async function queryAccountsByEmail(
  instanceUrl: string,
  accessToken: string,
  emails: string[],
): Promise<Map<string, EmailMatchAccount[]>> {
  if (emails.length === 0) return new Map();

  const CHUNK_SIZE = 200;
  const map = new Map<string, EmailMatchAccount[]>();
  const headers = { Authorization: `Bearer ${accessToken}` };

  for (let i = 0; i < emails.length; i += CHUNK_SIZE) {
    const chunk = emails.slice(i, i + CHUNK_SIZE);
    const inClause = chunk.map((e) => `'${e.replace(/'/g, "\\'")}'`).join(",");
    const soql =
      `SELECT Id, PersonEmail, FirstName, LastName, Student_ID_4D__c, Associate_ID_4D__c, Instructor_ID_4D__c, University_ID__pc ` +
      `FROM Account WHERE IsPersonAccount = true AND PersonEmail IN (${inClause})`;
    const { data } = await axios.get(
      `${instanceUrl}/services/data/${SF_API_VERSION}/query`,
      { params: { q: soql }, headers },
    );
    for (const rec of data.records as Array<
      EmailMatchAccount & { PersonEmail: string }
    >) {
      if (!rec.PersonEmail) continue;
      const key = rec.PersonEmail.toLowerCase();
      const existing = map.get(key);
      if (existing) existing.push(rec);
      else map.set(key, [rec]);
    }
  }

  return map;
}

// ── Course RecID → Course ID crosswalk ────────────────────────────────────────

/**
 * Streams the Course TSV and builds Map<RecID (numeric string), CourseID (text)>.
 * Used to resolve Course_Associate.Course_RecID → CourseOffering.External_ID_4D__c
 * before creating Staff Detail or Expense records.
 */
export async function loadCourseRecIdMap(
  fileId: string,
  accessToken: string,
): Promise<Map<string, string>> {
  const response = await axios.get(
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}`,
    {
      params: { alt: "media", supportsAllDrives: "true" },
      headers: { Authorization: `Bearer ${accessToken}` },
      responseType: "stream",
    },
  );

  return new Promise((resolve, reject) => {
    const map = new Map<string, string>();
    let headers: string[] = [];
    let recIdIdx = -1;
    let idIdx = -1;

    parse(response.data as NodeJS.ReadableStream, {
      delimiter: "\t",
      header: false,
      skipEmptyLines: false,
      quoteChar: "\x00",
      step: (result: ParseResult<string[]>) => {
        const row = (result.data as unknown as string[]).map((c) =>
          c.replace(/\r/g, ""),
        );
        if (headers.length === 0) {
          if (row.every((c) => c.trim() === "")) return;
          headers = row.map((h) => h.trim());
          recIdIdx = headers.indexOf("RecID");
          idIdx = headers.indexOf("ID");
          return;
        }
        if (recIdIdx === -1 || idIdx === -1) return;
        const recId = (row[recIdIdx] ?? "").trim();
        const id = (row[idIdx] ?? "").trim();
        if (recId && id) map.set(recId, id);
      },
      complete: () => resolve(map),
      error: (err: Error) => reject(err),
    });
  });
}

// ── Google Drive upload helper ─────────────────────────────────────────────────

/**
 * Uploads a text file to a Google Drive folder via the multipart upload API.
 * Returns the created file's Drive ID.
 */
export async function uploadFileToDrive(
  folderId: string,
  fileName: string,
  content: string,
  mimeType: string,
  accessToken: string,
): Promise<string> {
  const metadata = JSON.stringify({ name: fileName, parents: [folderId] });
  const boundary = "migration_boundary_abc123";
  const body =
    `--${boundary}\r\n` +
    `Content-Type: application/json; charset=UTF-8\r\n\r\n` +
    `${metadata}\r\n` +
    `--${boundary}\r\n` +
    `Content-Type: ${mimeType}\r\n\r\n` +
    `${content}\r\n` +
    `--${boundary}--`;

  const { data } = await axios.post(
    "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true",
    body,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": `multipart/related; boundary=${boundary}`,
      },
    },
  );
  return data.id as string;
}

// ── Bulk API types ─────────────────────────────────────────────────────────────

export interface BulkJobResult {
  jobId: string;
  state: string;
  numberRecordsProcessed: number;
  numberRecordsFailed: number;
  /** External IDs of records that Salesforce rejected (parsed from failedResults CSV). */
  failedExternalIds: Set<string>;
  /** Raw CSV text returned by the /failedResults endpoint (empty string if none). */
  failedCsv: string;
  /** Raw CSV text returned by the /successfulResults endpoint (empty string if none). */
  successfulCsv: string;
}

// ── Salesforce Bulk API 2.0 helpers ───────────────────────────────────────────

export async function createBulkJob(
  instanceUrl: string,
  accessToken: string,
  objectName: string,
  externalIdFieldName: string,
): Promise<string> {
  const { data } = await axios.post(
    `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest`,
    {
      object: objectName,
      operation: "upsert",
      externalIdFieldName,
      contentType: "CSV",
      lineEnding: "LF",
    },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    },
  );
  return data.id as string;
}

export async function uploadCsv(
  instanceUrl: string,
  accessToken: string,
  jobId: string,
  records: Record<string, unknown>[],
): Promise<void> {
  const allColumns = [...new Set(records.flatMap((r) => Object.keys(r)))];
  const csv = Papa.unparse(records, { newline: "\n", columns: allColumns });
  await axios.put(
    `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest/${jobId}/batches`,
    csv,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "text/csv",
      },
    },
  );
}

export async function closeJob(
  instanceUrl: string,
  accessToken: string,
  jobId: string,
): Promise<void> {
  await axios.patch(
    `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest/${jobId}`,
    { state: "UploadComplete" },
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
    },
  );
}

export interface BulkJobPollResult extends BulkJobResult {
  errorMessage?: string;
}

/** Recursively polls the job every POLL_INTERVAL_MS until a terminal state. */
export async function pollJobRecursively(
  instanceUrl: string,
  accessToken: string,
  jobId: string,
): Promise<BulkJobPollResult> {
  const { data } = await axios.get(
    `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest/${jobId}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );

  const state = data.state as string;
  if (state === "JobComplete" || state === "Failed" || state === "Aborted") {
    return {
      jobId,
      state,
      numberRecordsProcessed: (data.numberRecordsProcessed as number) ?? 0,
      numberRecordsFailed: (data.numberRecordsFailed as number) ?? 0,
      errorMessage: (data.errorMessage as string | undefined) ?? undefined,
      failedExternalIds: new Set<string>(),
      failedCsv: "",
      successfulCsv: "",
    };
  }

  await new Promise<void>((r) => setTimeout(r, POLL_INTERVAL_MS));
  return pollJobRecursively(instanceUrl, accessToken, jobId);
}

/** Runs the full Bulk API 2.0 lifecycle for a batch of records and logs results. */
export async function runBulkJob(
  instanceUrl: string,
  accessToken: string,
  objectName: string,
  externalIdFieldName: string,
  records: Record<string, unknown>[],
  logger: {
    info: (m: string) => void;
    warn: (m: string) => void;
    error: (m: string) => void;
  },
  logPrefix: string,
): Promise<BulkJobResult> {
  logger.info(
    `${logPrefix} Creating Bulk API 2.0 upsert job for ${objectName}…`,
  );
  let jobId: string;
  try {
    jobId = await createBulkJob(
      instanceUrl,
      accessToken,
      objectName,
      externalIdFieldName,
    );
    logger.info(`${logPrefix} Job created: ${jobId}`);
  } catch (err: unknown) {
    const e = err as { response?: { status?: number; data?: unknown } };
    logger.error(
      `${logPrefix} Failed to create bulk job.\n` +
        `  Status : ${e.response?.status ?? "unknown"}\n` +
        `  Details: ${JSON.stringify(e.response?.data, null, 2)}`,
    );
    throw err;
  }

  logger.info(`${logPrefix} Uploading ${records.length} records as CSV…`);
  try {
    await uploadCsv(instanceUrl, accessToken, jobId, records);
    logger.info(`${logPrefix} CSV upload accepted.`);
  } catch (err: unknown) {
    const e = err as { response?: { status?: number; data?: unknown } };
    logger.error(
      `${logPrefix} Failed to upload CSV to job ${jobId}.\n` +
        `  Status : ${e.response?.status ?? "unknown"}\n` +
        `  Details: ${JSON.stringify(e.response?.data, null, 2)}`,
    );
    throw err;
  }

  logger.info(`${logPrefix} Closing job (UploadComplete)…`);
  try {
    await closeJob(instanceUrl, accessToken, jobId);
  } catch (err: unknown) {
    const e = err as { response?: { status?: number; data?: unknown } };
    logger.error(
      `${logPrefix} Failed to close job ${jobId}.\n` +
        `  Status : ${e.response?.status ?? "unknown"}\n` +
        `  Details: ${JSON.stringify(e.response?.data, null, 2)}`,
    );
    throw err;
  }

  logger.info(`${logPrefix} Polling job ${jobId} for completion…`);
  let result: BulkJobResult;
  try {
    result = await pollJobRecursively(instanceUrl, accessToken, jobId);
  } catch (err: unknown) {
    const e = err as { response?: { status?: number; data?: unknown } };
    logger.error(
      `${logPrefix} Error while polling job ${jobId}.\n` +
        `  Status : ${e.response?.status ?? "unknown"}\n` +
        `  Details: ${JSON.stringify(e.response?.data, null, 2)}`,
    );
    throw err;
  }

  logger.info(
    `${logPrefix} Job ${result.jobId} finished — ` +
      `state=${result.state}, ` +
      `processed=${result.numberRecordsProcessed}, ` +
      `failed=${result.numberRecordsFailed}`,
  );

  if (result.state === "Failed") {
    // Fetch the job record again to get the errorMessage field
    try {
      const { data: jobInfo } = await axios.get(
        `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest/${jobId}`,
        { headers: { Authorization: `Bearer ${accessToken}` } },
      );
      logger.error(
        `${logPrefix} Job-level failure — errorMessage: ${(jobInfo.errorMessage as string) ?? "(none)"}` +
          `\n  Full job response: ${JSON.stringify(jobInfo, null, 2)}`,
      );
    } catch (_e) {
      logger.error(
        `${logPrefix} Job-level failure — could not fetch error details.`,
      );
    }
  }

  // Always fetch both result endpoints so callers can build reports
  // regardless of how many records succeeded or failed.
  try {
    const { data: successCsvRaw } = await axios.get<string>(
      `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest/${jobId}/successfulResults`,
      {
        headers: { Authorization: `Bearer ${accessToken}` },
        responseType: "text",
      },
    );
    result.successfulCsv = successCsvRaw;
  } catch (err: unknown) {
    const e = err as { response?: { status?: number; data?: unknown } };
    logger.warn(
      `${logPrefix} Could not retrieve successful results.\n` +
        `  Status : ${e.response?.status ?? "unknown"}\n` +
        `  Details: ${JSON.stringify(e.response?.data, null, 2)}`,
    );
  }

  if (result.numberRecordsFailed > 0) {
    logger.warn(
      `${logPrefix} ${result.numberRecordsFailed} record(s) failed. Fetching error details…`,
    );
    try {
      const { data: failedCsvRaw } = await axios.get<string>(
        `${instanceUrl}/services/data/${SF_API_VERSION}/jobs/ingest/${jobId}/failedResults`,
        {
          headers: { Authorization: `Bearer ${accessToken}` },
          responseType: "text",
        },
      );
      result.failedCsv = failedCsvRaw;

      const parsed = Papa.parse<Record<string, string>>(failedCsvRaw, {
        header: true,
        skipEmptyLines: true,
      });
      for (const row of parsed.data) {
        const extId = row[externalIdFieldName] ?? "";
        if (extId) result.failedExternalIds.add(extId);
      }

      logger.error(
        `${logPrefix} Failed record details (${result.failedExternalIds.size} IDs parsed):\n${failedCsvRaw}`,
      );
    } catch (err: unknown) {
      const e = err as { response?: { status?: number; data?: unknown } };
      logger.error(
        `${logPrefix} Could not retrieve failed results.\n` +
          `  Status : ${e.response?.status ?? "unknown"}\n` +
          `  Details: ${JSON.stringify(e.response?.data, null, 2)}`,
      );
    }
  }

  return result;
}
