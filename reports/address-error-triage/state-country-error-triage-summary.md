# Student Account State/Country Error Triage

Generated from:
- /Users/jax/Downloads/Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors.csv
- /Users/jax/Downloads/Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors (2).csv
- /Users/jax/Downloads/Address.settings-meta.xml

## Scope

The two CSVs contain 14592 total failed Account rows. This report classifies only Mailing State/Country picklist failures; non-address failures remain in the annotated CSVs as 'Not State/Country Error'.

## Address Error Summary

| Category | Records | State | Country |
|---|---:|---:|---:|
| Fixable by Code Update | 3260 | 2447 | 813 |
| Fixable by Salesforce Config | 95 | 25 | 70 |
| Needs Client Clarification | 6735 | 6522 | 213 |
| **Total Address Errors** | **10090** | **8994** | **1096** |

## By File

| File | Total Failed Rows | Address Errors | State Errors | Country Errors | Code | Salesforce Config | Client Clarification |
|---|---:|---:|---:|---:|---:|---:|---:|
| Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors.csv | 5814 | 3082 | 2790 | 292 | 1003 | 29 | 2050 |
| Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors (2).csv | 8778 | 7008 | 6204 | 804 | 2257 | 66 | 4685 |

## Triage Rules

- Fixable by Code Update: the row can be deterministically normalized by script to Salesforce country/state code fields.
- Fixable by Salesforce Config: the value is missing or inactive in Salesforce Address.settings metadata and requires Salesforce admin configuration or an approved org-level mapping.
- Needs Client Clarification: the row has conflicting country/state data, a city/county/country in the wrong field, or a value that requires a client decision before code or config should change it.
- Not State/Country Error: the row failed for a non-address reason and is outside this state/country triage.

## Output CSV Columns Added

- `State_Country_Triage_Category`
- `State_Country_Triage_Detail`
- `Suggested_PersonMailingCountryCode`
- `Suggested_PersonMailingStateCode`

## Common Code-Fix Details

| Value | Count |
|---|---:|
| Script can normalize state 'SP' with country 'Brazil' to PersonMailingCountryCode 'BR' and PersonMailingStateCode 'SP'. | 456 |
| Script can normalize state 'ON' with country 'Canada' to PersonMailingCountryCode 'CA' and PersonMailingStateCode 'ON'. | 279 |
| Script can normalize country value 'Russia' to PersonMailingCountryCode 'RU'. | 245 |
| State is blank/NA. Script should omit/null PersonMailingStateCode instead of sending a literal placeholder. | 203 |
| Script can normalize state 'BC' with country 'Canada' to PersonMailingCountryCode 'CA' and PersonMailingStateCode 'BC'. | 201 |
| Script can normalize state 'SAO PAULO' with country 'Brazil' to PersonMailingCountryCode 'BR' and PersonMailingStateCode 'SP'. | 188 |
| Script can normalize country value 'Korea, Republic Of (South)' to PersonMailingCountryCode 'KR'. | 180 |
| Script can normalize country value 'Turkey' to PersonMailingCountryCode 'TR'. | 136 |
| Script can normalize country value 'Viet Nam' to PersonMailingCountryCode 'VN'. | 123 |
| Script can normalize state 'NSW' with country 'Australia' to PersonMailingCountryCode 'AU' and PersonMailingStateCode 'NSW'. | 115 |
| Script can normalize state 'RJ' with country 'Brazil' to PersonMailingCountryCode 'BR' and PersonMailingStateCode 'RJ'. | 97 |
| Script can normalize state 'CA - CALIFORNIA' with country 'United States' to PersonMailingCountryCode 'US' and PersonMailingStateCode 'CA'. | 72 |
| Script can normalize state 'QC' with country 'Canada' to PersonMailingCountryCode 'CA' and PersonMailingStateCode 'QC'. | 56 |
| Script can normalize state 'VIC' with country 'Australia' to PersonMailingCountryCode 'AU' and PersonMailingStateCode 'VIC'. | 56 |
| Script can normalize state 'AB' with country 'Canada' to PersonMailingCountryCode 'CA' and PersonMailingStateCode 'AB'. | 53 |

## Top Code-Fix Values

| Category | Field | State Value | Country Value | Suggested CountryCode | Suggested StateCode | Count |
|---|---|---|---|---|---|---:|
| Fixable by Code Update | PersonMailingState | SP | Brazil | BR | SP | 456 |
| Fixable by Code Update | PersonMailingState | ON | Canada | CA | ON | 279 |
| Fixable by Code Update | PersonMailingState | BC | Canada | CA | BC | 201 |
| Fixable by Code Update | PersonMailingState | SAO PAULO | Brazil | BR | SP | 188 |
| Fixable by Code Update | PersonMailingState | NSW | Australia | AU | NSW | 115 |
| Fixable by Code Update | PersonMailingCountry | MOSCOW | Russia | RU |  | 106 |
| Fixable by Code Update | PersonMailingState | RJ | Brazil | BR | RJ | 97 |
| Fixable by Code Update | PersonMailingState | CA - CALIFORNIA | United States | US | CA | 72 |
| Fixable by Code Update | PersonMailingCountry | SEOUL | Korea, Republic Of (South) | KR |  | 62 |
| Fixable by Code Update | PersonMailingState | QC | Canada | CA | QC | 56 |
| Fixable by Code Update | PersonMailingState | VIC | Australia | AU | VIC | 56 |
| Fixable by Code Update | PersonMailingState | AB | Canada | CA | AB | 53 |
| Fixable by Code Update | PersonMailingState | MEXICO CITY | Mexico | MX | DF | 44 |
| Fixable by Code Update | PersonMailingState | CDMX | Mexico | MX | DF | 40 |
| Fixable by Code Update | PersonMailingState | MG | Brazil | BR | MG | 39 |
| Fixable by Code Update | PersonMailingState | DF | Brazil | BR | DF | 37 |
| Fixable by Code Update | PersonMailingState | RS | Brazil | BR | RS | 32 |
| Fixable by Code Update | PersonMailingCountry | California | Korea, Republic Of (South) | KR |  | 31 |
| Fixable by Code Update | PersonMailingCountry | ISTANBUL | Turkey | TR |  | 30 |
| Fixable by Code Update | PersonMailingState | TAMILNADU | India | IN | TN | 29 |
| Fixable by Code Update | PersonMailingCountry | California | Viet Nam | VN |  | 26 |
| Fixable by Code Update | PersonMailingState | CALIFORNIA (CA) | United States | US | CA | 24 |
| Fixable by Code Update | PersonMailingCountry | California | Russia | RU |  | 22 |
| Fixable by Code Update | PersonMailingState | NEW DELHI | India | IN | DL | 22 |
| Fixable by Code Update | PersonMailingState | NUEVO LEON | Mexico | MX | NL | 20 |
| Fixable by Code Update | PersonMailingState | UP | India | IN | UP | 20 |
| Fixable by Code Update | PersonMailingState | MH | India | IN | MH | 19 |
| Fixable by Code Update | PersonMailingState | PARANA | Brazil | BR | PR | 19 |
| Fixable by Code Update | PersonMailingState | CIUDAD DE MEXICO | Mexico | MX | DF | 18 |
| Fixable by Code Update | PersonMailingState | NL | Mexico | MX | NL | 18 |

## Common Salesforce-Config Details

| Value | Count |
|---|---:|
| Country/territory is not active as a country in the provided Salesforce Address.settings metadata. Salesforce admin must add/activate the country or decide an allowed replacement mapping. | 70 |
| State/territory exists in Salesforce Address.settings for this country but is inactive. Salesforce admin must activate it or approve a replacement mapping. | 25 |

## Top Salesforce-Config Values

| Category | Field | State Value | Country Value | Suggested CountryCode | Suggested StateCode | Count |
|---|---|---|---|---|---|---:|
| Fixable by Salesforce Config | PersonMailingState | Puerto Rico | United States | US |  | 19 |
| Fixable by Salesforce Config | PersonMailingCountry | TEHRAN | Iran |  |  | 17 |
| Fixable by Salesforce Config | PersonMailingCountry | California | Iran |  |  | 13 |
| Fixable by Salesforce Config | PersonMailingCountry | IRAN | Iran |  |  | 5 |
| Fixable by Salesforce Config | PersonMailingState | PUERTO RICO | United States | US |  | 4 |
| Fixable by Salesforce Config | PersonMailingCountry | KHARTOUM | Sudan |  |  | 3 |
| Fixable by Salesforce Config | PersonMailingCountry | U.S. Virgin Islands | Virgin Islands, U.S. |  |  | 3 |
| Fixable by Salesforce Config | PersonMailingCountry | 0 | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | AL-REMAL | Palestinian Territories |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | ALEPPO | Syria |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | ASH | iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | California | Korea, Democratic People's Rep |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | California | Palestinian Territories |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | California | Puerto Rico |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | California | Syria |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | EAST AZARBAIJAN | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | FARDIS | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | FARS | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry |  | Guam |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | Guam | Guam |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | HAMEDAN_HAMEDAN_HAME | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry |  | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | IR | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | KERMAN | iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | KH | IRAN |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | KHOUZESTAN | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | MOSLEM | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | OTHER | Iran |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | Puerto Rico | Puerto Rico |  |  | 1 |
| Fixable by Salesforce Config | PersonMailingCountry | PUERTO RICO | Puerto Rico |  |  | 1 |

## Common Client-Clarification Details

| Value | Count |
|---|---:|
| State cannot be deterministically mapped for the current row. It may conflict with the country, be a city/county/country value in the state field, or require Salesforce metadata expansion. Client/source-data owner must confirm the intended address. | 6522 |
| Client decision needed. Salesforce metadata has Hong Kong as a China subdivision (CountryCode CN, StateCode 91), not as a standalone country. Confirm whether to map to CN/91 or change Salesforce country config. | 190 |
| Country value is not a country or contains mixed location data. Client/source-data owner must confirm the correct country and whether the value belongs in city, state, county, or another address field. | 18 |
| Country value is not recognized by the current normalization rules or Salesforce metadata. Client/source-data owner must confirm the intended country. | 5 |

## Top Client-Clarification Values

| Category | Field | State Value | Country Value | Suggested CountryCode | Suggested StateCode | Count |
|---|---|---|---|---|---|---:|
| Needs Client Clarification | PersonMailingState | California | China | CN |  | 198 |
| Needs Client Clarification | PersonMailingState | SINGAPORE | Singapore | SG |  | 184 |
| Needs Client Clarification | PersonMailingState | LONDON | United Kingdom | GB |  | 181 |
| Needs Client Clarification | PersonMailingState | DUBAI | United Arab Emirates | AE |  | 117 |
| Needs Client Clarification | PersonMailingState | California | Canada | CA |  | 115 |
| Needs Client Clarification | PersonMailingCountry | HONG KONG | Hong Kong | CN | 91 | 85 |
| Needs Client Clarification | PersonMailingState | LIMA | Peru | PE |  | 69 |
| Needs Client Clarification | PersonMailingState | BERLIN | Germany | DE |  | 68 |
| Needs Client Clarification | PersonMailingState | PUNJAB | Pakistan | PK |  | 65 |
| Needs Client Clarification | PersonMailingState | California | India | IN |  | 63 |
| Needs Client Clarification | PersonMailingState | MADRID | Spain | ES |  | 62 |
| Needs Client Clarification | PersonMailingState | AP |  |  |  | 51 |
| Needs Client Clarification | PersonMailingState | NRW | Germany | DE |  | 46 |
| Needs Client Clarification | PersonMailingState | Puerto Rico | Brazil | BR |  | 46 |
| Needs Client Clarification | PersonMailingState | ZURICH | Switzerland | CH |  | 44 |
| Needs Client Clarification | PersonMailingState | BOGOTA | Colombia | CO |  | 42 |
| Needs Client Clarification | PersonMailingState | BARCELONA | Spain | ES |  | 40 |
| Needs Client Clarification | PersonMailingState | CUNDINAMARCA | Colombia | CO |  | 38 |
| Needs Client Clarification | PersonMailingState | South Carolina | Brazil | BR |  | 37 |
| Needs Client Clarification | PersonMailingState | BUENOS AIRES | Argentina | AR |  | 34 |
| Needs Client Clarification | PersonMailingState | KYIV | Ukraine | UA |  | 34 |
| Needs Client Clarification | PersonMailingState | California | Egypt | EG |  | 33 |
| Needs Client Clarification | PersonMailingState | BANGKOK | Thailand | TH |  | 32 |
| Needs Client Clarification | PersonMailingState | LAGOS | Nigeria | NG |  | 32 |
| Needs Client Clarification | PersonMailingState | METRO MANILA | Philippines | PH |  | 32 |
| Needs Client Clarification | PersonMailingState | RIYADH | Saudi Arabia | SA |  | 32 |
| Needs Client Clarification | PersonMailingState | California | United Kingdom | GB |  | 30 |
| Needs Client Clarification | PersonMailingState | PARIS | France | FR |  | 30 |
| Needs Client Clarification | PersonMailingState | AUCKLAND | New Zealand | NZ |  | 28 |
| Needs Client Clarification | PersonMailingState | FRANCE | France | FR |  | 28 |

## Generated Files

- /Users/jax/Connect Projects/stanford-csp-migration/reports/address-error-triage/Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors - Annotated.csv
- /Users/jax/Connect Projects/stanford-csp-migration/reports/address-error-triage/Stanford CSP - Student Import - Account - 2026-05-19 13_01 - Errors (2) - Annotated.csv
- /Users/jax/Connect Projects/stanford-csp-migration/reports/address-error-triage/state-country-error-triage-aggregation.csv
