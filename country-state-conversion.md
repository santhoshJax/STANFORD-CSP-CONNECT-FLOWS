# Country & State Conversion — Stanford CSP Migration

Describes how raw TSV values for `Country` and `State_Prov` are normalized before
being sent to Salesforce as `PersonMailingCountry` and `PersonMailingState`.

Salesforce is configured with **text fields** (not State/Country picklists), so values
must arrive as clean full names — e.g. `"United States"`, `"California"`.

---

## How it works in code

```ts
// Country — always normalized
record.PersonMailingCountry = normalizeCountryName(raw.Country);

// State — normalized for US only; non-US passed through as-is
const isUS = record.PersonMailingCountry === "United States";
record.PersonMailingState = isUS
  ? normalizeStateName(raw.State_Prov)
  : str(raw.State_Prov);
```

---

## Country Conversion — `normalizeCountryName()`

Lookup order:
1. `COUNTRY_CODE_ALIASES` — typos, abbreviations, alternate spellings
2. `COUNTRY_NAME_TO_CODE` — exact 2-letter ISO codes and exact full names
3. Unrecognized → raw value passed through unchanged

---

### United States

| Source raw value | `PersonMailingCountry` |
|---|---|
| `US` | `United States` |
| `United States` | `United States` |
| `USA` | `United States` |
| `U.S.A.` | `United States` |
| `U.S.` | `United States` |
| `U S A` | `United States` |
| `US OF A` | `United States` |
| `USA N` | `United States` |
| `COUNTRY US` | `United States` |
| `UNITED STATES` | `United States` |
| `UNITED STATES OF AMERICA` | `United States` |
| `THE UNITED STATES OF AMERICA` | `United States` |
| `ESTADOS UNIDOS` | `United States` |
| `UNITES STATES` (typo) | `United States` |
| `UNITED STATED` (typo) | `United States` |
| `UNITED STATE` (typo) | `United States` |
| `UINITED STATES` (typo) | `United States` |
| `UNITIED STATES` (typo) | `United States` |
| `UNITESD STATES` (typo) | `United States` |
| `UNITED STA` (truncated) | `United States` |
| `UNITED STA OF AM` (truncated) | `United States` |
| `U S A REALLY YOU NEED THIS` | `United States` |

---

### United Kingdom

| Source raw value | `PersonMailingCountry` |
|---|---|
| `GB` | `United Kingdom` |
| `United Kingdom` | `United Kingdom` |
| `UK` | `United Kingdom` |
| `U.K.` | `United Kingdom` |
| `U K` | `United Kingdom` |
| `UNITED KINGDON` (typo) | `United Kingdom` |
| `ENGLAND UK` | `United Kingdom` |

---

### Canada

| Source raw value | `PersonMailingCountry` |
|---|---|
| `CA` | `Canada` |
| `Canada` | `Canada` |
| `CANANDA` (typo) | `Canada` |

---

### Australia

| Source raw value | `PersonMailingCountry` |
|---|---|
| `AU` | `Australia` |
| `Australia` | `Australia` |
| `ASTRALIA` (typo) | `Australia` |

---

### India

| Source raw value | `PersonMailingCountry` |
|---|---|
| `IN` | `India` |
| `India` | `India` |
| `IND` | `India` |

---

### China

| Source raw value | `PersonMailingCountry` |
|---|---|
| `CN` | `China` |
| `China` | `China` |
| `PEOPLE S REPUBLIC OF CHINA` | `China` |

---

### South Korea

| Source raw value | `PersonMailingCountry` |
|---|---|
| `KR` | `South Korea` |
| `South Korea` | `South Korea` |
| `SOUTH KOREA` | `South Korea` |
| `KOREA SOUTH` | `South Korea` |
| `KOREA` | `South Korea` |
| `KOREA REPUBLIC OF` | `South Korea` |
| `KOREA REPUBLIC OF SOUTH` | `South Korea` |
| `REPUBLIC OF KOREA` | `South Korea` |

---

### Russia

| Source raw value | `PersonMailingCountry` |
|---|---|
| `RU` | `Russia` |
| `Russia` | `Russia` |
| `RUSSIA` | `Russia` |
| `RUSSIAN FEDERATION` | `Russia` |

---

### Brazil

| Source raw value | `PersonMailingCountry` |
|---|---|
| `BR` | `Brazil` |
| `Brazil` | `Brazil` |
| `BRASIL` (typo) | `Brazil` |

---

### Netherlands

| Source raw value | `PersonMailingCountry` |
|---|---|
| `NL` | `Netherlands` |
| `Netherlands` | `Netherlands` |
| `THE NETHERLANDS` | `Netherlands` |
| `NETHERLAND` | `Netherlands` |

---

### Taiwan

| Source raw value | `PersonMailingCountry` |
|---|---|
| `TW` | `Taiwan` |
| `Taiwan` | `Taiwan` |
| `TAIWAN R O C` | `Taiwan` |

---

### Hong Kong

| Source raw value | `PersonMailingCountry` |
|---|---|
| `HK` | `Hong Kong` |
| `Hong Kong` | `Hong Kong` |
| `HONG KONG` | `Hong Kong` |

---

### United Arab Emirates

| Source raw value | `PersonMailingCountry` |
|---|---|
| `AE` | `United Arab Emirates` |
| `United Arab Emirates` | `United Arab Emirates` |
| `UAE` | `United Arab Emirates` |

---

### Colombia

| Source raw value | `PersonMailingCountry` |
|---|---|
| `CO` | `Colombia` |
| `Colombia` | `Colombia` |
| `COLOMBIA COLOMBIA` | `Colombia` |

---

### All other recognized countries (2-letter code or exact full name)

| Source raw value | `PersonMailingCountry` |
|---|---|
| `MX` · `Mexico` | `Mexico` |
| `NZ` · `New Zealand` | `New Zealand` |
| `JP` · `Japan` | `Japan` |
| `DE` · `Germany` | `Germany` |
| `FR` · `France` | `France` |
| `IT` · `Italy` | `Italy` |
| `ES` · `Spain` | `Spain` |
| `PT` · `Portugal` | `Portugal` |
| `BE` · `Belgium` | `Belgium` |
| `CH` · `Switzerland` | `Switzerland` |
| `AT` · `Austria` | `Austria` |
| `SE` · `Sweden` | `Sweden` |
| `NO` · `Norway` | `Norway` |
| `DK` · `Denmark` | `Denmark` |
| `FI` · `Finland` | `Finland` |
| `IE` · `Ireland` | `Ireland` |
| `PL` · `Poland` | `Poland` |
| `AR` · `Argentina` | `Argentina` |
| `CL` · `Chile` | `Chile` |
| `PE` · `Peru` | `Peru` |
| `VE` · `Venezuela` | `Venezuela` |
| `ZA` · `South Africa` | `South Africa` |
| `NG` · `Nigeria` | `Nigeria` |
| `KE` · `Kenya` | `Kenya` |
| `EG` · `Egypt` | `Egypt` |
| `IL` · `Israel` | `Israel` |
| `SA` · `Saudi Arabia` | `Saudi Arabia` |
| `SG` · `Singapore` | `Singapore` |
| `TH` · `Thailand` | `Thailand` |
| `PH` · `Philippines` | `Philippines` |
| `MY` · `Malaysia` | `Malaysia` |
| `ID` · `Indonesia` | `Indonesia` |
| `PK` · `Pakistan` | `Pakistan` |
| `BD` · `Bangladesh` | `Bangladesh` |
| `LK` · `Sri Lanka` | `Sri Lanka` |
| `NP` · `Nepal` | `Nepal` |

---

### Aliases with no full name in COUNTRIES table — passed through unchanged

| Source raw value | `PersonMailingCountry` |
|---|---|
| `TURKEY` · `TURKIYE` | raw value (e.g. `Turkey`) |
| `VIET NAM` · `VIETNAM` | raw value |
| `CZECH REPUBLIC` · `CZECHIA` | raw value |
| `BOLIVIA` | raw value |
| `TANZANIA` · `TANZANIA UNITED REPUBLIC OF` | raw value |
| `IRAN` | raw value |
| `SUDAN` | raw value |
| `ROMANI` | raw value |
| `LIBANON` | raw value |
| `SULTANATE OF OMAN` | raw value |
| `REPUBLIC OF MOLDOVA` | raw value |
| `CURACAO` · `MACAU` · `LAOS` · `COTE DIVOIRE` | raw value |

> Any country not matched at all → raw value passed through unchanged

---

## State Conversion — `normalizeStateName()` — US records only

State normalization only runs when `PersonMailingCountry === "United States"`.
All other country records skip this entirely — `str()` only (whitespace trim).

---

### Via exact 2-letter code

| Source | `PersonMailingState` |
|---|---|
| `AL` | `Alabama` |
| `AK` | `Alaska` |
| `AZ` | `Arizona` |
| `AR` | `Arkansas` |
| `CA` | `California` |
| `CO` | `Colorado` |
| `CT` | `Connecticut` |
| `DE` | `Delaware` |
| `FL` | `Florida` |
| `GA` | `Georgia` |
| `HI` | `Hawaii` |
| `ID` | `Idaho` |
| `IL` | `Illinois` |
| `IN` | `Indiana` |
| `IA` | `Iowa` |
| `KS` | `Kansas` |
| `KY` | `Kentucky` |
| `LA` | `Louisiana` |
| `ME` | `Maine` |
| `MD` | `Maryland` |
| `MA` | `Massachusetts` |
| `MI` | `Michigan` |
| `MN` | `Minnesota` |
| `MS` | `Mississippi` |
| `MO` | `Missouri` |
| `MT` | `Montana` |
| `NE` | `Nebraska` |
| `NV` | `Nevada` |
| `NH` | `New Hampshire` |
| `NJ` | `New Jersey` |
| `NM` | `New Mexico` |
| `NY` | `New York` |
| `NC` | `North Carolina` |
| `ND` | `North Dakota` |
| `OH` | `Ohio` |
| `OK` | `Oklahoma` |
| `OR` | `Oregon` |
| `PA` | `Pennsylvania` |
| `RI` | `Rhode Island` |
| `SC` | `South Carolina` |
| `SD` | `South Dakota` |
| `TN` | `Tennessee` |
| `TX` | `Texas` |
| `UT` | `Utah` |
| `VT` | `Vermont` |
| `VA` | `Virginia` |
| `WA` | `Washington` |
| `WV` | `West Virginia` |
| `WI` | `Wisconsin` |
| `WY` | `Wyoming` |
| `DC` | `District of Columbia` |
| `PR` | `Puerto Rico` |
| `GU` | `Guam` |
| `VI` | `U.S. Virgin Islands` |
| `AS` | `American Samoa` |
| `MP` | `Northern Mariana Islands` |

---

### Via extra aliases — abbreviations, typos, city names, formatted codes

| Source raw value | `PersonMailingState` |
|---|---|
| `FLA` · `FLA.` | `Florida` |
| `N.Y.` | `New York` |
| `NYC` · `NEW YORK CITY` | `New York` |
| `MASS` | `Massachusetts` |
| `ORE` | `Oregon` |
| `ILL` · `ILL.` | `Illinois` |
| `CAL` | `California` |
| `CALIFONIA` (typo) | `California` |
| `CALIFORONIA` (typo) | `California` |
| `US-0-CA` | `California` |
| `US-0-PA` | `Pennsylvania` |
| `US-0-OH` | `Ohio` |
| `NEWJERSEY` (no space) | `New Jersey` |
| `NEW JERESEY` (typo) | `New Jersey` |
| `W I` (space) | `Wisconsin` |

---

### Via leading dash pattern — `"XX - FULLNAME"` or `"XX-FULLNAME"`

| Source raw value | `PersonMailingState` |
|---|---|
| `CA - CALIFORNIA` · `CA-CALIFORNIA` · `CA- CALIFORNIA` | `California` |
| `TX - TEXAS` · `TX-TEXAS` · `TX- TEXAS` | `Texas` |
| `WA - WASHINGTON` | `Washington` |
| `NY - NEW YORK` | `New York` |
| `VA - VIRGINIA` | `Virginia` |
| `TN - TENNESSEE` | `Tennessee` |
| `GA - GEORGIA` | `Georgia` |
| `WI - WISCONSIN` | `Wisconsin` |
| `IN - INDIANA` | `Indiana` |
| `NH - NEW HAMPSHIRE` | `New Hampshire` |
| `PA - PENNSYLVANIA` | `Pennsylvania` |
| `AZ - ARIZONA` | `Arizona` |
| `MN - MINNESOTA` | `Minnesota` |
| `MT - MONTANA` | `Montana` |
| `OR - OREGON` | `Oregon` |
| `DC - DISTRICT OF COL` | `District of Columbia` |
| `MD - MARYLAND` · `MD-MARYLAND` | `Maryland` |
| `MA - MASSACHUSETTS` | `Massachusetts` |
| `MI-MICHIGAN` | `Michigan` |
| `AR-ARKANSAS` | `Arkansas` |

---

### Via bracketed code — `"FULLNAME (XX)"` or `"FULLNAME [XX]"`

| Source raw value | `PersonMailingState` |
|---|---|
| `California (CA)` · `California [CA]` · `CALIFORNIA(CA)` | `California` |
| `IOWA (IA)` | `Iowa` |
| `OHIO (OH)` | `Ohio` |
| `OKLAHOMA (OK)` | `Oklahoma` |
| `VIRGINIA (VA)` | `Virginia` |
| `WASHINGTON (WA)` | `Washington` |
| `ILLINOIS (IL)` | `Illinois` |
| `NEW YORK (NY)` | `New York` |
| `TEXAS (TX)` | `Texas` |
| `MASSACHUSETTS (MA)` | `Massachusetts` |

---

### Via hardcoded values

| Source raw value | `PersonMailingState` |
|---|---|
| `D.C.` · `D.C` | `District of Columbia` |
| `CALIF.` · `CALIF` | `California` |

---

### Via first standalone 2-letter word

| Source raw value | `PersonMailingState` |
|---|---|
| `VA.` | `Virginia` |
| `PA.` | `Pennsylvania` |
| `MD.` | `Maryland` |
| `IL ILLINOIS` | `Illinois` |
| `ID IDAHO` | `Idaho` |
| `MD MARYLAND` | `Maryland` |
| `MT MONTANA` | `Montana` |
| `CA, US` | `California` |
| `CA\|CALIFORNIA` | `California` |
| `WASHINGTON DC` | `District of Columbia` |

---

### Non-US states — always passed through unchanged

| Example source value | `PersonMailingState` |
|---|---|
| `Ontario` | `Ontario` |
| `British Columbia` · `BC` | `BC` |
| `New South Wales` | `New South Wales` |
| `Karnataka` | `Karnataka` |
| any blank value | `` (empty) |

---

## Source files

| File | Purpose |
|---|---|
| `src/studentFlow.ts` | Calls `normalizeCountryName` and `normalizeStateName` inside `mapToAccount()` |
| `src/utils.ts` | All conversion functions and lookup tables |
