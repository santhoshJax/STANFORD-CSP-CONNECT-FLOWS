// ── Date ──────────────────────────────────────────────────────────────────────

export function parseCourseDate(v: string | undefined | null): string | null {
  const s = (v ?? "").trim();
  if (!s || s === "00/00/0000" || s === "00/00/00") return null;
  const parts = s.split("/");
  if (parts.length !== 3) return null;
  const [m, d, y] = parts;
  const year = y.length === 2 ? "20" + y : y;
  const iso = `${year}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
  if (isNaN(Date.parse(iso))) return null;
  return iso + "T00:00:00.000Z";
}

// ── Duration ─────────────────────────────────────────────────────────────────

export function parseDuration(v: string | undefined | null): number | null {
  if (!v?.trim()) return null;
  const s = v.trim();
  const weekMatch = s.match(/(\d+\.?\d*)\s*week/i);
  if (weekMatch) return parseFloat(weekMatch[1]);
  if (/1\s*day/i.test(s)) return 0.14;
  const dayMatch = s.match(/(\d+)\s*day/i);
  if (dayMatch) return Math.round((parseInt(dayMatch[1], 10) / 7) * 100) / 100;
  return null;
}

// Parses "2 days" / "5 weeks" / "1 day" etc. into split value + unit for
// Duration_Value__c (Number) and Duration_Unit__c (Picklist: Days, Weeks).
export function parseDurationSplit(
  v: string | undefined | null,
): { value: number; unit: "Days" | "Weeks" } | null {
  if (!v?.trim()) return null;
  const s = v.trim();
  const weekMatch = s.match(/(\d+\.?\d*)\s*week/i);
  if (weekMatch) return { value: parseFloat(weekMatch[1]), unit: "Weeks" };
  const dayMatch = s.match(/(\d+\.?\d*)\s*day/i);
  if (dayMatch) return { value: parseFloat(dayMatch[1]), unit: "Days" };
  return null;
}

// ── Enrollment status ─────────────────────────────────────────────────────────

export function normaliseEnrollmentStatus(
  raw: string | undefined | null,
  cancelled: boolean,
): string {
  if (cancelled) return "Cancelled";
  const map: Record<string, string> = {
    open: "Open",
    pending: "Pending",
    closedweb: "ClosedWeb",
    closed: "Closed",
    cancelled: "Cancelled",
    full: "Full",
    true: "Open",
    false: "Closed",
    "1": "Open",
    "0": "Closed",
  };
  return map[(raw ?? "").toLowerCase().trim()] ?? "Closed";
}

// ── Catalog notes ─────────────────────────────────────────────────────────────

/**
 * Clean catalog notes for a Salesforce Rich Text Area field.
 * Replaces _4DNL_ tokens with newlines. HTML tags (<br>, <b>, <i>, etc.)
 * are preserved as-is since the target field is Rich Text Area.
 */
export function normaliseCatalogNotes(v: string | undefined | null): string | null {
  if (!v?.trim()) return null;
  return v.replace(/_4DNL_/g, "\n").trim();
}

// ── Notes merge ───────────────────────────────────────────────────────────────

export function mergeNotes(
  a: string | undefined | null,
  b: string | undefined | null,
): string | null {
  const parts = [a, b]
    .map((s) => s?.trim())
    .filter((s): s is string => Boolean(s));
  return parts.length > 0 ? parts.join(" | ") : null;
}

// ── Boolean ───────────────────────────────────────────────────────────────────

export function parseBool(v: string | undefined | null): boolean | null {
  if (v === null || v === undefined || v === "") return null;
  const s = String(v).trim().toLowerCase();
  if (s === "true" || s === "1" || s === "yes") return true;
  if (s === "false" || s === "0" || s === "no") return false;
  return null;
}

// ── Roster email ──────────────────────────────────────────────────────────────

export function normaliseRosterEmail(v: string | undefined | null): string | null {
  if (!v?.trim()) return null;
  const s = v.trim();
  if (s === "FALSE" || s === "0") return null;
  return s;
}

// ── Closed_DTS ────────────────────────────────────────────────────────────────

export function parseClosedDTS(v: unknown): string | null {
  if (v === null || v === undefined || v === "" || v === "0" || v === 0)
    return null;
  if (typeof v === "number") return Math.round(v).toString();
  const s = String(v).trim();
  if (/e\+/i.test(s)) return Math.round(parseFloat(s)).toString();
  return s === "0" ? null : s;
}

// ── Format ────────────────────────────────────────────────────────────────────

export function normaliseFormat(
  format: string | undefined | null,
  hybrid: string | undefined | null,
): string | null {
  // Hybrid column: boolean true or text "Large Hybrid"/"Small Hybrid" → Flex Hybrid
  const h = (hybrid ?? "").trim().toLowerCase();
  if (parseBool(hybrid) === true || h === "large hybrid" || h === "small hybrid")
    return "Flex Hybrid";
  if (!format?.trim()) return null;
  const map: Record<string, string> = {
    "on campus": "On-campus",
    "on-campus course": "On-campus",
    "on-campus": "On-campus",
    "off-campus": "Off-campus",
    online: "Online",
    "online course": "Online",
    "flex online": "Flex Online",
    "flex hybrid": "Flex Hybrid",
    "live online": "Live Online",
  };
  // Org picklist: On-campus, Online, Flex Online, Flex Hybrid, Live Online,
  // Off-campus, Hybrid. Unmapped/junk source text must not be written raw.
  return map[format.trim().toLowerCase()] ?? null;
}

/**
 * Derive Format__c from the section suffix when the Format column is empty.
 * W = Flex Online, H = On-campus, Z = Live Online
 * A/B/C/D/E and others = parallel sections only, no format implied.
 */
export function formatFromSuffix(suffix: string | null): string | null {
  if (!suffix) return null;
  const s = suffix.toUpperCase();
  if (s === "W") return "Flex Online";
  if (s === "H") return "On-campus";
  if (s === "Z") return "Live Online";
  return null;
}

// ── Weekdays ──────────────────────────────────────────────────────────────────

export function parseWeekdays(s: string | undefined | null): Record<string, boolean> {
  const flags: Record<string, boolean> = {
    IsMonday: false,
    IsTuesday: false,
    IsWednesday: false,
    IsThursday: false,
    IsFriday: false,
    IsSaturday: false,
    IsSunday: false,
  };
  if (!s?.trim()) return flags;
  const parts = s
    .toLowerCase()
    .split(/[,/\s&]+/)
    .map((p) => p.trim())
    .filter(Boolean);
  for (const part of parts) {
    if (part.startsWith("mon")) flags.IsMonday = true;
    if (part.startsWith("tue")) flags.IsTuesday = true;
    if (part.startsWith("wed")) flags.IsWednesday = true;
    if (part.startsWith("thu")) flags.IsThursday = true;
    if (part.startsWith("fri")) flags.IsFriday = true;
    if (part.startsWith("sat")) flags.IsSaturday = true;
    if (part.startsWith("sun")) flags.IsSunday = true;
  }
  return flags;
}

// ── Course time ───────────────────────────────────────────────────────────────

function normalizeTimeStr(t: string): string {
  return t
    .replace(/[–—]/g, "-")                     // em/en dash → hyphen
    .replace(/\b([ap])\.?m\.?\b/gi, "$1m")     // a.m. / p.m. / a.m → am/pm
    .trim();
}

function parseTime12h(t: string): string | null {
  let s = normalizeTimeStr(t).toLowerCase();
  if (s === "noon") return "12:00:00";
  // Expand shorthand "8 am" → "8:00 am"
  s = s.replace(/^(\d{1,2})\s*(am|pm)$/, "$1:00 $2");
  const m = s.match(/^(\d{1,2}):(\d{2})\s*(am|pm)$/);
  if (!m) return null;
  let h = parseInt(m[1], 10);
  const min = m[2];
  const ampm = m[3];
  if (ampm === "pm" && h !== 12) h += 12;
  if (ampm === "am" && h === 12) h = 0;
  return `${String(h).padStart(2, "0")}:${min}:00`;
}

export function parseCourseTime(s: string | undefined | null): {
  startTime: string | null;
  endTime: string | null;
} {
  if (!s?.trim()) return { startTime: null, endTime: null };

  const norm = normalizeTimeStr(s);

  // Split into start/end parts
  let startRaw: string;
  let endRaw: string;

  if (norm.includes(" - ")) {
    const parts = norm.split(" - ");
    startRaw = parts[0].trim();
    endRaw = parts[1]?.trim() ?? "";
  } else {
    // Handle no-space hyphen like "10:15-1:00 pm"
    const m = norm.match(/^(\d{1,2}:\d{2}(?:\s*(?:am|pm))?)\s*-\s*(\d{1,2}:\d{2}.*)$/i);
    if (m) {
      startRaw = m[1].trim();
      endRaw = m[2].trim();
    } else {
      startRaw = norm;
      endRaw = "";
    }
  }

  // If start has no am/pm but end does, infer it
  if (!/\b(am|pm)\b/i.test(startRaw) && endRaw) {
    const endAmPmMatch = endRaw.match(/\b(am|pm)\b/i);
    if (endAmPmMatch) {
      const endAmPm = endAmPmMatch[1];
      const candidate = `${startRaw} ${endAmPm}`;
      const tryStart = parseTime12h(candidate);
      const tryEnd = parseTime12h(endRaw);
      if (tryStart && tryEnd && tryStart <= tryEnd) {
        startRaw = candidate;
      } else {
        // Start is earlier in the day than end — use opposite am/pm
        startRaw = `${startRaw} ${endAmPm.toLowerCase() === "am" ? "pm" : "am"}`;
      }
    }
  }

  return {
    startTime: startRaw ? parseTime12h(startRaw) : null,
    endTime: endRaw ? parseTime12h(endRaw) : null,
  };
}

// ── Numeric helpers ───────────────────────────────────────────────────────────

export function parseIntVal(v: string | undefined | null): number | null {
  if (!v?.trim()) return null;
  const s = v.trim();
  if (s === "0") return null;
  const n = parseInt(s, 10);
  return isNaN(n) ? null : n;
}

export function parseFloatVal(v: string | undefined | null): number | null {
  if (!v?.trim()) return null;
  const s = v.trim();
  if (s === "0" || s === "0.0") return null;
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

// ── LearningCourse dedup ──────────────────────────────────────────────────────

export function stripSectionSuffix(code: string): string {
  return (
    code
      .trim()
      // Space-separated letter suffix: "OWC 303 A" → "OWC 303", "OWC 303 WA" → "OWC 303"
      .replace(/\s+[A-Za-z]+$/, "")
      // Hyphen-digit suffix: "WSP 400-01A" → "WSP 400"
      .replace(/-\d+[A-Z]*$/i, "")
      .trim()
  );
}

/** Returns the trailing section suffix letter(s), e.g. "OWC 303 A" → "A", or null if none. */
export function extractSectionSuffix(code: string): string | null {
  // Space-separated letter suffix
  const m = code.trim().match(/\s+([A-Za-z]+)$/);
  if (m) return m[1].toUpperCase();
  // Hyphen-digit suffix
  const h = code.trim().match(/-(\d+[A-Z]*)$/i);
  if (h) return h[1].toUpperCase();
  return null;
}
