import type { JsonRecord, JsonValue } from "./types.js";

const SENSITIVE_KEY = /(?:^|[_-])(?:authorization|bearer|api[_ -]?key|access[_ -]?key|secret|password|passwd|credential|token|cookie|set-cookie)(?:$|[_-])/i;
const PRIVATE_CONTENT_KEY = /^(?:reasoning(?:_content)?|thinking(?:_content)?|chain[_ -]?of[_ -]?thought)$/i;

export function redactText(value: string, maxLength = 16_384): string {
  let redacted = value;
  redacted = redacted.replace(/(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+/gi, "$1[REDACTED]");
  redacted = redacted.replace(/(bearer\s+)[A-Za-z0-9._~+/=-]+/gi, "$1[REDACTED]");
  redacted = redacted.replace(/\bsk-[A-Za-z0-9][A-Za-z0-9_-]*/g, "sk-[REDACTED]");
  redacted = redacted.replace(
    /(\b(?:api[_ -]?key|access[_ -]?key|secret|password|passwd|credential|token)\b\s*[:=]\s*)(["']?)([^\s,"';}]+)\2/gi,
    "$1$2[REDACTED]$2",
  );
  if (redacted.length <= maxLength) return redacted;
  return `${redacted.slice(0, maxLength)}…[TRUNCATED]`;
}

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function sanitizeJson(value: unknown, maxDepth = 12, depth = 0): JsonValue {
  if (depth > maxDepth) return "[TRUNCATED]";
  if (value === null) return null;
  if (typeof value === "string") return redactText(value);
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeJson(item, maxDepth, depth + 1));
  }
  if (!isRecord(value)) return null;

  const output: { [key: string]: JsonValue } = {};
  for (const [key, item] of Object.entries(value)) {
    if (PRIVATE_CONTENT_KEY.test(key)) continue;
    output[key] = SENSITIVE_KEY.test(key)
      ? "[REDACTED]"
      : sanitizeJson(item, maxDepth, depth + 1);
  }
  return output;
}

export function asJsonRecord(value: unknown): JsonRecord | null {
  if (!isRecord(value)) return null;
  return value;
}

export function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

export function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

export function asArray(value: unknown): readonly unknown[] | null {
  return Array.isArray(value) ? value : null;
}
