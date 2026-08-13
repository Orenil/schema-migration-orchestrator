/**
 * Migration specs are trusted operator input (like a migration file you'd review in a PR), not
 * end-user input — but table/column names still get interpolated into DDL/DML strings because
 * Postgres does not support parameter binding for identifiers. We whitelist-validate every
 * identifier before interpolation so a typo'd or malicious spec fails loudly instead of producing
 * broken SQL, and we double-quote everything so mixed-case/reserved-word identifiers work.
 */
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/;

export function assertValidIdentifier(name: string, what: string): void {
  if (!IDENTIFIER_RE.test(name)) {
    throw new Error(
      `Invalid ${what} "${name}": identifiers must be alphanumeric/underscore, optionally schema-qualified.`
    );
  }
}

/** Quotes a (possibly schema-qualified) identifier, e.g. "public.users" -> "public"."users". */
export function quoteIdent(name: string): string {
  return name
    .split('.')
    .map((part) => `"${part.replace(/"/g, '""')}"`)
    .join('.');
}
