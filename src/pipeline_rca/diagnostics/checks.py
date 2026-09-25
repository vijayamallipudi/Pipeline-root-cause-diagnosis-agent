"""The individual diagnostic checks.

Each check returns a list of Findings. A finding says what's wrong, which
transition introduced it (source->raw, raw->staging, staging->final) and
carries evidence - segment breakdowns, sample values, etc.

A problem is only reported at the layer where it first appears. If staging
already has the nulls, final having them too isn't a new finding - it gets
listed under persists_in instead.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import duckdb

from pipeline_rca.diagnostics.contracts import (
    CONTRACTS,
    MAX_UNKNOWN_RISK_PCT,
    MEASURE_TOLERANCE_PCT,
)
from pipeline_rca.diagnostics.snapshot import NUMERIC_TYPES, TableSnapshot
from pipeline_rca.pipeline.staging import EXCLUDED_STATUSES

CRITICAL, WARNING, INFO = "critical", "warning", "info"

# raw balance with currency formatting stripped - what the value was *meant* to be
PARSED_RAW_BALANCE = "TRY_CAST(REPLACE(REPLACE(CAST(balance AS VARCHAR), '$', ''), ',', '') AS DOUBLE)"
EXCLUDED_SQL = ", ".join(f"'{s}'" for s in EXCLUDED_STATUSES)


@dataclass
class Finding:
    check: str
    category: str
    severity: str
    transition: str
    table: str
    column: str | None
    message: str
    expected: Any = None
    actual: Any = None
    evidence: dict = field(default_factory=dict)

    @property
    def failed(self) -> bool:
        return self.severity != INFO

    def to_dict(self) -> dict:
        return asdict(self)


def _q(con, sql, params=None):
    return con.execute(sql, params or []).fetchall()


def _scalar(con, sql, params=None):
    return con.execute(sql, params or []).fetchone()[0]


def _upstream(table: str, column: str | None = None) -> str | None:
    if table == "staging.customers":
        return "raw.customers"
    if table == "staging.loans":
        return "raw.loans"
    if table == "final.loan_portfolio":
        return "staging.customers" if column in ("credit_score", "state") else "staging.loans"
    return None


def _downstream(table: str) -> list[str]:
    """Every table fed (directly or not) by this one."""
    direct = [t for t in (c.name for c in CONTRACTS) if _upstream(t) == table or
              (table == "staging.customers" and t == "final.loan_portfolio")]
    return direct + [d for t in direct for d in _downstream(t)]


def _segments(con, table: str, flag_sql: str) -> list[dict]:
    """Break a problem down by source_system, if the table has one."""
    rows = _q(
        con,
        f"SELECT source_system, COUNT(*), SUM(CASE WHEN {flag_sql} THEN 1 ELSE 0 END) "
        f"FROM {table} GROUP BY 1 ORDER BY 3 DESC, 1",
    )
    return [
        {"source_system": s, "rows": int(n), "affected": int(a or 0),
         "affected_pct": round(100.0 * (a or 0) / n, 1)}
        for s, n, a in rows
    ]


def _concentrated_in(segments: list[dict]) -> str | None:
    """If basically all affected rows come from one source system, name it."""
    total = sum(s["affected"] for s in segments)
    hit = [s for s in segments if s["affected"] > 0]
    if total and len(hit) == 1:
        return f"source_system = '{hit[0]['source_system']}'"
    return None


def _has_col(snap: TableSnapshot, col: str) -> bool:
    return col in snap.columns


# ---------------------------------------------------------------- schema

def check_schema(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    findings = []
    for c in CONTRACTS:
        snap = snaps[c.name]
        for col, expected_type in c.columns.items():
            if col not in snap.columns:
                findings.append(Finding("schema", "SCHEMA_DRIFT", CRITICAL, c.written_by, c.name, col,
                                        f"{c.name}.{col} is missing", expected_type, None))
                continue
            actual_type = snap.columns[col].dtype
            if actual_type == expected_type:
                continue

            evidence: dict = {}
            if expected_type in NUMERIC_TYPES and actual_type == "VARCHAR":
                flag = f'TRY_CAST("{col}" AS DOUBLE) IS NULL AND "{col}" IS NOT NULL'
                bad = _scalar(con, f"SELECT COUNT(*) FROM {c.name} WHERE {flag}")
                samples = [r[0] for r in _q(con, f'SELECT DISTINCT "{col}" FROM {c.name} WHERE {flag} LIMIT 3')]
                evidence = {"non_numeric_rows": int(bad),
                            "non_numeric_pct": round(100.0 * bad / snap.row_count, 1),
                            "sample_values": samples}
                if any("$" in s or "," in s for s in samples):
                    evidence["pattern"] = "values contain currency symbols / thousands separators"
                if _has_col(snap, "source_system"):
                    segs = _segments(con, c.name, flag)
                    evidence["segments"] = segs
                    evidence["affected_segment"] = _concentrated_in(segs)

            findings.append(Finding(
                "schema", "SCHEMA_DRIFT", CRITICAL, c.written_by, c.name, col,
                f"{c.name}.{col} arrived as {actual_type}, contract says {expected_type}",
                expected_type, actual_type, evidence,
            ))
    return findings


# ---------------------------------------------------------------- nulls

def check_nulls(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    findings = []
    for c in CONTRACTS:
        snap = snaps[c.name]
        for col in c.columns:
            prof = snap.columns.get(col)
            if prof is None:
                continue
            limit = c.null_limit(col)
            if prof.null_pct <= limit:
                continue

            up = _upstream(c.name, col)
            up_prof = snaps[up].columns.get(col) if up else None
            if up_prof is not None and up_prof.null_pct > limit:
                continue  # inherited - already reported upstream

            evidence: dict = {"null_rows": prof.null_count, "limit_pct": limit}
            if up_prof is not None:
                evidence["upstream_table"] = up
                evidence["upstream_null_pct"] = up_prof.null_pct
                evidence["upstream_dtype"] = up_prof.dtype
                if up_prof.dtype != prof.dtype:
                    evidence["note"] = (f"upstream column is {up_prof.dtype}; values that failed "
                                        f"the cast to {prof.dtype} became NULL")
            if _has_col(snap, "source_system"):
                segs = _segments(con, c.name, f'"{col}" IS NULL')
                evidence["segments"] = segs
                evidence["affected_segment"] = _concentrated_in(segs)
            evidence["persists_in"] = [
                d for d in _downstream(c.name)
                if col in snaps[d].columns and snaps[d].columns[col].null_pct > limit
            ]

            where = "arrived with" if c.written_by == "source->raw" else f"picked up during {c.written_by}:"
            findings.append(Finding(
                "null_rate", "NULL_SPIKE", CRITICAL if prof.null_pct > 10 else WARNING,
                c.written_by, c.name, col,
                f"{c.name}.{col} {where} {prof.null_pct}% nulls (limit {limit}%)",
                f"<= {limit}%", f"{prof.null_pct}%", evidence,
            ))
    return findings


# ---------------------------------------------------------------- uniqueness

def check_uniqueness(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    findings = []
    for c in CONTRACTS:
        snap = snaps[c.name]
        up = _upstream(c.name, c.primary_key)
        up_dups = snaps[up].duplicate_keys if up and snaps[up].primary_key == c.primary_key else 0
        new_dups = snap.duplicate_keys - up_dups
        if new_dups <= 0:
            continue

        pk = c.primary_key
        exact = snap.row_count - _scalar(con, f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {c.name})")
        evidence: dict = {"duplicate_rows": snap.duplicate_keys, "exact_copies": int(exact),
                          "upstream_duplicate_rows": up_dups}
        if _has_col(snap, "origination_date"):
            lo, hi, n = con.execute(
                f"SELECT CAST(MIN(origination_date) AS DATE)::VARCHAR, CAST(MAX(origination_date) AS DATE)::VARCHAR, "
                f"COUNT(*) FROM (SELECT {pk}, ANY_VALUE(origination_date) AS origination_date "
                f"FROM {c.name} GROUP BY {pk} HAVING COUNT(*) > 1)"
            ).fetchone()
            evidence["duplicated_keys"] = int(n)
            evidence["origination_date_range"] = [lo, hi]
            overall_lo = _scalar(con, f"SELECT CAST(MIN(origination_date) AS DATE)::VARCHAR FROM {c.name}")
            evidence["table_origination_start"] = overall_lo
        if _has_col(snap, "source_system"):
            rows = _q(con, f"SELECT source_system, COUNT(*) - COUNT(DISTINCT {pk}) FROM {c.name} GROUP BY 1 ORDER BY 2 DESC")
            evidence["segments"] = [{"source_system": s, "extra_rows": int(n)} for s, n in rows]
        evidence["persists_in"] = [d for d in _downstream(c.name) if snaps[d].duplicate_keys > 0]

        findings.append(Finding(
            "uniqueness", "DUPLICATE_ROWS", CRITICAL, c.written_by, c.name, pk,
            f"{c.name} has {snap.duplicate_keys:,} duplicate {pk} rows, none upstream"
            if not up_dups else f"{c.name} has {new_dups:,} more duplicate {pk} rows than upstream",
            0, snap.duplicate_keys, evidence,
        ))
    return findings


# ---------------------------------------------------------------- join keys

def check_key_integrity(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    """customer_id on a loan shouldn't change between raw and staging (beyond trimming)."""
    base = """
        FROM (SELECT DISTINCT loan_id, TRIM(customer_id) AS raw_id, source_system FROM raw.loans) r
        JOIN (SELECT DISTINCT loan_id, customer_id AS stg_id FROM staging.loans) s USING (loan_id)
        WHERE r.raw_id <> s.stg_id
    """
    changed = _scalar(con, f"SELECT COUNT(*) {base}")
    if not changed:
        return []

    segs = _q(con, f"SELECT source_system, COUNT(*) {base} GROUP BY 1 ORDER BY 2 DESC")
    examples = _q(con, f"SELECT raw_id, stg_id {base} LIMIT 3")
    zeros = _scalar(con, f"SELECT COUNT(*) {base} AND LPAD(s.stg_id, LENGTH(r.raw_id)::INTEGER, '0') = r.raw_id")
    orphans = _scalar(con, "SELECT COUNT(*) FROM staging.loans l ANTI JOIN staging.customers c USING (customer_id)")

    evidence = {
        "changed_rows": int(changed),
        "segments": [{"source_system": s, "changed_rows": int(n)} for s, n in segs],
        "affected_segment": f"source_system = '{segs[0][0]}'" if len(segs) == 1 else None,
        "examples": [f"'{a}' -> '{b}'" for a, b in examples],
        "rows_now_unmatched_in_customers": int(orphans),
    }
    if zeros == changed:
        evidence["pattern"] = "leading zeros stripped - looks like the key was cast to an integer"

    return [Finding(
        "key_integrity", "JOIN_KEY_MISMATCH", CRITICAL, "raw->staging", "staging.loans", "customer_id",
        f"customer_id changed on {changed:,} loans between raw and staging",
        "unchanged", f"{changed:,} changed", evidence,
    )]


# ---------------------------------------------------------------- row counts

def check_row_counts(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    findings = []

    # raw -> staging customers: only dedupe should change the count
    exp = _scalar(con, "SELECT COUNT(DISTINCT TRIM(customer_id)) FROM raw.customers")
    act = snaps["staging.customers"].row_count
    if act != exp:
        findings.append(_count_finding("raw->staging", "staging.customers", exp, act, {}))

    # raw -> staging loans: dedupe + drop WITHDRAWN
    raw_n = snaps["raw.loans"].row_count
    withdrawn = _scalar(con, f"SELECT COUNT(DISTINCT loan_id) FROM raw.loans WHERE UPPER(TRIM(status)) IN ({EXCLUDED_SQL})")
    raw_dups = snaps["raw.loans"].duplicate_keys
    exp = _scalar(con, f"SELECT COUNT(DISTINCT loan_id) FROM raw.loans WHERE UPPER(TRIM(status)) NOT IN ({EXCLUDED_SQL})")
    act = snaps["staging.loans"].row_count
    findings.append(Finding(
        "row_count", "EXPECTED_CHANGE", INFO, "raw->staging", "staging.loans", None,
        f"{raw_n - exp:,} loans are expected to drop out in staging: {withdrawn:,} WITHDRAWN "
        f"(business rule) and {raw_dups:,} duplicate keys",
        exp, exp, {"raw_rows": raw_n, "withdrawn": int(withdrawn), "raw_duplicates": raw_dups},
    ))
    if act != exp:
        findings.append(_count_finding("raw->staging", "staging.loans", exp, act, {"raw_rows": raw_n}))

    # staging -> final: every staged loan should make it through the join
    exp = snaps["staging.loans"].row_count
    act = snaps["final.loan_portfolio"].row_count
    if act != exp:
        evidence = {}
        if act < exp:
            evidence = _orphan_evidence(con)
        findings.append(_count_finding("staging->final", "final.loan_portfolio", exp, act, evidence))
    return findings


def _count_finding(transition, table, expected, actual, evidence) -> Finding:
    diff = actual - expected
    pct = round(100.0 * diff / expected, 1) if expected else 0.0
    if diff > 0:
        cat, msg = "ROW_INFLATION", f"{table} has {diff:,} more rows than expected (+{pct}%)"
    else:
        cat, msg = "ROW_LOSS", f"{-diff:,} rows lost during {transition} ({pct}%)"
    return Finding("row_count", cat, CRITICAL if abs(pct) >= 1 else WARNING, transition, table, None,
                   msg, expected, actual, evidence)


def _orphan_evidence(con) -> dict:
    anti = "FROM staging.loans l ANTI JOIN staging.customers c USING (customer_id)"
    orphans = _scalar(con, f"SELECT COUNT(*) {anti}")
    if not orphans:
        return {}
    segs = _q(con, f"SELECT source_system, COUNT(*) {anti} GROUP BY 1 ORDER BY 2 DESC")
    samples = [r[0] for r in _q(con, f"SELECT DISTINCT customer_id {anti} LIMIT 3")]
    key_len = _scalar(con, "SELECT MODE(LENGTH(customer_id)) FROM staging.customers")
    padded = _scalar(
        con,
        f"SELECT COUNT(*) {anti} WHERE LPAD(l.customer_id, {key_len}, '0') IN (SELECT customer_id FROM staging.customers)",
    )
    missing_types = [r[0] for r in _q(
        con,
        "SELECT DISTINCT loan_type FROM staging.loans EXCEPT SELECT DISTINCT loan_type FROM final.loan_portfolio ORDER BY 1",
    )]
    ev = {
        "cause": "loans whose customer_id has no match in staging.customers are dropped by the inner join",
        "unmatched_rows": int(orphans),
        "segments": [{"source_system": s, "unmatched_rows": int(n)} for s, n in segs],
        "affected_segment": f"source_system = '{segs[0][0]}'" if len(segs) == 1 else None,
        "sample_unmatched_keys": samples,
        "customer_key_length": int(key_len),
        "match_if_zero_padded": int(padded),
    }
    if missing_types:
        ev["loan_types_missing_from_final"] = missing_types
    return ev


# ---------------------------------------------------------------- totals

def check_measures(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    """Total outstanding balance should reconcile layer to layer."""
    findings = []

    raw_by_seg = dict(_q(con, f"""
        SELECT source_system, SUM(bal) FROM (
            SELECT loan_id, ANY_VALUE(source_system) AS source_system, ANY_VALUE({PARSED_RAW_BALANCE}) AS bal
            FROM raw.loans WHERE UPPER(TRIM(status)) NOT IN ({EXCLUDED_SQL}) GROUP BY loan_id
        ) GROUP BY 1
    """))
    stg_by_seg = dict(_q(con, "SELECT source_system, COALESCE(SUM(balance), 0) FROM staging.loans GROUP BY 1"))
    fin_by_seg = dict(_q(con, "SELECT source_system, COALESCE(SUM(balance), 0) FROM final.loan_portfolio GROUP BY 1"))

    for transition, table, exp_seg, act_seg in (
        ("raw->staging", "staging.loans", raw_by_seg, stg_by_seg),
        ("staging->final", "final.loan_portfolio", stg_by_seg, fin_by_seg),
    ):
        exp, act = sum(exp_seg.values()), sum(act_seg.values())
        pct = 100.0 * (act - exp) / exp if exp else 0.0
        if abs(pct) <= MEASURE_TOLERANCE_PCT:
            continue
        segs = []
        for s in sorted(exp_seg):
            e, a = exp_seg.get(s, 0.0), act_seg.get(s, 0.0)
            segs.append({"source_system": s, "expected": round(e, 2), "actual": round(a, 2),
                         "delta": round(a - e, 2)})
        segs.sort(key=lambda x: abs(x["delta"]), reverse=True)
        direction = "short" if act < exp else "over"
        findings.append(Finding(
            "measure", "MEASURE_DRIFT", CRITICAL if abs(pct) >= 5 else WARNING, transition, table, "balance",
            f"total balance in {table} is {direction} by ${abs(act - exp):,.0f} ({pct:+.1f}%) vs the layer before",
            round(exp, 2), round(act, 2), {"delta": round(act - exp, 2), "delta_pct": round(pct, 2), "segments": segs},
        ))
    return findings


# ---------------------------------------------------------------- distribution

def check_distribution(con, snaps: dict[str, TableSnapshot]) -> list[Finding]:
    n = snaps["final.loan_portfolio"].row_count
    unknown = _scalar(con, "SELECT COUNT(*) FROM final.loan_portfolio WHERE risk_band = 'UNKNOWN'")
    pct = round(100.0 * unknown / n, 1) if n else 0.0
    if pct <= MAX_UNKNOWN_RISK_PCT:
        return []
    null_score = _scalar(con, "SELECT COUNT(*) FROM final.loan_portfolio WHERE risk_band = 'UNKNOWN' AND credit_score IS NULL")
    return [Finding(
        "distribution", "DISTRIBUTION_SHIFT", CRITICAL if pct > 10 else WARNING,
        "staging->final", "final.loan_portfolio", "risk_band",
        f"{pct}% of loans in final have risk_band = UNKNOWN (limit {MAX_UNKNOWN_RISK_PCT}%)",
        f"<= {MAX_UNKNOWN_RISK_PCT}%", f"{pct}%",
        {"unknown_rows": int(unknown), "unknown_with_null_credit_score": int(null_score),
         "note": "risk_band is UNKNOWN whenever credit_score is NULL"},
    )]


ALL_CHECKS = [
    check_schema,
    check_key_integrity,
    check_uniqueness,
    check_nulls,
    check_row_counts,
    check_measures,
    check_distribution,
]
