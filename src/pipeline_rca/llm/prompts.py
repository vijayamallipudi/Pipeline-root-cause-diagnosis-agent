"""Turns the diagnostic report into a prompt.

Small local models make things up if you hand them a big JSON blob, so the
findings get flattened into short plain-text facts first. The model's job is
to explain those facts, not to find new ones. The fix suggestions come from
a playbook per failure type so they're grounded in something real.
"""

from __future__ import annotations

import json

from pipeline_rca.diagnostics.checks import Finding

SYSTEM_PROMPT = """You are a senior data engineer writing an incident note for your team.
A loan data pipeline moves data raw -> staging -> final in DuckDB. An automated
diagnostic has already found what went wrong. Your job is to explain it clearly.

Rules:
- Only use facts from the FACTS section. Do not invent numbers, tables, columns or causes.
- The FIX PLAYBOOK is a list of suggestions for the fix section, not facts about what happened.
- Always name the exact table and column from the root cause.
- Keep numbers exactly as given.
- EXPECTED CHANGES are normal business rules. Never list them as impact or as part of the problem.
- Be direct and specific. No filler, no apologies, no mention of being an AI.
- Write in Markdown using exactly these headings:

## Summary
(2-3 sentences a manager could read: what broke and what it affects)

## Where it broke
(the pipeline step and table.column where the problem started)

## Root cause
(what happened and the evidence that proves it)

## Downstream impact
(bullet list of the symptoms this caused further down the pipeline)

## Recommended fix
(numbered steps: immediate fix, then how to stop it happening again)

All five sections are required.
"""

# what an experienced engineer would do for each type of failure
FIX_PLAYBOOK = {
    "SCHEMA_DRIFT": [
        "Confirm with the source system owner whether their export format changed and when.",
        "Parse the new format explicitly in staging (strip currency symbols / separators) instead of a blind numeric cast.",
        "Make the schema contract check fail the load rather than letting values coerce to NULL.",
        "Reprocess the affected loads once the fix is in.",
    ],
    "NULL_SPIKE": [
        "Check the upstream feed / enrichment job logs for the affected load.",
        "Re-run the enrichment for the affected records.",
        "Add a null-rate threshold that blocks promotion to final when it's breached.",
    ],
    "JOIN_KEY_MISMATCH": [
        "Find the recent change in the staging transform that touches the key column.",
        "Keep IDs as strings end to end - never cast zero-padded keys to integers.",
        "Re-run staging and final for the affected loads.",
        "Add a key-format check (length / pattern) and an orphan-row count before the join.",
    ],
    "DUPLICATE_ROWS": [
        "Check the load / retry logs for the run that double-loaded.",
        "Make the load idempotent (MERGE / upsert on the key, or delete-then-insert by batch).",
        "De-duplicate staging and rebuild final.",
        "Keep a unique-key check on staging so this fails loudly next time.",
    ],
}

# evidence keys worth showing the model, in order
EVIDENCE_KEYS = [
    "affected_segment", "pattern", "note", "examples", "sample_values", "sample_unmatched_keys",
    "non_numeric_rows", "non_numeric_pct", "null_rows", "upstream_null_pct", "upstream_dtype",
    "changed_rows", "duplicate_rows", "exact_copies", "origination_date_range", "unmatched_rows",
    "match_if_zero_padded", "loan_types_missing_from_final", "rows_now_unmatched_in_customers",
    "cause", "delta", "delta_pct", "unknown_rows", "persists_in",
]


def _fmt(value) -> str:
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)[:300]
    return str(value)


def _segments(finding: Finding) -> str | None:
    segs = finding.evidence.get("segments")
    if not segs:
        return None
    parts = []
    for s in segs[:4]:
        name = s.get("source_system")
        rest = ", ".join(f"{k}={_fmt(v)}" for k, v in s.items() if k != "source_system")
        parts.append(f"{name} ({rest})")
    return "; ".join(parts)


def describe_finding(f: Finding) -> str:
    where = f"{f.table}.{f.column}" if f.column else f.table
    lines = [f"[{f.category}] at step {f.transition}, {where}: {f.message}"]
    if f.expected is not None or f.actual is not None:
        lines.append(f"  expected: {_fmt(f.expected)} | actual: {_fmt(f.actual)}")
    for key in EVIDENCE_KEYS:
        val = f.evidence.get(key)
        if val not in (None, [], ""):
            lines.append(f"  {key}: {_fmt(val)}")
    segs = _segments(f)
    if segs:
        lines.append(f"  by source_system: {segs}")
    return "\n".join(lines)


def build_facts(report: dict, root: Finding, symptoms: list[Finding], expected: list[Finding]) -> str:
    impact = report.get("impact", {})
    lines = [
        "FACTS",
        f"Run: {report.get('run_id')}",
        "Pipeline: source systems -> raw -> staging (pandas cleaning) -> final (SQL join of loans to customers)",
        f"Checks: {', '.join(f'{k}={v}' for k, v in report.get('checks', {}).items())}",
        "",
        "ROOT CAUSE (earliest failure in the pipeline):",
        describe_finding(root),
        "",
        "DOWNSTREAM SYMPTOMS (caused by the root cause):",
    ]
    lines += [describe_finding(s) for s in symptoms] or ["  none"]
    lines += ["", "EXPECTED CHANGES (normal business rules, NOT problems):"]
    lines += [f"  {f.message}" for f in expected] or ["  none"]
    lines += [
        "",
        "FINAL LAYER NOW:",
        f"  loan rows: {_fmt(impact.get('final_loan_rows'))} (staging had {_fmt(impact.get('staging_loan_rows'))})",
        f"  distinct loans: {_fmt(impact.get('final_distinct_loans'))}",
        f"  total balance: {_fmt(impact.get('final_total_balance'))}",
        "",
        "FIX PLAYBOOK for this type of failure:",
    ]
    lines += [f"  - {step}" for step in FIX_PLAYBOOK.get(root.category, ["Investigate the root cause above."])]
    return "\n".join(lines)


def build_user_prompt(facts: str) -> str:
    return facts + "\n\nWrite the incident note now, using the headings from your instructions."
