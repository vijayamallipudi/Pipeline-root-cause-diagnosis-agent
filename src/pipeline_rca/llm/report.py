"""Generates the root-cause report for a diagnosed run.

Flow:
  healthy run          -> short template note, no LLM call needed
  failed run           -> ask ollama to write it up from the facts
  ollama down / output doesn't mention the real table+column
                       -> fall back to a template report so there's always something

Every report ends with the raw evidence from the checks, so whoever reads it
can verify what the model wrote.
"""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pipeline_rca import config
from pipeline_rca.db import connect
from pipeline_rca.diagnostics import DiagnosticReport
from pipeline_rca.diagnostics.checks import Finding
from pipeline_rca.llm import client
from pipeline_rca.llm.prompts import (
    FIX_PLAYBOOK,
    SYSTEM_PROMPT,
    build_facts,
    build_user_prompt,
    describe_finding,
)

RCA_DDL = f"""
CREATE TABLE IF NOT EXISTS {config.META}.rca_reports (
    run_id      VARCHAR,
    created_at  TIMESTAMP,
    source      VARCHAR,   -- llm / template
    model       VARCHAR,
    grounded    BOOLEAN,
    seconds     DOUBLE,
    note        VARCHAR,
    markdown    VARCHAR
)
"""


@dataclass
class RcaReport:
    run_id: str | None
    markdown: str
    source: str  # "llm" or "template"
    model: str | None = None
    grounded: bool = True
    seconds: float = 0.0
    note: str = ""
    path: Path | None = None


def _is_grounded(text: str, root: Finding) -> bool:
    """The write-up has to at least name the table and column that actually broke."""
    low = text.lower()
    table_short = root.table.split(".")[-1]
    has_table = root.table.lower() in low or table_short.lower() in low
    has_column = (root.column or "").lower() in low
    return has_table and has_column


def _clean_llm_text(text: str) -> str:
    text = text.strip()
    # models sometimes wrap the whole answer in ```markdown ... ``` - a leftover fence
    # would swallow the evidence block below it, so strip it
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3]
    # drop any chatter before the first heading
    idx = text.find("## ")
    return text[idx:].strip() if idx > 0 else text.strip()


REQUIRED_SECTIONS = ["Summary", "Where it broke", "Root cause", "Downstream impact", "Recommended fix"]


def _template_sections(root: Finding, symptoms: list[Finding]) -> dict[str, str]:
    where = f"{root.table}.{root.column}" if root.column else root.table
    seg = root.evidence.get("affected_segment")
    pattern = root.evidence.get("pattern")
    impact = "\n".join(f"- {s.message} ({s.transition})" for s in symptoms) or "- none"
    fix = "\n".join(f"{i}. {step}" for i, step in enumerate(FIX_PLAYBOOK.get(root.category, []), 1))
    return {
        "Summary": f"{root.message}. The problem started at the {root.transition} step"
        + (f" and is limited to {seg}" if seg else "")
        + f". It caused {len(symptoms)} downstream issue(s) in later layers.",
        "Where it broke": f"Step `{root.transition}`, column `{where}`.",
        "Root cause": f"{root.category.replace('_', ' ').title()}: {root.message}."
        + (f" Pattern: {pattern}." if pattern else ""),
        "Downstream impact": impact,
        "Recommended fix": fix,
    }


def _template_body(root: Finding, symptoms: list[Finding]) -> str:
    sections = _template_sections(root, symptoms)
    return "\n\n".join(f"## {h}\n{sections[h]}" for h in REQUIRED_SECTIONS)


def _fill_missing_sections(text: str, root: Finding, symptoms: list[Finding]) -> tuple[str, list[str]]:
    """Small models sometimes skip a section - fill any gaps from the template."""
    missing = [h for h in REQUIRED_SECTIONS if f"## {h}".lower() not in text.lower()]
    if not missing:
        return text, []
    sections = _template_sections(root, symptoms)
    extra = "\n\n".join(f"## {h}\n{sections[h]}" for h in missing)
    return text.rstrip() + "\n\n" + extra, missing


EVIDENCE_WIDTH = 100


def _wrap(line: str) -> str:
    """Wrap long evidence lines, keeping the indent so nested values still line up."""
    if len(line) <= EVIDENCE_WIDTH:
        return line
    indent = " " * (len(line) - len(line.lstrip()) + 4)
    return textwrap.fill(line, EVIDENCE_WIDTH, subsequent_indent=indent, break_on_hyphens=False)


def _evidence_appendix(root: Finding, symptoms: list[Finding]) -> str:
    blocks = [describe_finding(root)] + [describe_finding(s) for s in symptoms]
    body = "\n".join(_wrap(line) for line in "\n\n".join(blocks).splitlines())
    return "---\n### Evidence from automated checks\n```\n" + body + "\n```"


def _healthy_note(diag: DiagnosticReport) -> str:
    checks = ", ".join(diag.checks)
    expected = "\n".join(f"- {f.message}" for f in diag.expected_changes) or "- none"
    return (
        "## Summary\nAll checks passed. Row counts, schemas, keys, nulls and balance totals "
        f"reconcile across raw, staging and final.\n\nChecks run: {checks}\n\n"
        f"## Expected changes\n{expected}"
    )


def _header(diag: DiagnosticReport, source: str, model: str | None) -> str:
    by = f"ollama / {model}" if source == "llm" else "template"
    return f"# Pipeline incident report - run {diag.run_id}\n\nStatus: **{diag.status}** | written by: {by}\n"


def generate_report(
    diag: DiagnosticReport,
    db_path=None,
    model: str | None = None,
    use_llm: bool = True,
    save: bool = True,
    on_token=None,
) -> RcaReport:
    model = model or config.OLLAMA_MODEL
    root = diag.root_cause

    if root is None:
        report = RcaReport(diag.run_id, _header(diag, "template", None) + "\n" + _healthy_note(diag), "template",
                           note="healthy run - no LLM call")
    else:
        body, source, grounded, seconds, note = None, "template", True, 0.0, ""
        if not use_llm:
            note = "LLM disabled"
        elif not client.is_available(model):
            note = f"ollama or model '{model}' not available - used template"
        else:
            facts = build_facts(diag.for_llm(), root, diag.symptoms, diag.expected_changes)
            try:
                result = client.chat(SYSTEM_PROMPT, build_user_prompt(facts), model=model, on_token=on_token)
                seconds = result.seconds
                text = _clean_llm_text(result.text)
                if _is_grounded(text, root):
                    body, filled = _fill_missing_sections(text, root, diag.symptoms)
                    source = "llm"
                    if filled:
                        note = "filled from template: " + ", ".join(filled)
                else:
                    grounded = False
                    note = "LLM output didn't name the root-cause table/column - used template"
            except client.OllamaError as e:
                note = f"ollama error: {e}"

        if body is None:
            body = _template_body(root, diag.symptoms)
        markdown = "\n".join([
            _header(diag, source, model),
            body,
            "",
            _evidence_appendix(root, diag.symptoms),
        ])
        report = RcaReport(diag.run_id, markdown, source, model if source == "llm" else None,
                           grounded, seconds, note)

    if save and diag.run_id:
        _save(report, db_path)
    return report


def _save(report: RcaReport, db_path) -> None:
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^A-Za-z0-9_-]", "_", report.run_id)
    report.path = config.REPORTS_DIR / f"{safe_id}.md"
    report.path.write_text(report.markdown, encoding="utf-8")

    with connect(db_path) as con:
        con.execute(RCA_DDL)
        con.execute(f"DELETE FROM {config.META}.rca_reports WHERE run_id = ?", [report.run_id])
        con.execute(
            f"INSERT INTO {config.META}.rca_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [report.run_id, datetime.now(timezone.utc).replace(tzinfo=None), report.source, report.model,
             report.grounded, report.seconds, report.note, report.markdown],
        )
