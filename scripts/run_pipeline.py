"""Run the pipeline and diagnose it.

    python scripts/run_pipeline.py                        # clean run
    python scripts/run_pipeline.py --scenario null_spike  # break something
    python scripts/run_pipeline.py --list                 # list scenarios
    python scripts/run_pipeline.py --scenario type_change --json      # full diagnostic output
    python scripts/run_pipeline.py --scenario type_change --explain   # + AI root-cause report (ollama)
    python scripts/run_pipeline.py --fail-on-problem                  # exit 1 if a problem is found (for cron/CI)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_rca import config  # noqa: E402
from pipeline_rca.chaos import Scenario, list_scenarios  # noqa: E402
from pipeline_rca.diagnostics import DiagnosticReport, diagnose  # noqa: E402
from pipeline_rca.llm import generate_report  # noqa: E402
from pipeline_rca.pipeline import run_pipeline  # noqa: E402


def print_report(report: DiagnosticReport) -> None:
    print(f"\nDiagnosis: {report.status}")
    print("  checks: " + "  ".join(f"{k}={v}" for k, v in report.checks.items()))

    for f in report.expected_changes:
        print(f"  expected: {f.message}")

    if report.root_cause:
        rc = report.root_cause
        print(f"\nRoot cause  [{rc.category}] at {rc.transition}, {rc.table}.{rc.column}")
        print(f"  {rc.message}")
        for key in ("affected_segment", "pattern", "examples", "sample_values", "origination_date_range"):
            if rc.evidence.get(key):
                print(f"  {key}: {rc.evidence[key]}")

    if report.symptoms:
        print("\nDownstream symptoms")
        for f in report.symptoms:
            print(f"  - [{f.category}] {f.transition}: {f.message}")

    ev = report.evaluation
    if ev.get("scenario") not in (None, "none"):
        verdict = "correct" if ev["correct"] else "WRONG"
        print(f"\nCheck vs injected chaos: {verdict} "
              f"(expected {ev['scenario']} at {ev['location_expected']}, "
              f"got {ev['diagnosed_as']} at {ev['location_found']})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default="none", choices=[s.value for s in Scenario])
    parser.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    parser.add_argument("--db", default=str(config.DB_PATH), help="DuckDB file path")
    parser.add_argument("--list", action="store_true", help="list chaos scenarios and exit")
    parser.add_argument("--json", action="store_true", help="print the full diagnostic report as JSON")
    parser.add_argument("--explain", action="store_true", help="write a root-cause report with the local LLM")
    parser.add_argument("--model", default=config.OLLAMA_MODEL, help="ollama model to use with --explain")
    parser.add_argument("--fail-on-problem", action="store_true",
                        help="exit with code 1 when diagnostics find a problem, so schedulers/CI can alert on it")
    args = parser.parse_args()

    if args.list:
        for s in list_scenarios():
            print(f"{s['scenario']:<18} [{s['stage']}:{s['table']}] {s['title']}")
            print(f"{'':<18} {s['description']}\n")
        return 0

    result = run_pipeline(scenario=args.scenario, db_path=args.db, seed=args.seed)
    report = diagnose(db_path=args.db, run_id=result.run_id)

    exit_code = 1 if args.fail_on_problem and report.status != "HEALTHY" else 0

    if args.json:
        out = report.to_dict()
        if args.explain:
            rca = generate_report(report, db_path=args.db, model=args.model)
            out["rca_report"] = {"source": rca.source, "model": rca.model, "note": rca.note,
                                 "path": str(rca.path) if rca.path else None, "markdown": rca.markdown}
        print(json.dumps(out, indent=2, default=str))
        return exit_code

    print(f"\nRun {result.run_id}  scenario={result.scenario}")
    print("Row counts")
    for layer, tables in result.row_counts.items():
        print(f"  {layer:<8} " + "  ".join(f"{t}={n:,}" for t, n in tables.items()))
    print_report(report)

    if args.explain:
        print(f"\nWriting root-cause report with {args.model} (this can take a minute or two on CPU)...")
        rca = generate_report(report, db_path=args.db, model=args.model)
        print(f"[{rca.source}{', ' + str(rca.seconds) + 's' if rca.seconds else ''}] {rca.note}".rstrip())
        print("\n" + rca.markdown)
        if rca.path:
            print(f"\nSaved to {rca.path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
