"""Run the pipeline from the command line.

    python scripts/run_pipeline.py                        # clean run
    python scripts/run_pipeline.py --scenario null_spike  # break something
    python scripts/run_pipeline.py --list                 # list scenarios
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_rca import config  # noqa: E402
from pipeline_rca.chaos import Scenario, list_scenarios  # noqa: E402
from pipeline_rca.db import connect  # noqa: E402
from pipeline_rca.pipeline import run_pipeline  # noqa: E402

# quick sanity numbers per layer - the real diagnostics come in phase 2
VITALS_SQL = """
SELECT '{layer}'                                        AS layer,
       COUNT(*)                                         AS loans,
       COUNT(DISTINCT loan_id)                          AS distinct_loans,
       ROUND(SUM(TRY_CAST(balance AS DOUBLE)), 0)       AS total_balance,
       ROUND(100.0 * AVG((TRY_CAST(balance AS DOUBLE) IS NULL)::INT), 1) AS balance_not_numeric_pct,
       typeof(ANY_VALUE(balance))                       AS balance_type
FROM {table}
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default="none", choices=[s.value for s in Scenario])
    parser.add_argument("--seed", type=int, default=config.DEFAULT_SEED)
    parser.add_argument("--db", default=str(config.DB_PATH), help="DuckDB file path")
    parser.add_argument("--list", action="store_true", help="list chaos scenarios and exit")
    args = parser.parse_args()

    if args.list:
        for s in list_scenarios():
            print(f"{s['scenario']:<18} [{s['stage']}:{s['table']}] {s['title']}")
            print(f"{'':<18} {s['description']}\n")
        return 0

    result = run_pipeline(scenario=args.scenario, db_path=args.db, seed=args.seed)

    print(f"\nRun {result.run_id}  scenario={result.scenario}  db={args.db}\n")
    print("Row counts per layer")
    for layer, tables in result.row_counts.items():
        print(f"  {layer:<8} " + "  ".join(f"{t}={n:,}" for t, n in tables.items()))

    with connect(args.db) as con:
        vitals = con.execute(
            " UNION ALL ".join(
                VITALS_SQL.format(layer=layer, table=table)
                for layer, table in (
                    ("raw", "raw.loans"),
                    ("staging", "staging.loans"),
                    ("final", "final.loan_portfolio"),
                )
            )
        ).df()
        bands = con.execute(
            "SELECT risk_band, COUNT(*) AS loans FROM final.loan_portfolio GROUP BY 1 ORDER BY 1"
        ).df()

    print("\nLoan vitals per layer")
    print(vitals.to_string(index=False))
    print("\nFinal risk bands")
    print(bands.to_string(index=False))

    if result.ground_truth:
        gt = result.ground_truth
        print(f"\nChaos injected at {gt['injected_at']}: {gt['title']} ({gt['rows_affected']:,} rows)")
        print(f"  {gt['description']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
