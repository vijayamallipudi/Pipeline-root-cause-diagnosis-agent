# Pipeline Root-Cause Diagnosis Agent

A lot of my time as a data engineer goes into answering one question: "why doesn't this number match?" Usually the answer is buried somewhere between the source extract and the final table. A column changed type upstream, a join started dropping rows, a load ran twice. Finding it means going layer by layer and comparing row counts, schemas and totals until something doesn't line up.

This project automates that process. It runs a small loan pipeline (raw -> staging -> final) in DuckDB, breaks it on purpose, and then traces where things went wrong. A local LLM then writes up the root cause in plain English.

Everything runs locally and costs nothing. No cloud services, no API keys. The LLM part uses Ollama.

## Progress

- [x] Phase 1: synthetic data, 3-layer pipeline, chaos injector
- [x] Phase 2: diagnostic engine (snapshots and diffs per layer)
- [ ] Phase 3: root-cause report via Ollama
- [ ] Phase 4: Airflow DAG
- [ ] Phase 5: Streamlit demo
- [ ] Phase 6: architecture diagram and final docs

## How the pipeline works

```
 source systems          raw                 staging (pandas)          final (DuckDB SQL)

 CRM / KYC     --+                           clean codes, cast types
 CORE_BANKING  --+--> raw.customers  -->     drop WITHDRAWN loans  --> final.loan_portfolio
 CARD_PLATFORM --+    raw.loans              dedupe on keys            final.portfolio_summary
 MORTGAGE_SVC  --+
                       ^                       ^
                  chaos can hit here      or here
```

**raw** holds the extracts exactly as they arrived, messy casing and all.

**staging** cleans them up in pandas. It trims and upper-cases codes, casts types and de-dupes on the primary key. It also filters out `WITHDRAWN` loans, since those were never funded. That filter means a healthy run already goes from 5,000 loans in raw to about 4,860 in staging. I kept it in deliberately. The diagnostic step has to learn that some drops are expected and others aren't.

**final** is plain SQL. `loan_portfolio` joins loans to customers and adds a risk band and a delinquency flag. `portfolio_summary` rolls it up by loan type, which is the kind of table a dashboard would read from.

**meta.pipeline_runs** logs every run: which scenario was used, row counts per layer, and exactly what the chaos injector broke.

### The data

It's all fake, generated with Faker and NumPy with a fixed seed so runs are reproducible.

- `customers` (2,000 rows): zero-padded `customer_id` like `00001234`, name, email, state, DOB, credit score, income
- `loans` (5,000 rows): loan type, source system, principal, balance, rate, term, status, origination date

Loans come from three source systems. Cards come from `CARD_PLATFORM`, mortgages from `MORTGAGE_SVC`, and everything else from `CORE_BANKING`. That split matters for a couple of the failure scenarios below.

## Failure scenarios

I picked failures I've actually run into. None of them crash the job. The pipeline goes green and the numbers are just wrong, and those are the failures that take longest to find.

**`type_change`**: `CORE_BANKING` starts sending `balance` as a formatted string (`$12,345.67`). In raw, the column becomes VARCHAR. Staging does `pd.to_numeric(errors="coerce")`, which never fails and quietly turns roughly half the balances into NULL. The final portfolio comes up about $43M short.

**`null_spike`**: the credit bureau enrichment partially fails and about 35% of customers come through with no credit score. Around 1,700 loans end up in an `UNKNOWN` risk band.

**`dropped_join_key`**: a change in staging casts `CARD_PLATFORM` customer IDs to int, so `00001234` becomes `1234`. The inner join into final quietly drops all ~1,480 card loans, and `CREDIT_CARD` disappears from the summary.

**`duplicate_rows`**: a retried load isn't idempotent and re-appends the last 12 months of loans. That gives 630 duplicate loan IDs and inflates balances by about $27M.

For every run, the injector writes down what it broke: the column, the segment and the row count. Later phases use that to check whether the diagnosis got it right.

## Diagnostics

After every run, the diagnostic engine profiles each table in each layer: row count, column types, null %, distinct counts, min/max/sum. It then runs these checks against a set of contracts (`diagnostics/contracts.py`):

| Check | What it looks for |
|---|---|
| schema | column types that don't match the contract |
| key_integrity | join keys that changed between raw and staging |
| uniqueness | duplicate primary keys |
| nulls | columns over their null limit |
| row_counts | rows lost or gained that the business rules don't explain |
| measures | total balance not reconciling from one layer to the next |
| distribution | too many loans falling into the UNKNOWN risk band |

Finding that something is wrong is the easy part. The hard part is working out which failure caused the others. A type change upstream shows up as nulls in staging and a short total in final, and every one of those checks fails. So the engine only reports a problem at the layer where it *first* appears, and marks later layers as `persists_in`. It then sorts the failures in pipeline order. The earliest one is the root cause and the rest are symptoms. When two failures start at the same step, the more specific one wins. A key change explains row loss, not the other way around.

Each finding carries evidence you'd want before raising a ticket: which source system is affected, sample bad values, and a detected pattern where there is one ("leading zeros stripped", "currency symbols"). The WITHDRAWN drop in staging is reported as an expected change, not a failure.

Because the chaos injector records what it actually broke, every diagnosis is checked against the answer. Right now it gets the right cause and the right location for all four scenarios, and it reports the clean run as healthy.

Snapshots and reports are saved in `meta.layer_snapshots` and `meta.diagnostic_reports`.

## Running it

You need Python 3.10 or newer.

```bash
git clone https://github.com/vijayamallipudi/Pipeline-root-cause-diagnosis-agent.git
cd Pipeline-root-cause-diagnosis-agent

python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Mac/Linux

pip install -r requirements.txt
pip install -e .
```

Then:

```bash
python scripts/run_pipeline.py --list                        # see the scenarios
python scripts/run_pipeline.py                               # clean run
python scripts/run_pipeline.py --scenario dropped_join_key   # break something
pytest
```

Here's what the `dropped_join_key` run prints:

```
Diagnosis: FAILED
  checks: schema=PASS  key_integrity=FAIL  uniqueness=PASS  nulls=PASS  row_counts=FAIL  measures=FAIL  distribution=PASS
  expected: 141 loans are expected to drop out in staging: 141 WITHDRAWN (business rule) and 0 duplicate keys

Root cause  [JOIN_KEY_MISMATCH] at raw->staging, staging.loans.customer_id
  customer_id changed on 1,481 loans between raw and staging
  affected_segment: source_system = 'CARD_PLATFORM'
  pattern: leading zeros stripped - looks like the key was cast to an integer
  examples: ["'00001839' -> '1839'", "'00000353' -> '353'", "'00001418' -> '1418'"]

Downstream symptoms
  - [ROW_LOSS] staging->final: 1,481 rows lost during staging->final (-30.5%)
  - [MEASURE_DRIFT] staging->final: total balance in final.loan_portfolio is short by $9,281,746 (-4.4%) vs the layer before
```

Add `--json` to get the full structured report.

The database ends up at `data/pipeline.duckdb` if you want to poke around in it.

You can override a few settings with environment variables: `RCA_DB_PATH`, `RCA_SEED` (default 42), `RCA_N_CUSTOMERS` (2000) and `RCA_N_LOANS` (5000).

## Repo layout

```
src/pipeline_rca/
    config.py             settings
    data_generator.py     fake customers and loans
    chaos.py              failure scenarios
    db.py                 duckdb helpers
    pipeline/
        extract.py        raw layer
        staging.py        staging layer
        final.py          final layer
        runner.py         runs everything, logs the run
    diagnostics/
        contracts.py      expected schema, keys, null limits per table
        snapshot.py       profiles every table in every layer
        checks.py         the individual checks
        engine.py         runs checks, picks root cause vs symptoms
scripts/run_pipeline.py   CLI
tests/                    pytest
dags/                     Airflow (coming)
app/                      Streamlit (coming)
```

## Stack

Python, pandas, DuckDB, Faker and pytest so far. Ollama, Airflow and Streamlit come in the next phases.
