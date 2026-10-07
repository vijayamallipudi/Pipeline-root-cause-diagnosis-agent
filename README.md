# Pipeline Root-Cause Diagnosis Agent

A lot of my time as a data engineer goes into answering one question: "why doesn't this number match?" Usually the answer is buried somewhere between the source extract and the final table. A column changed type upstream, a join started dropping rows, a load ran twice. Finding it means going layer by layer and comparing row counts, schemas and totals until something doesn't line up.

This project automates that process. It runs a small loan pipeline (raw -> staging -> final) in DuckDB, breaks it on purpose, and then traces where things went wrong. A local LLM then writes up the root cause in plain English.

Everything runs locally and costs nothing. No cloud services, no API keys. The LLM part uses Ollama.

## Progress

- [x] Phase 1: synthetic data, 3-layer pipeline, chaos injector
- [x] Phase 2: diagnostic engine (snapshots and diffs per layer)
- [x] Phase 3: root-cause report via Ollama
- [x] Phase 4: Airflow DAG
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

## AI root-cause report

The last step turns the diagnosis into an incident note that someone who wasn't debugging the pipeline could read. It uses a local model through Ollama (`llama3.2` by default), so nothing leaves the machine and there's no API bill.

I didn't want the model doing the investigating. Small models will happily make up a cause if you hand them a pile of JSON. So the diagnostic engine finds the answer, and the model only explains it:

- The findings are flattened into short plain-text facts: root cause, symptoms, and the expected changes it shouldn't flag.
- Fix suggestions come from a small playbook for each failure type, so the "recommended fix" section is based on what you'd actually do.
- The ground truth from the chaos injector is never included in the prompt.
- If the output doesn't mention the actual table and column that broke, it's thrown out and a template report is used instead. If it skips a section, that section gets filled from the template.
- If Ollama isn't running, you still get the template report. The pipeline never fails because of the LLM.
- Every report ends with the raw evidence from the checks, so you can verify what the model wrote.

Reports go to `data/reports/<run_id>.md` and `meta.rca_reports`. On my laptop (CPU only), `llama3.2` takes about 1–2 minutes per report. A bigger model like `llama3.1:8b` writes better notes but is slower.

## Airflow

The DAG in `dags/pipeline_rca_dag.py` runs the whole thing as separate tasks:

```
start_run -> extract_to_raw -> transform_staging -> build_final
          -> run_diagnostics -> generate_rca_report -> quality_gate
```

You trigger it with a `scenario` param (`none` for a clean run) and optionally a `model` for the report.

Some decisions that went into it:

- **The DAG file is thin.** Each task calls a plain function in `pipeline_rca/tasks.py`, so all the logic can be run and tested without Airflow. The tests in `tests/test_tasks.py` call those functions in DAG order, passing only the `run_id` between them like XCom would.
- **No shared memory between tasks.** Each task runs in its own process, so all run state (scenario, row counts, what chaos was injected) lives in `meta.pipeline_runs`. I had to refactor the runner for this. Originally it kept that state in memory for the whole run.
- **The quality gate runs last, after the report.** When a run fails, the DAG still goes red, but the incident report is already written. I'd rather have the explanation waiting than a failed task and nothing else.
- **`max_active_runs=1`**, because DuckDB only allows one writer at a time.
- **Every layer remembers which run wrote it.** Splitting the stages into tasks exposed a bug: staging for a new run could quietly clean the *previous* run's raw data if its own extract hadn't run. Now a stage refuses to read another run's layer and says what to run first.
- **Retries are set per task.** Pipeline stages retry once, which is safe because a stage can re-run for the same run. The report task gets a time limit longer than Ollama's own timeout, so a slow model falls back to the template instead of being killed first. The quality gate never retries, because bad data doesn't fix itself and retrying would only delay the alert.

`scripts/airflow_setup.sh` installs Airflow in its own venv with the pinned constraints file and points it at this repo's `dags/` folder. It runs on Linux, Mac, or WSL on Windows:

```bash
bash scripts/airflow_setup.sh          # one-time install
bash scripts/airflow_setup.sh start    # then open http://localhost:8080
```

**How it's tested.** I couldn't run Airflow on my own laptop. Airflow doesn't run natively on Windows, and installing WSL needs admin rights I don't have on that machine. So:

- the task logic is plain Python in `tasks.py`, and the tests run it in DAG order
- the DAG file itself is loaded in the tests against a small stand-in for `airflow.sdk`, which checks task order, retry settings and params. I broke a dependency on purpose to make sure that test catches it.
- the next step is CI on a Linux runner that runs the DAG with real Airflow

If Ollama runs on Windows, set `networkingMode=mirrored` in `%UserProfile%\.wslconfig` so WSL can reach it on localhost. If Airflow can't reach Ollama, the reports fall back to the template.

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

For the AI write-up, install [Ollama](https://ollama.com), pull a model and add `--explain`:

```bash
ollama pull llama3.2
python scripts/run_pipeline.py --scenario type_change --explain
python scripts/run_pipeline.py --scenario type_change --explain --model llama3.1:8b   # any model you have
```

`--json --explain` puts the report in the JSON output under `rca_report`.

If you want to run this from cron or CI, `--fail-on-problem` makes the command exit with code 1 when diagnostics find something, so whatever is scheduling it can alert on it. It's off by default.

The database ends up at `data/pipeline.duckdb` if you want to poke around in it.

You can override a few settings with environment variables: `RCA_DB_PATH`, `RCA_SEED` (default 42), `RCA_N_CUSTOMERS` (2000), `RCA_N_LOANS` (5000), `RCA_OLLAMA_MODEL` (llama3.2) and `OLLAMA_HOST` (http://localhost:11434).

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
    llm/
        client.py         minimal ollama client (urllib, no extra deps)
        prompts.py        facts + fix playbook -> prompt
        report.py         writes the report, grounding check, template fallback
    tasks.py              one function per airflow task
scripts/run_pipeline.py   CLI
scripts/airflow_setup.sh  airflow install + start (Linux/Mac/WSL)
tests/                    pytest
dags/pipeline_rca_dag.py  Airflow DAG
app/                      Streamlit (coming)
```

## Stack

Python, pandas, DuckDB, Faker, Ollama, Airflow and pytest so far. Streamlit comes next.
