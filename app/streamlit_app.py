"""Demo UI: pick a failure, run the pipeline, watch it get diagnosed and explained.

    streamlit run app/streamlit_app.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_rca import config  # noqa: E402
from pipeline_rca.chaos import list_scenarios  # noqa: E402
from pipeline_rca.db import connect  # noqa: E402
from pipeline_rca.diagnostics import DiagnosticReport, diagnose, load_report  # noqa: E402
from pipeline_rca.diagnostics.contracts import TRANSITIONS  # noqa: E402
from pipeline_rca.llm import client, generate_report  # noqa: E402
from pipeline_rca.pipeline.runner import finish_run, run_stage, start_run  # noqa: E402

st.set_page_config(page_title="Pipeline Root-Cause Diagnosis", page_icon="🔎", layout="wide")

SCENARIOS = {s["scenario"]: s for s in list_scenarios()}
SOURCES = ["CORE_BANKING", "CARD_PLATFORM", "MORTGAGE_SVC"]
# validated categorical palette (light / dark), one fixed color per source system
SOURCE_COLORS = {"light": ["#2a78d6", "#eb6834", "#1baf7a"], "dark": ["#3987e5", "#d95926", "#199e70"]}
LAYERS = ["raw", "staging", "final"]
LOAN_TABLE = {"raw": "raw.loans", "staging": "staging.loans", "final": "final.loan_portfolio"}


# ---------------------------------------------------------------- data access

def query(sql: str, params=None) -> pd.DataFrame:
    try:
        with connect() as con:
            return con.execute(sql, params or []).df()
    except duckdb.CatalogException:  # tables don't exist yet
        return pd.DataFrame()


def past_runs() -> pd.DataFrame:
    return query(f"""
        SELECT r.run_id, r.scenario, r.started_at, d.status, d.root_cause
        FROM {config.META}.pipeline_runs r
        LEFT JOIN {config.META}.diagnostic_reports d USING (run_id)
        WHERE d.status IS NOT NULL
        ORDER BY r.started_at DESC LIMIT 20
    """)


def snapshots(run_id: str) -> dict[str, dict]:
    df = query(f"SELECT layer, table_name, profile FROM {config.META}.layer_snapshots WHERE run_id = ?", [run_id])
    return {f"{r.layer}.{r.table_name}": json.loads(r.profile) for r in df.itertuples()}


def saved_rca(run_id: str) -> dict | None:
    df = query(f"SELECT * FROM {config.META}.rca_reports WHERE run_id = ?", [run_id])
    return None if df.empty else df.iloc[0].to_dict()


def balance_by_source() -> pd.DataFrame:
    # raw balance may be text (type_change) - only count what's readable as a number
    parts = [
        f"SELECT '{layer}' AS layer, source_system, COUNT(*) AS loans, "
        f"COALESCE(SUM(TRY_CAST(balance AS DOUBLE)), 0) AS balance FROM {table} GROUP BY 2"
        for layer, table in LOAN_TABLE.items()
    ]
    return query(" UNION ALL ".join(parts))


def run_scenario(run_id: str) -> str:
    df = query(f"SELECT scenario FROM {config.META}.pipeline_runs WHERE run_id = ?", [run_id])
    return "none" if df.empty else df.iloc[0].scenario


def is_latest(run_id: str) -> bool:
    runs = query(f"SELECT run_id FROM {config.META}.pipeline_runs ORDER BY started_at DESC LIMIT 1")
    return not runs.empty and runs.iloc[0].run_id == run_id


def theme() -> str:
    try:
        return st.context.theme.type or "light"
    except AttributeError:
        return "light"


# ---------------------------------------------------------------- running

def run_pipeline_with_progress(scenario: str) -> str:
    with st.status("Running the pipeline...", expanded=True) as status:
        run_id = start_run(scenario)
        for stage, label in (
            ("extract", "Landing source data in **raw**"),
            ("staging", "Cleaning it into **staging**"),
            ("final", "Building the **final** tables"),
        ):
            st.write(label)
            run_stage(stage, run_id)
        finish_run(run_id)
        st.write("Running diagnostic checks")
        report = diagnose(run_id=run_id)
        healthy = report.status == "HEALTHY"
        status.update(
            label="Pipeline ran - all checks passed" if healthy else "Pipeline ran - the checker found a problem in the data",
            state="complete" if healthy else "error",
            expanded=False,
        )
    return run_id


# ---------------------------------------------------------------- rendering

def render_flow(report: DiagnosticReport, snaps: dict) -> None:
    """source -> raw -> staging -> final, with each arrow marked by what happened there."""
    root_t = report.root_cause.transition if report.root_cause else None
    symptom_t = {f.transition for f in report.symptoms}

    def arrow(transition: str) -> str:
        if transition == root_t:
            return "🔴 <b>root cause</b>"
        if transition in symptom_t:
            return "🟠 symptom"
        return "🟢 ok"

    cols = st.columns([1.1, 0.8, 1.3, 0.8, 1.3, 0.8, 1.3])
    cols[0].markdown("**Source systems**")
    cols[0].caption("CORE_BANKING · CARD_PLATFORM · MORTGAGE_SVC")

    prev_rows = None
    for i, layer in enumerate(LAYERS):
        cols[1 + i * 2].markdown(f"<div style='text-align:center;padding-top:14px'>→<br>{arrow(TRANSITIONS[i])}</div>",
                                 unsafe_allow_html=True)
        snap = snaps.get(LOAN_TABLE[layer], {})
        rows = snap.get("row_count")
        bal = (snap.get("columns", {}).get("balance") or {}).get("sum")
        box = cols[2 + i * 2]
        change = None if prev_rows is None or rows is None else rows - prev_rows
        delta = None if change is None else ("no change" if change == 0 else f"{change:+,} rows")
        box.metric(f"{layer.title()} loans", f"{rows:,}" if rows is not None else "-", delta, delta_color="off",
                   delta_arrow="off" if change == 0 else "auto")
        box.caption(f"balance ${bal / 1e6:,.1f}M" if bal else "balance: not numeric")
        prev_rows = rows

    st.caption("Arrows: 🟢 nothing wrong here · 🟠 knock-on effect of the problem · 🔴 where the problem started")


CHECK_HELP = {
    "schema": "Is every column the right kind of data - numbers where there should be numbers?",
    "key_integrity": "Did any customer IDs change between raw and staging? Changed IDs can't be matched to customers.",
    "uniqueness": "Is any loan listed more than once?",
    "nulls": "Are too many values missing (blank)?",
    "row_counts": "Did loans go missing or appear between steps without a business reason?",
    "measures": "Does the total amount owed add up from one step to the next?",
    "distribution": "Are too many loans ending up with an UNKNOWN risk label?",
}


def render_checks(report: DiagnosticReport) -> None:
    st.markdown("**The 7 checks** (hover the ⓘ to see what each one looks for)")
    cols = st.columns(len(report.checks))
    for col, (name, result) in zip(cols, report.checks.items()):
        col.markdown(f"{'✅' if result == 'PASS' else '❌'} **{name.replace('_', ' ')}**  \n"
                     f"<span style='opacity:.7'>{result}</span>", unsafe_allow_html=True,
                     help=CHECK_HELP.get(name))


def render_guide(expanded: bool) -> None:
    with st.expander("👋 New here? How to use this page", expanded=expanded):
        st.markdown(
            "**What this is:** a bank's loan data goes through 3 steps - **raw** (as delivered), **staging** "
            "(cleaned) and **final** (ready for reports). Sometimes something goes wrong on the way and the "
            "numbers in the reports end up wrong without anyone noticing. This page checks every step and, "
            "if something's off, tells you what went wrong, where, and how to fix it.\n\n"
            "**How to use it:**\n"
            "1. In the sidebar, pick a problem to cause on purpose (or **Healthy run** for no problem).\n"
            "2. Click **Run pipeline**.\n"
            "3. Look at the banner and the arrows - a 🔴 marks where the problem started.\n"
            "4. Open the tabs: **AI report** (plain-English explanation), **Diagnosis** (the clues), "
            "**Data** (see it in a chart) and **Answer key** (proof the checker got it right).\n\n"
            "**Red doesn't mean the page is broken.** Think of a smoke alarm: if you break something on "
            "purpose and the alarm goes off, it's working. It would only be bad if it stayed quiet."
        )


EVIDENCE_LABELS = {
    "affected_segment": "Affected segment",
    "pattern": "Pattern",
    "examples": "Examples",
    "sample_values": "Sample values",
    "origination_date_range": "Origination dates of duplicates",
    "note": "Note",
    "persists_in": "Also shows up in",
    "loan_types_missing_from_final": "Loan types missing from final",
}


def render_diagnosis(report: DiagnosticReport) -> None:
    rc = report.root_cause
    if rc is None:
        st.success("No problems found. Row counts, types, keys, nulls and balances all reconcile.", icon="✅")
    else:
        st.markdown(f"#### Root cause · `{rc.category}`")
        st.markdown(f"**{rc.message}**  \nStarted at `{rc.transition}` in `{rc.table}"
                    f"{'.' + rc.column if rc.column else ''}`")
        for key, label in EVIDENCE_LABELS.items():
            val = rc.evidence.get(key)
            if val:
                st.markdown(f"- **{label}:** {', '.join(map(str, val)) if isinstance(val, list) else val}")

        if report.symptoms:
            st.markdown("#### Downstream symptoms")
            for f in report.symptoms:
                st.markdown(f"- `{f.category}` at `{f.transition}`: {f.message}")

    if report.expected_changes:
        st.markdown("#### Expected changes (not problems)")
        for f in report.expected_changes:
            st.markdown(f"- {f.message}")

    with st.expander("Full diagnostic output (JSON)"):
        st.json(report.for_llm(), expanded=False)


def render_ai_report(report: DiagnosticReport, model: str, generate_now: bool) -> None:
    saved = saved_rca(report.run_id)

    if saved is None and not generate_now:
        st.info("No report written for this run yet.")
        if st.button(f"Write the report with {model}", type="primary"):
            st.session_state.generate_for = report.run_id
            st.rerun()
        return

    if saved is None:
        if report.root_cause is not None and client.is_available(model):
            st.caption(f"Writing with {model} on this machine - usually 1-2 minutes on CPU.")
        box = st.empty()
        streamed: list[str] = []

        def on_token(chunk: str) -> None:
            streamed.append(chunk)
            box.markdown("".join(streamed) + " ▌")

        with st.spinner("Writing the root-cause report..."):
            rca = generate_report(report, model=model, on_token=on_token)
        box.empty()
        saved = saved_rca(report.run_id) or {"markdown": rca.markdown, "source": rca.source, "note": rca.note,
                                               "model": rca.model, "seconds": rca.seconds}

    by = f"ollama · {saved['model']} · {saved['seconds']:.0f}s" if saved["source"] == "llm" else "template"
    st.caption(f"Written by: {by}")
    if saved.get("note"):
        st.caption(f"Note: {saved['note']}")
    st.markdown(saved["markdown"])
    st.download_button("Download report (.md)", saved["markdown"], file_name=f"rca_{report.run_id}.md")


def render_data(run_id: str) -> None:
    if not is_latest(run_id):
        st.info("The tables only hold the most recent run. Run the pipeline again to explore this one.")
        return

    df = balance_by_source()
    if df.empty:
        return
    df["balance_m"] = df["balance"] / 1e6

    st.markdown("#### Outstanding balance by source system, per layer")
    st.caption("Only balances readable as numbers are counted. A bar that shrinks or grows between "
               "layers is where money went missing or got double counted.")
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("layer:N", sort=LAYERS, title=None, axis=alt.Axis(labelAngle=0)),
            xOffset=alt.XOffset("source_system:N", sort=SOURCES, scale=alt.Scale(paddingInner=0.08)),
            y=alt.Y("balance_m:Q", title="Balance ($M)", axis=alt.Axis(gridOpacity=0.35, domain=False)),
            color=alt.Color("source_system:N", title="Source system",
                            scale=alt.Scale(domain=SOURCES, range=SOURCE_COLORS[theme()]),
                            legend=alt.Legend(orient="top")),
            tooltip=[
                alt.Tooltip("layer:N", title="Layer"),
                alt.Tooltip("source_system:N", title="Source"),
                alt.Tooltip("balance:Q", title="Balance", format="$,.0f"),
                alt.Tooltip("loans:Q", title="Loans", format=","),
            ],
        )
        .properties(height=320)
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, width="stretch")
    with st.expander("Show as table"):
        table = df.pivot(index="source_system", columns="layer", values="balance")[LAYERS].reindex(SOURCES)
        st.dataframe(table.style.format("${:,.0f}"), width="stretch")

    st.markdown("#### Browse a table")
    snaps = snapshots(run_id)
    name = st.selectbox("Table", list(snaps), index=list(snaps).index("final.loan_portfolio"))
    left, right = st.columns([3, 2])
    left.caption("First 100 rows")
    left.dataframe(query(f"SELECT * FROM {name} LIMIT 100"), width="stretch", height=360)
    right.caption("Column profile from the snapshot")
    prof = pd.DataFrame(snaps[name]["columns"].values())[["name", "dtype", "null_pct", "distinct_count", "min", "max"]]
    for col in ("min", "max"):  # mix of numbers and dates - show as text
        prof[col] = prof[col].map(lambda v: "" if v is None else (f"{v:,.2f}" if isinstance(v, float) else str(v)))
    right.dataframe(prof, width="stretch", height=360, hide_index=True)


def render_answer_key(report: DiagnosticReport, run_id: str) -> None:
    ev = report.evaluation
    truth = query(f"SELECT ground_truth FROM {config.META}.pipeline_runs WHERE run_id = ?", [run_id])
    gt = json.loads(truth.iloc[0].ground_truth) if not truth.empty and truth.iloc[0].ground_truth else {}

    st.caption("The chaos injector writes down exactly what it broke. The diagnosis never sees this - "
               "it's only used here to check the diagnosis was right.")
    if not gt:
        st.markdown("Nothing was injected on this run.")
        st.markdown(f"**Diagnosis said:** {report.status} → " + ("✅ correct" if ev.get("correct") else "❌ wrong"))
        return

    left, right = st.columns(2)
    left.markdown("**What was actually broken**")
    left.markdown(f"- Scenario: `{gt['scenario']}`\n- Injected at: `{gt['injected_at']}`\n"
                  f"- Rows affected: {gt.get('rows_affected', 0):,}\n\n{gt['description']}")
    right.markdown("**What the diagnosis found**")
    right.markdown(f"- Diagnosed as: `{ev.get('diagnosed_as')}` {'✅' if ev.get('scenario_correct') else '❌'}\n"
                   f"- Location: `{ev.get('location_found')}` {'✅' if ev.get('location_correct') else '❌'}")
    (st.success if ev.get("correct") else st.error)(
        "Diagnosis matches what was injected." if ev.get("correct") else "Diagnosis did not match.")


# ---------------------------------------------------------------- page

def sidebar() -> tuple[str, bool, str, bool]:
    st.sidebar.header("Break something")
    scenario = st.sidebar.selectbox(
        "Failure to inject",
        list(SCENARIOS),
        format_func=lambda s: SCENARIOS[s]["title"],
    )
    st.sidebar.caption(SCENARIOS[scenario]["description"])

    models = [m for m in client.available_models() if "embed" not in m]
    if models:
        # exact name first, then "<name>:latest" - so "llama3.2" doesn't land on "llama3.2:1b"
        wanted = [config.OLLAMA_MODEL, f"{config.OLLAMA_MODEL}:latest"]
        default = next((models.index(w) for w in wanted if w in models), 0)
        model = st.sidebar.selectbox("Local model (Ollama)", models, index=default)
        write_now = st.sidebar.toggle("Write the AI report right away", value=True)
    else:
        model = config.OLLAMA_MODEL
        write_now = False
        st.sidebar.warning("Ollama isn't running, so reports will use the built-in template.", icon="⚠️")

    run_clicked = st.sidebar.button("Run pipeline", type="primary", width="stretch")

    return scenario, run_clicked, model, write_now


def sidebar_history() -> None:
    """Drawn after any new run so the list (and selection) includes it."""
    runs = past_runs()
    if runs.empty:
        return
    st.sidebar.divider()
    st.sidebar.subheader("Past runs")
    labels = {
        r.run_id: f"{r.started_at:%b %d %H:%M} · {SCENARIOS.get(r.scenario, {}).get('title', r.scenario)} · "
                  f"{'✅ healthy' if r.status == 'HEALTHY' else '🚨 problem found'}"
        for r in runs.itertuples()
    }
    current = st.session_state.get("run_id")
    picked = st.sidebar.radio("Open a previous run", list(labels), format_func=labels.get,
                              index=list(labels).index(current) if current in labels else 0,
                              key=f"history_{current}", label_visibility="collapsed")
    st.session_state.run_id = picked


def main() -> None:
    st.title("🔎 Pipeline Root-Cause Diagnosis")
    st.markdown("A loan pipeline moves data **raw → staging → final**. Pick a failure, run it, and see where it "
                "broke, why, and an incident note written by a local LLM.")

    selected, run_clicked, model, write_now = sidebar()
    render_guide(expanded=not st.session_state.get("run_id") and not run_clicked and past_runs().empty)

    if run_clicked:
        run_id = run_pipeline_with_progress(selected)
        st.session_state.run_id = run_id
        if write_now:
            st.session_state.generate_for = run_id

    sidebar_history()
    run_id = st.session_state.get("run_id")
    if not run_id:
        st.info("Pick a failure in the sidebar and click **Run pipeline** to start.", icon="👈")
        return

    try:
        report = load_report(run_id)
    except KeyError:
        st.warning("That run hasn't been diagnosed.")
        return

    shown = run_scenario(run_id)
    st.subheader(f"Showing: {SCENARIOS.get(shown, {}).get('title', shown)}")
    if selected != shown:
        st.info(f"You picked **{SCENARIOS[selected]['title']}** but haven't run it yet. Click **Run pipeline** "
                f"in the sidebar - below are the results for **{SCENARIOS.get(shown, {}).get('title', shown)}**.",
                icon="👈")

    if report.root_cause:
        rc = report.root_cause
        caught = " The checker caught the problem you injected - see the Answer key tab." \
            if report.evaluation.get("correct") and shown != "none" else ""
        st.error(f"**Problem detected in the data** - root cause: `{rc.category}` at `{rc.transition}` in "
                 f"`{rc.table}{'.' + rc.column if rc.column else ''}`.{caught}", icon="🚨")
    else:
        st.success("**HEALTHY** - every check passed, the numbers can be trusted", icon="✅")

    render_flow(report, snapshots(run_id))
    render_checks(report)
    st.divider()

    generate_now = st.session_state.get("generate_for") == run_id
    tab_report, tab_diag, tab_data, tab_key = st.tabs(["📝 AI report", "🔬 Diagnosis", "📊 Data", "🗝️ Answer key"])
    with tab_report:
        render_ai_report(report, model, generate_now)
        if generate_now:
            st.session_state.generate_for = None
    with tab_diag:
        render_diagnosis(report)
    with tab_data:
        render_data(run_id)
    with tab_key:
        render_answer_key(report, run_id)


def db_is_busy(error: Exception) -> bool:
    """DuckDB allows one process per file - true when the CLI or Airflow has it open."""
    msg = str(error).lower()
    return "being used by another process" in msg or "lock" in msg  # windows / linux wording


try:
    main()
except duckdb.IOException as e:
    if not db_is_busy(e):
        raise
    st.warning("The database is busy - another run (the CLI or Airflow) is using it right now. "
               "DuckDB only lets one program use the file at a time. Wait for it to finish and try again.",
               icon="⏳")
    st.button("Try again")
