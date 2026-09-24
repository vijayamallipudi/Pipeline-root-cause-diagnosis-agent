"""Final layer (SQL).

loan_portfolio    - loans joined to customers, with risk band + delinquency flag
portfolio_summary - KPIs by loan type
"""

from __future__ import annotations

from pipeline_rca import config
from pipeline_rca.db import connect, row_count

LOAN_PORTFOLIO_SQL = f"""
CREATE OR REPLACE TABLE {config.FINAL}.loan_portfolio AS
SELECT
    l.loan_id,
    l.customer_id,
    c.state,
    c.credit_score,
    CASE
        WHEN c.credit_score IS NULL THEN 'UNKNOWN'
        WHEN c.credit_score >= 740   THEN 'PRIME'
        WHEN c.credit_score >= 670   THEN 'NEAR_PRIME'
        ELSE 'SUBPRIME'
    END                                                        AS risk_band,
    l.loan_type,
    l.source_system,
    l.principal,
    l.balance,
    l.interest_rate,
    l.term_months,
    l.status,
    l.status IN ('DELINQUENT_30', 'DELINQUENT_90', 'DEFAULT')  AS is_delinquent,
    CAST(l.origination_date AS DATE)                           AS origination_date
FROM {config.STAGING}.loans AS l
INNER JOIN {config.STAGING}.customers AS c
    ON l.customer_id = c.customer_id
"""

PORTFOLIO_SUMMARY_SQL = f"""
CREATE OR REPLACE TABLE {config.FINAL}.portfolio_summary AS
SELECT
    loan_type,
    COUNT(*)                                   AS loan_count,
    ROUND(SUM(balance), 2)                     AS total_balance,
    ROUND(AVG(interest_rate), 4)               AS avg_interest_rate,
    ROUND(AVG(is_delinquent::INT), 4)          AS delinquency_rate,
    ROUND(AVG((risk_band = 'SUBPRIME')::INT), 4) AS subprime_share
FROM {config.FINAL}.loan_portfolio
GROUP BY loan_type
ORDER BY loan_type
"""


def build_final(db_path=None) -> dict[str, int]:
    with connect(db_path) as con:
        con.execute(LOAN_PORTFOLIO_SQL)
        con.execute(PORTFOLIO_SUMMARY_SQL)
        return {
            "loan_portfolio": row_count(con, config.FINAL, "loan_portfolio"),
            "portfolio_summary": row_count(con, config.FINAL, "portfolio_summary"),
        }
