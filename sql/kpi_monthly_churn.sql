-- Deliverable 4: Monthly Churn Rate KPI
--
WITH churned_by_month AS (
    SELECT churn_month            AS mth,
           COUNT(*)               AS churned_customers
    FROM   churn_dw.dim_customer
    WHERE  is_current  = 1
      AND  is_churned  = 1
      AND  churn_month IS NOT NULL
    GROUP BY churn_month
),
totals AS (
    SELECT COUNT(*) AS total_customers
    FROM   churn_dw.dim_customer
    WHERE  is_current = 1
),
cumulative AS (
    SELECT mth,
           churned_customers,
           COALESCE(
             SUM(churned_customers) OVER (
               ORDER BY mth
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ), 0
           ) AS churned_before
    FROM churned_by_month
)
SELECT c.mth                                        AS churn_month,
       c.churned_customers,
       t.total_customers - c.churned_before         AS active_at_start,
       ROUND(100.0 * c.churned_customers
             / (t.total_customers - c.churned_before), 2)
                                                    AS monthly_churn_rate_pct
FROM       cumulative c
CROSS JOIN totals     t
ORDER BY   c.mth;
