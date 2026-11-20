-- Input relations: ISO date strings and one report_period row; end is exclusive.
WITH totals AS (
    SELECT department,
           SUM(CASE WHEN date < current_start THEN admissions ELSE 0 END) AS previous_admissions,
           SUM(CASE WHEN date >= current_start THEN admissions ELSE 0 END) AS current_admissions
    FROM department_daily_metrics CROSS JOIN report_period
    WHERE date >= previous_start AND date < current_end
    GROUP BY department
)
SELECT department, previous_admissions, current_admissions,
       current_admissions - previous_admissions AS delta,
       CASE WHEN previous_admissions = 0 THEN NULL
            ELSE ROUND(100.0 * CAST(current_admissions - previous_admissions AS DOUBLE) / previous_admissions, 2)
       END AS change_pct
FROM totals
ORDER BY department;
