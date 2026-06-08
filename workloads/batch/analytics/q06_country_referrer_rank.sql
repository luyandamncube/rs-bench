-- workloads\clickstream\q06_country_referrer_rank.sql
WITH referrer_rollup AS (
    SELECT
        country_code,
        referrer_domain,
        COUNT(*) AS event_count,
        SUM(revenue) AS total_revenue
    FROM ${table_name}
    GROUP BY country_code, referrer_domain
),
ranked AS (
    SELECT
        country_code,
        referrer_domain,
        event_count,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY country_code
            ORDER BY total_revenue DESC, referrer_domain ASC
        ) AS revenue_rank
    FROM referrer_rollup
)
SELECT
    country_code,
    referrer_domain,
    event_count,
    total_revenue,
    revenue_rank
FROM ranked
WHERE revenue_rank <= 3
ORDER BY country_code ASC, revenue_rank ASC, total_revenue DESC, referrer_domain ASC;
