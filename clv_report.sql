SELECT
    c.customer_id,
    c.full_name,
    COUNT(o.order_id) AS total_orders_placed,
    ROUND(COALESCE(SUM(o.usd_amount), 0), 2) AS lifetime_value_usd,
    strftime('%Y-%m', c.signup_date) AS customer_cohort
FROM dim_customers AS c
LEFT JOIN fct_orders AS o
       ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name, c.signup_date
ORDER BY lifetime_value_usd DESC, c.customer_id;