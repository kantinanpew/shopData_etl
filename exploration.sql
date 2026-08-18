-- check datas from databasefile

SELECT 'vw_raw_customers' AS source, COUNT(*)  AS rows FROM vw_raw_customers
UNION ALL SELECT 'vw_raw_orders', COUNT(*) FROM vw_raw_orders
UNION ALL SELECT 'vw_exchange_rates', COUNT(*) FROM vw_exchange_rates;

-- check duplicated customer id

SELECT customer_id, COUNT(*) as occurrences,
    GROUP_CONCAT(signup_date, '|' ) AS signup_date,
    GROUP_CONCAT(COALESCE(email,'NULL'),'|' ) AS emails
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) >1;
