-- check datas from databasefile

SELECT 'vw_raw_customers' AS source, COUNT(*)  AS rows FROM vw_raw_customers
UNION ALL SELECT 'vw_raw_orders', COUNT(*) FROM vw_raw_orders
UNION ALL SELECT 'vw_exchange_rates', COUNT(*) FROM vw_exchange_rates;

-- check duplicated customer

SELECT customer_id, COUNT(*) as occurrences,
    GROUP_CONCAT(signup_date, '|' ) AS signup_date,
    GROUP_CONCAT(COALESCE(email,'NULL'),'|' ) AS emails
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) >1;

-- check missing contact info Null etc...

SELECT customer_id, full_name, email, phone
FROM vw_raw_customers
WHERE  email IS NULL OR TRIM(email) = ''
OR email IS NULL OR TRIM(phone) = '';

-- check the phone no. formats

SELECT customer_id, phone,
    CASE 
        WHEN phone GLOB '*[A-Za-z]*' THEN 'contains letters'
        WHEN phone GLOB '*[^0-9]' THEN 'contains symbols/ spaces'
    END AS issue
FROM vw_raw_customers
WHERE phone IS NOT NULL AND phone GLOB '*[^0-9]';