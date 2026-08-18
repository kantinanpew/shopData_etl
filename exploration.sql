---------CUSTOMERS
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
        WHEN phone GLOB '*[^0-9]*' THEN 'contains symbols/ spaces'
    END AS issue
FROM vw_raw_customers
WHERE phone IS NOT NULL AND phone GLOB '*[^0-9]*';


-------- ORDERS

-- check for non postive amounts
SELECT order_id, customer_id, order_date, total_amount, currency, status
FROM vw_raw_orders
WHERE total_amount <= 0 OR total_amount IS NULL;

-- check for missing currency
SELECT order_id, customer_id,order_date,total_amount,currency
FROM vw_raw_orders 
WHERE currency is NULL OR TRIM(currency) = '' OR order_date IS NULL;

-- check for nonexist customer id

SELECT o.order_id, o.customer_id, o.total_amount,o.currency 
FROM vw_raw_orders o
LEFT JOIN (SELECT DISTINCT customer_id from vw_raw_customers) c
    ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;

-- order status

SELECT status, COUNT(*) AS total, ROUND(SUM(total_amount),2) AS sum_amount
FROM vw_raw_orders
GROUP BY status;

