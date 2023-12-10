DROP VIEW IF EXISTS customer_profiling;

CREATE OR REPLACE VIEW customer_profiling AS
SELECT
    "customer"."CUSTOMER_GENDER",
    "customer"."CUSTOMER_JOB_TITLE",
    "customer"."CUSTOMER_JOB_LEVEL",
    "product"."PRODUCT_TYPE",
    COUNT(DISTINCT "sales"."ORDER_ID") AS "TOTAL_TRANSACTIONS",
    ROUND(SUM("sales"."TOTAL_AMOUNT")::numeric, 2) AS "TOTAL_AMOUNT_SPENT",
    ROUND(AVG("sales"."TOTAL_AMOUNT")::numeric, 2) AS "AVERAGE_AMOUNT_PER_TRANSACTION"
FROM
    "customer"
JOIN
    "sales" ON "customer"."CUSTOMER_ID" = "sales"."CUSTOMER_ID"
JOIN
    "product" ON "sales"."PRODUCT_ID" = "product"."PRODUCT_ID"
GROUP BY
   "customer"."CUSTOMER_GENDER",
    "customer"."CUSTOMER_JOB_TITLE",
    "customer"."CUSTOMER_JOB_LEVEL",
    "product"."PRODUCT_TYPE"
	
ORDER BY
    "TOTAL_TRANSACTIONS" DESC;

SELECT * FROM customer_profiling;
