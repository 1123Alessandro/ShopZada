DROP VIEW IF EXISTS customer_segmentation;

CREATE OR REPLACE VIEW customer_segmentation AS
SELECT
    "customer"."CUSTOMER_ID",
    "customer"."CUSTOMER_NAME",
    "customer"."CUSTOMER_GENDER",
    "customer"."CUSTOMER_JOB_TITLE",
    "customer"."CUSTOMER_JOB_LEVEL",
    "customer"."CUSTOMER_COUNTRY",
    COUNT(DISTINCT "sales"."ORDER_ID") AS "TOTAL_TRANSACTIONS",
    ROUND(SUM("sales"."TOTAL_AMOUNT")::numeric, 2) AS "TOTAL_AMOUNT_SPENT",
    ROUND(AVG("sales"."TOTAL_AMOUNT")::numeric, 2) AS "AVERAGE_AMOUNT_PER_TRANSACTION"
FROM
    "customer"
JOIN
    "sales" ON "customer"."CUSTOMER_ID" = "sales"."CUSTOMER_ID"
GROUP BY
    "customer"."CUSTOMER_ID",
    "customer"."CUSTOMER_NAME",
    "customer"."CUSTOMER_GENDER",
    "customer"."CUSTOMER_JOB_TITLE",
    "customer"."CUSTOMER_JOB_LEVEL",
    "customer"."CUSTOMER_COUNTRY"
ORDER BY
    "customer"."CUSTOMER_JOB_TITLE";