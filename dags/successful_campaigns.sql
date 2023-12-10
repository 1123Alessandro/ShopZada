DROP VIEW IF EXISTS successful_campaigns;

CREATE OR REPLACE VIEW successful_campaigns AS
SELECT
    campaign."CAMPAIGN_ID",
    campaign."CAMPAIGN_NAME",
    campaign."CAMPAIGN_DESCRIPTION",
    campaign."DISCOUNT_PERCENTAGE",
    EXTRACT(YEAR FROM date."DATE_ID") AS YEAR,
    EXTRACT(MONTH FROM date."DATE_ID") AS MONTH,
    COUNT("campaign transaction"."ORDER_ID") AS TOTAL_TRANSACTIONS,
    ROUND(SUM(sales."TOTAL_AMOUNT")::numeric, 2) AS TOTAL_SALES,
    ROUND(AVG(sales."TOTAL_AMOUNT")::numeric, 2) AS AVERAGE_TRANSACTION_AMOUNT
FROM
    campaign
JOIN
    "campaign transaction" ON campaign."CAMPAIGN_ID" = "campaign transaction"."CAMPAIGN_ID"
JOIN
    "sales" ON "campaign transaction"."ORDER_ID" = "sales"."ORDER_ID"
JOIN
    date ON "sales"."DATE_ID" = date."DATE_ID"
GROUP BY
    campaign."CAMPAIGN_ID", campaign."CAMPAIGN_NAME", campaign."CAMPAIGN_DESCRIPTION", campaign."DISCOUNT_PERCENTAGE", YEAR, MONTH
ORDER BY
    campaign."CAMPAIGN_ID", YEAR, MONTH;

select*from successful_campaigns