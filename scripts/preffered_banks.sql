CREATE OR REPLACE VIEW preffered_banks_customers AS

SELECT "CUSTOMER_CREDIT_CARD_BANK", COUNT(*) AS "COUNT" 
FROM customer 
GROUP BY "CUSTOMER_CREDIT_CARD_BANK" 
ORDER BY "COUNT" DESC;

--------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW preffered_banks_sales AS
  
SELECT "CUSTOMER_CREDIT_CARD_BANK", COUNT(*) AS "COUNT" 
FROM customer 
inner join sales on customer."CUSTOMER_ID" = sales."CUSTOMER_ID"
GROUP BY "CUSTOMER_CREDIT_CARD_BANK" 
ORDER BY "COUNT" DESC;