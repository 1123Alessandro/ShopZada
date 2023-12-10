CREATE OR REPLACE VIEW products_top_performers AS
  
SELECT "PRODUCT_TYPE", "PRODUCT_NAME", ROUND(SUM("TOTAL_AMOUNT"):: numeric, 2) AS "TOTAL_SALES"
FROM product
inner join sales on product."PRODUCT_ID" = sales."PRODUCT_ID"
GROUP BY "PRODUCT_TYPE", "PRODUCT_NAME"
ORDER BY "TOTAL_SALES" DESC;

------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW type_top_performer AS
  SELECT "PRODUCT_TYPE", "PRODUCT_NAME", "TOTAL_SALES"
  FROM (
    SELECT
      "PRODUCT_TYPE",
      "PRODUCT_NAME",
      ROUND(SUM("TOTAL_AMOUNT")::numeric, 2) AS "TOTAL_SALES",
      ROW_NUMBER() OVER (PARTITION BY "PRODUCT_TYPE" ORDER BY SUM("TOTAL_AMOUNT") DESC) AS rnk
    FROM product
    INNER JOIN sales ON product."PRODUCT_ID" = sales."PRODUCT_ID"
    GROUP BY "PRODUCT_TYPE", "PRODUCT_NAME"
  ) ranked_products
  WHERE rnk = 1;