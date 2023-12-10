CREATE VIEW campaign_sales AS
SELECT 
	"campaign transaction"."CAMPAIGN_ID",
    "campaign transaction"."ORDER_ID",
    sales."PRODUCT_ID",
    sales."TOTAL_AMOUNT",
    product."PRODUCT_TYPE"
FROM
	"campaign transaction"
INNER JOIN sales
	ON "campaign transaction"."ORDER_ID" = sales."ORDER_ID"
INNER JOIN product
    ON sales."PRODUCT_ID" = product."PRODUCT_ID";

-------------------------------------------------------------------------------------------

create view association_analysis as
select
    "CAMPAIGN_ID", "PRODUCT_TYPE", round(sum("TOTAL_AMOUNT")::numeric, 2) as total
from
    campaign_sales
group by
    "CAMPAIGN_ID", "PRODUCT_TYPE"
order by
    total desc;
