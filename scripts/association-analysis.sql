CREATE VIEW base_campaign_sales AS
SELECT 
	"campaign transaction"."CAMPAIGN_ID",
    c."CAMPAIGN_NAME",
    "campaign transaction"."ORDER_ID",
    sales."PRODUCT_ID",
    sales."TOTAL_AMOUNT",
    product."PRODUCT_TYPE"
FROM
	"campaign transaction"
INNER JOIN sales
	ON "campaign transaction"."ORDER_ID" = sales."ORDER_ID"
INNER JOIN product
    ON sales."PRODUCT_ID" = product."PRODUCT_ID"
inner join campaign c
    on "campaign transaction"."CAMPAIGN_ID" = c."CAMPAIGN_ID";

-------------------------------------------------------------------------------------------

create view total_campaign_sales as
select
    "CAMPAIGN_NAME", "PRODUCT_TYPE", round(sum("TOTAL_AMOUNT")::numeric, 2) as total
from
    base_campaign_sales
group by
    "CAMPAIGN_NAME", "PRODUCT_TYPE"
order by
    total desc;
