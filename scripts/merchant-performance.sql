create view successful_merchants as
select
    m."MERCHANT_ID",
    m."STAFF_ID",
    sum(sa."TOTAL_AMOUNT") as total_sales
from
    "merchant performance" m
inner join sales sa
    on m."ORDER_ID" = sa."ORDER_ID"
group by
    m."MERCHANT_ID", m."STAFF_ID"
order by
    total_sales desc

-------------------------------------------------------------------------------------------

create view staff_analysis as
select
    sm.*,
    s."STAFF_JOB_LEVEL"
from
    successful_merchants sm
inner join staff s
    on sm."STAFF_ID" = s."STAFF_ID"
order by sm.total_sales desc;

-------------------------------------------------------------------------------------------

create view delay_analysis as
select
    sm."MERCHANT_ID",
    sum(sm.total_sales) as sales,
    count(s."DELAY_IN_DAYS") as total_delays
from
    successful_merchants sm
inner join sales s
    on sm."MERCHANT_ID" = s."MERCHANT_ID"
group by
    sm."MERCHANT_ID"
order by
    total_delays desc;
