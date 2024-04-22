/*
     Excelサンプル（FY22 SOH File.xlsx）「TEDS SELL」シートに相当
*/

select 
    DATA_DATE,
    CUSTOMER,
    trim(Supplier_ItemCode) as Supplier_ItemCode,
    -- sum(ifnull(Q_VALUE,0))    as weekly_VALUE,
    -- sum(ifnull(q_COST,0))     as weekly_COST
    ifnull(Q_VALUE,0)       as weekly_VALUE,
    ifnull(q_COST,0)        as weekly_COST,
    ifnull(QUANTITY,0)      as weekly_QUANTITY
from
    {{ ref('stg_custmers_sell_csv')}}
-- group by
--     UPLORAD_TIME,
--     DATA_DATE,
--     CUSTOMER,
--     trim(Supplier_ItemCode)


