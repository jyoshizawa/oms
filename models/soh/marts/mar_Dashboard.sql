with w_soh as (
    select 
        DATADATE as DATA_DATE,
        CUSTOMER,
        Supplier_ItemCode,
        weekly_QTYON_HAND,
        weekly_QTYON_ORDER
    from
        {{ ref ('mar_weekly_soh')}}
),
 
w_sell as (
    select 
        DATA_DATE,
        CUSTOMER,
        Supplier_ItemCode,
        weekly_VALUE,
        weekly_COST,
        weekly_quantity
    from
        {{ ref ('mar_weekly_sell')}}
)

select 
    t1.DATA_DATE,
    t1.CUSTOMER,
    t1.Supplier_ItemCode,
    t1.weekly_QTYON_HAND,
    t1.weekly_QTYON_ORDER,
    ifnull(t2.weekly_VALUE, 0 )     as weekly_VALUE,
    ifnull(t2.weekly_COST, 0 )      as weekly_COST,
    ifnull(t2.weekly_quantity, 0 )  as weekly_quantity
from 
    w_soh as t1
left join
    w_sell as t2
on t1.DATA_DATE = t2.DATA_DATE
    and t1.CUSTOMER = t2.CUSTOMER
    and t1.Supplier_ItemCode = t2.Supplier_ItemCode

-- where
--      t1.SUPPLIER_ITEMCODE  = 'V314060BG000'

order by 1,2,3