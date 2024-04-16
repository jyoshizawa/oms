/*
     Excelサンプル（FY22 SOH File.xlsx）「Weekly SOH」シートに相当
*/

select 
    DATADATE,
    CUSTOMER,
    trim(Supplier_ItemCode)     as Supplier_ItemCode,
    sum(ifnull(QTYON_HAND,0))   as weekly_QTYON_HAND,
    sum(ifnull(QTYON_ORDER,0))  as weekly_QTYON_ORDER
from
    {{ ref('stg_custmers_csv')}}
    -- IT_TEST_DB.DBT_4_YOSHI.STG_CUSTMERS_CSV
   group by
        UPLORAD_TIME,
        DATADATE,
        CUSTOMER,
        trim(Supplier_ItemCode)
