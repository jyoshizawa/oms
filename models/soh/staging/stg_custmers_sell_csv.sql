/*
  csv ロードテーブルのうち、重複をのぞいた計算対象データ
*/

with wk_upload as (
    select
        DATA_DATE,
        CUSTOMER,
        max(UPLORAD_TIME) as M_UPLORAD_TIME
    from 
        {{ source('soh', 'test_cust_sell_data') }}
    where 
        DATA_DATE is not null
    group by 
        DATA_DATE,
        CUSTOMER
)

select 
    t2.*
from    
    wk_upload as t1
inner join 
    {{ source('soh', 'test_cust_sell_data') }} as t2
on
    t1.m_UPLORAD_TIME = t2.UPLORAD_TIME
    and t1.DATA_DATE = t2.DATA_DATE



