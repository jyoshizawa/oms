with wk_upload as (
    select
        DATADATE,
        CUSTOMER,
        max(UPLORAD_TIME) as M_UPLORAD_TIME
    from 
        {{ source('soh', 'test_cust_data') }}
    where 
        DATADATE is not null
    group by 
        DATADATE,
        CUSTOMER
)

select 
    t2.*
from    
    wk_upload as t1
inner join 
    {{ source('soh', 'test_cust_data') }} as t2
on
    t1.m_UPLORAD_TIME = t2.UPLORAD_TIME
    and t1.DATADATE = t2.DATADATE

