

with w_all as (
    select * 
    from 
        it_test_db.test_schema.psi_month_2309_10_11
),

w_list as (
    select distinct
        PSI_ELEMENT 
    from 
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_TOP10K
    where 
        PSI_ELEMENT like 'B%' 
        or PSI_ELEMENT like 'P%' 
)

select 
    a.*
from 
    w_all as a
inner join 
    w_list as b on a.psi_element = b.psi_element
