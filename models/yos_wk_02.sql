/*
　テーブルとviewの結合できそう
*/

with w_list as (
    select * 
    from 
        it_test_db.test_schema.psi_month_2309_10_11
)
select 
    a.*
from 
    w_list as a
inner join 
    it_test_db.dbt_schema_yos_01.yos_wk_01 as b on a.psi_element = b.psi_element
