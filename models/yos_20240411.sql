with w_list as (
    select 
        *,
        cast(replace( SCENARIO,'/','-') || '-01' as timestamp) as date_SCENARIO,  --日付型
        cast(left(to_varchar(yyyymm),4) || '-' || right(to_varchar(yyyymm),2) || '-01' as timestamp) as date_YYYYMM
    from
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
    where 
        SCENARIO in ('2023/09','2023/10','2023/11')
)

-- select count(1) from w_list  -- 1160121件

select * from w_list
