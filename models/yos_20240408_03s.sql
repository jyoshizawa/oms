
with w_list as (
    select 
        PLANNING_DATA,
        SCENARIO,
        YYYYMM,
        ITEM_CD,
        PSI_ELEMENT,
        QUANTITY,
        cast(replace( SCENARIO,'/','-') || '-01' as timestamp) as date_SCENARIO,  --日付型
        cast(left(to_varchar(yyyymm),4) || '-' || right(to_varchar(yyyymm),2) || '-01' as timestamp) as date_YYYYMM
    from
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
    where 
        PLANNING_DATA ='Monthly Plan'
        and ITEM_CD = 'V741002BE000'
        and PSI_ELEMENT = 'P'

)

-- select * from w_list


    select 
        date_SCENARIO,
        PLANNING_DATA,
        sum(QUANTITY) as S_QUANTITY
    from
        w_list
    group by
        date_SCENARIO,
        PLANNING_DATA
    