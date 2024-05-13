
{% set FY_year = '2024' %}  -- 指定年度

----------------------------------------------------------------------------
-- SCENARIO と YYYYMM の算出
with w_year_base as (
    select
        cast(cast({{FY_year}} as int) - 1 as varchar)  as last_yyyy,
        {{FY_year}} as yyyy 
),

w_scenario_and_yyyymm as (
    select
        'FY' || yyyy || '_5月' as fy_name,
        last_yyyy || '/05'  as SCENARIO_base,
        last_yyyy || '/08'  as SCENARIO_ac_m,
        last_yyyy || '05' as start_yyyymm,
        last_yyyy || '08' as end_yyyymm
    from
        w_year_base

    union all
    select
        'FY' || yyyy || '_9月' as fy_name,
        last_yyyy || '/09'  as SCENARIO_base,
        last_yyyy || '/12'  as SCENARIO_ac_m,
        last_yyyy || '09' as start_yyyymm,
        last_yyyy || '12' as end_yyyymm
    from
        w_year_base

    union all
    select
        'FY' || yyyy || '_1月' as fy_name,
        last_yyyy || '/12'  as SCENARIO_base,
        yyyy || '/03'  as SCENARIO_ac_m,
        last_yyyy || '12' as start_yyyymm,
        yyyy || '03' as end_yyyymm
    from
        w_year_base
),


----------------------------------------------------------------------------
-- Local 情報
w_local_list as (
    select *
    from (
        values
        ( 11, 'S031',	'02_EU' ),
        ( 12, 'S041',	'03_US' ),
        ( 13, 'S001',	'01_JP' ),
        ( 14, 'S011',	'04_CN' ),
        ( 15, 'S051',	'05_AU' ),
        ( 21, 'S015',	'06_HK' ),
        ( 22, 'S019',	'12_SP' ),
        ( 23, 'S018',	'11_ML' ),
        ( 24, 'S020',	'13_TH' ),
        ( 25, 'S017',	'14_TW' ),
        ( 26, 'S021',	'15_IN' ),
        ( 31, 'S002',	'HQ Others' )
    ) as lical_list(SITE_SORT, SITE_CD, SITE_NAME )
) ,


----------------------------------------------------------------------------
-- PSI 計算
w_sum_list as (
    select
        PSI_PERIOD,
        SCENARIO,
        BUSINESS_SEG_CD,
        YYYYMM,
        ITEM_CLASSIFICATION_CD7,    
        ProductTypeCD,
        SeriesCD,
        ModelCD,
        ModelCD_COLOR,
        Model_COLOR,
        SITE_CD,
        sum(QTY) as QTY
    from
        -- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
         {{ref('stg_VIEW_PSISalesForcast')}}
    where
        PSI_ELEMENT not in ('LP ETD(IN)') -- Excelのシートから除かれていたたようなので 条件を追加
        and  (
                SCENARIO in  (select SCENARIO_BASE from w_scenario_and_yyyymm)
                or SCENARIO in  (select SCENARIO_ac_m from w_scenario_and_yyyymm)
        )
    group by
        PSI_PERIOD,
        SCENARIO,
        BUSINESS_SEG_CD,
        YYYYMM,
        ITEM_CLASSIFICATION_CD7,    
        ProductTypeCD,
        SeriesCD,
        ModelCD,
        ModelCD_COLOR,
        Model_COLOR,
        SITE_CD
),

--
w_base as (  -- 1 SCENARIO 基準月 
    select 
        t2.fy_name,
        t1.*
    from 
        w_sum_list as t1
    inner join 
        w_scenario_and_yyyymm as t2
    on 
        t1.SCENARIO = t2.SCENARIO_base
        and t1.YYYYMM between t2.start_yyyymm and t2.end_yyyymm
),

w_actual_month as (  -- 2 SCENARIO 計画月
    select 
        t2.fy_name,
        t1.*
    from 
        w_sum_list as t1
    inner join 
        w_scenario_and_yyyymm as t2
    on 
        t1.SCENARIO = t2.SCENARIO_ac_m
        and t1.YYYYMM between t2.start_yyyymm and t2.end_yyyymm
)

----------------------------------------------------------------------------

select
    t1.fy_name,
    t1.PSI_PERIOD,
    t1.SCENARIO,
    t1.BUSINESS_SEG_CD,
    t1.YYYYMM,
    t1.ITEM_CLASSIFICATION_CD7,    
    t1.ProductTypeCD,
    t1.SeriesCD,
    t1.ModelCD,
    t1.ModelCD_COLOR,
    t1.Model_COLOR,
    t1.SITE_CD,
    t3.SITE_NAME,
    t3.SITE_SORT,
    t1.QTY as qty,
    ifnull(t2.QTY,0) as latest_qty,
    t4.ZZKAISO,
    t4.DESCRIPTION
from
    w_base as t1
left join
    w_actual_month as t2
on 
    t1.fy_name = t2.fy_name
    and t1.PSI_PERIOD = t2.PSI_PERIOD
    and t1.BUSINESS_SEG_CD= t2.BUSINESS_SEG_CD
    and t1.YYYYMM = t2.YYYYMM
    and t1.ITEM_CLASSIFICATION_CD7 = t2.ITEM_CLASSIFICATION_CD7
    and t1.ProductTypeCD = t2.ProductTypeCD
    and t1.SeriesCD = t2.SeriesCD
    and t1.ModelCD = t2.ModelCD
    and t1.ModelCD_COLOR = t2.ModelCD_COLOR
    and t1.Model_COLOR = t2.Model_COLOR
    and t1.SITE_CD = t2.SITE_CD

left join
    w_local_list as t3
on
    t1.SITE_CD = t3.SITE_CD

left join   
    {{ref('stg_View_ITEM_CLASSIFICATION_LIST')}} as t4  
on  t1.SeriesCD = t4.ITEM_CLASSIFICATION_CD
