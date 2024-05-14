
{% set FY_actual_m = '2024/03' %}
{% set FY_base_m = '2024/01' %}

----------------------------------------------------------------------------
-- PSI 計算
with w_sum_list as (
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
        and SCENARIO in  ('{{FY_actual_m}}', '{{FY_base_m}}' )
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
        *
    from 
        w_sum_list
    where SCENARIO = '{{ FY_actual_m }}'
),

w_actual_month as (  -- 2 SCENARIO 計画月
    select 
        *
    from 
        w_sum_list
    where SCENARIO = '{{ FY_base_m }}'
)

----------------------------------------------------------------------------

select
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
    -- t3.SITE_NAME,
    -- t3.SITE_SORT,
    t1.QTY as qty,
    ifnull(t2.QTY,0) as latest_qty,
    t4.ZZKAISO,
    t4.DESCRIPTION
from
    w_base as t1
left join
    w_actual_month as t2
on 
    t1.PSI_PERIOD = t2.PSI_PERIOD
    and t1.BUSINESS_SEG_CD= t2.BUSINESS_SEG_CD
    and t1.YYYYMM = t2.YYYYMM
    and t1.ITEM_CLASSIFICATION_CD7 = t2.ITEM_CLASSIFICATION_CD7
    and t1.ProductTypeCD = t2.ProductTypeCD
    and t1.SeriesCD = t2.SeriesCD
    and t1.ModelCD = t2.ModelCD
    and t1.ModelCD_COLOR = t2.ModelCD_COLOR
    and t1.Model_COLOR = t2.Model_COLOR
    and t1.SITE_CD = t2.SITE_CD

-- left join
--     w_local_list as t3
-- on
--     t1.SITE_CD = t3.SITE_CD

left join   
    {{ref('stg_View_ITEM_CLASSIFICATION_LIST')}} as t4  
on  t1.SeriesCD = t4.ITEM_CLASSIFICATION_CD
