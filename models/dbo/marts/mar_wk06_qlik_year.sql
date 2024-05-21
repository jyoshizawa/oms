--sample04_LP納期遵守モニタリング_2024 の確認用クエリ  MAR_DBO_LP_FORCAST mar_dbo_lp_forcast.sql

{% set FY_year = "2024" %}  -- 指定年度

-- 実績　04月～02月
select 
    psi_period,
    scenario,
    yyyymm,
    business_seg_cd,
    item_classification_cd7,
    producttypecd,
    seriescd,
    modelcd,
    modelcd_color,
    model_color,
    site_cd,
    site_name,
    site_sort,
    zzkaiso,
    description,
    sum(qty) as qty,
    sum(qty) as latest_qty  -- 同じ値
from
    -- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
    {{ ref("stg_wk11_psi_sales_forcast") }}   -- stg_wk11_psi_sales_forcast
where
    PSI_PERIOD = 'FY' || {{ FY_year }}
    and psi_element in ('LP ETD(IN)')  --  条件を追加 ※仮です
    and scenario = cast(cast({{ FY_year }} as int) - 1 as varchar) || '/04'
    and YYYYMM between cast(cast({{ FY_year }} as int) - 1 as varchar) || '04' and {{ FY_year }}  || '02'
group by
    psi_period,
    scenario,
    yyyymm,
    business_seg_cd,
    item_classification_cd7,
    producttypecd,
    seriescd,
    modelcd,
    modelcd_color,
    model_color,
    site_cd,
    site_name,
    site_sort,
    zzkaiso,
    description


-- 03月 計画  01月ベース
union all
select 
    t1.*,
    ifnull(t2.qty,0) as latest_qty
from (
    select 
        psi_period,
        scenario,
        yyyymm,
        business_seg_cd,
        item_classification_cd7,
        producttypecd,
        seriescd,
        modelcd,
        modelcd_color,
        model_color,
        site_cd,
        site_name,
        site_sort,
        zzkaiso,
        description,
        sum(qty) as qty
    from
        -- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
        {{ ref("stg_wk11_psi_sales_forcast") }}
    where
        PSI_PERIOD = 'FY' || {{ FY_year }}
        and psi_element not in ('LP ETD(IN)')  --  条件を追加 ※仮です
        and scenario ={{ FY_year }}  || '/03'
        and YYYYMM = {{ FY_year }}  || '03'
    group by
        psi_period,
        scenario,
        yyyymm,
        business_seg_cd,
        item_classification_cd7,
        producttypecd,
        seriescd,
        modelcd,
        modelcd_color,
        model_color,
        site_cd,
        site_name,
        site_sort,
        zzkaiso,
        description
) as t1

left join (
    select 
        psi_period,
        scenario,
        yyyymm,
        business_seg_cd,
        item_classification_cd7,
        producttypecd,
        seriescd,
        modelcd,
        modelcd_color,
        model_color,
        site_cd,
        site_name,
        site_sort,
        zzkaiso,
        description,
        sum(qty) as qty
    from
        -- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
        {{ ref("stg_wk11_psi_sales_forcast") }}
    where
        PSI_PERIOD = 'FY' || {{ FY_year }}
        and psi_element in ('LP ETD(IN)')  --  条件を追加 ※仮です
        and scenario ={{ FY_year }}  || '/01'      --  in ({{ FY_year }}  || '/01',  {{ FY_year }}  || '/03' )
        and YYYYMM = {{ FY_year }}  || '03'
    group by
        psi_period,
        scenario,
        yyyymm,
        business_seg_cd,
        item_classification_cd7,
        producttypecd,
        seriescd,
        modelcd,
        modelcd_color,
        model_color,
        site_cd,
        site_name,
        site_sort,
        zzkaiso,
        description
) as t2
on
    t1.psi_period = t2.psi_period
    and  t1.yyyymm = t2.yyyymm
    and  t1.business_seg_cd = t2.business_seg_cd
    and  t1.item_classification_cd7 = t2.item_classification_cd7
    and  t1.producttypecd = t2.producttypecd
    and  t1.seriescd = t2.seriescd
    and  t1.modelcd = t2.modelcd
    and  t1.modelcd_color = t2.modelcd_color
    and  t1.model_color = t2.model_color
    and  t1.site_cd = t2.site_cd
    and  t1.site_name = t2.site_name
    and  t1.site_sort = t2.site_sort
    and  t1.zzkaiso = t2.zzkaiso
    and  t1.description = t2.description

