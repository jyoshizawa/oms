-- mar_dbo_LP_Forcast   -- 年間　　5/13 数値確認中！！！！

{% set FY_year = "2024" %}  -- 指定年度

-- --------------------------------------------------------------------------
-- SCENARIO と YYYYMM の算出
with
    w_year_base as (
        select
            cast(cast({{ FY_year }} as int) - 1 as varchar) as last_yyyy,
            {{ FY_year }} as yyyy
    ),

    w_scenario_and_yyyymm as (
        -- 2月までの実績用
        select
            yyyy || '/01' as scenario_base,
            yyyy || '/03' as scenario_ac_m,
            last_yyyy || '04' as ac_start_yyyymm,
            yyyy || '03' as ac_end_yyyymm,
            yyyy || '01' as lp_start_yyyymm,
            yyyy || '03' as lp_end_yyyymm
        from w_year_base
    ),

    -- --------------------------------------------------------------------------
    -- Local 情報
    w_local_list as (
        select *
        from
            (
                values
                    (11, 'S031', '02_EU'),
                    (12, 'S041', '03_US'),
                    (13, 'S001', '01_JP'),
                    (14, 'S011', '04_CN'),
                    (15, 'S051', '05_AU'),
                    (21, 'S015', '06_HK'),
                    (22, 'S019', '12_SP'),
                    (23, 'S018', '11_ML'),
                    (24, 'S020', '13_TH'),
                    (25, 'S017', '14_TW'),
                    (26, 'S021', '15_IN'),
                    (31, 'S002', 'HQ Others')
            ) as lical_list(site_sort, site_cd, site_name)
    ),


    -- --------------------------------------------------------------------------
    w_sum_list as (

        -- PSI 計算1  12月までの実績
        select
            psi_period,
            scenario,
            business_seg_cd,
            yyyymm,
            item_classification_cd7,
            producttypecd,
            seriescd,
            modelcd,
            modelcd_color,
            model_color,
            site_cd,
            sum(qty) as qty
        from
            -- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
            {{ ref("stg_VIEW_PSISalesForcast") }}
        where
            psi_element in ('LP ETD(IN)')  --  条件を追加
            and scenario in (select scenario_base from w_scenario_and_yyyymm )
        group by
            psi_period,
            scenario,
            business_seg_cd,
            yyyymm,
            item_classification_cd7,
            producttypecd,
            seriescd,
            modelcd,
            modelcd_color,
            model_color,
            site_cd
        
        -- 3月 LP
        union all
        select
            psi_period,
            scenario,
            business_seg_cd,
            yyyymm,
            item_classification_cd7,
            producttypecd,
            seriescd,
            modelcd,
            modelcd_color,
            model_color,
            site_cd,
            sum(qty) as qty
        from
            -- IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST
            {{ ref("stg_VIEW_PSISalesForcast") }}
        where
            psi_element not in ('LP ETD(IN)')  --  条件を追加
            and scenario in (select scenario_ac_m from w_scenario_and_yyyymm )
        group by
            psi_period,
            scenario,
            business_seg_cd,
            yyyymm,
            item_classification_cd7,
            producttypecd,
            seriescd,
            modelcd,
            modelcd_color,
            model_color,
            site_cd
    ),

 --
    w_base as (  -- 1 SCENARIO 基準月 
        select t1.*
        from w_sum_list as t1
        inner join
            w_scenario_and_yyyymm as t2
            on t1.scenario = t2.scenario_base
            and t1.yyyymm between t2.ac_start_yyyymm and t2.ac_end_yyyymm
    ),

    w_actual_month as (  -- 2 SCENARIO 計画月
        select t1.*
        from w_sum_list as t1
        inner join
            w_scenario_and_yyyymm as t2
            on t1.scenario = t2.scenario_ac_m
            and t1.yyyymm between t2.lp_start_yyyymm and t2.lp_end_yyyymm
    )



----------------------------------------------------------------------------
select
    t1.psi_period,
    t1.scenario,
    t1.business_seg_cd,
    t1.yyyymm,
    t1.item_classification_cd7,
    t1.producttypecd,
    t1.seriescd,
    t1.modelcd,
    t1.modelcd_color,
    t1.model_color,
    t1.site_cd,
    t3.site_name,
    t3.site_sort,
    t1.qty as qty,
    case when right(t1.yyyymm,2) = '03'  then ifnull(t2.qty, 0) else t1.qty end  as latest_qty,
    t4.zzkaiso,
    t4.description
from w_base as t1
left join
    w_actual_month as t2
on 
    t1.psi_period = t2.psi_period
    and t1.business_seg_cd = t2.business_seg_cd
    and t1.yyyymm = t2.yyyymm
    and t1.item_classification_cd7 = t2.item_classification_cd7
    and t1.producttypecd = t2.producttypecd
    and t1.seriescd = t2.seriescd
    and t1.modelcd = t2.modelcd
    and t1.modelcd_color = t2.modelcd_color
    and t1.model_color = t2.model_color
    and t1.site_cd = t2.site_cd

left join w_local_list as t3 on t1.site_cd = t3.site_cd

left join
    {{ ref("stg_View_ITEM_CLASSIFICATION_LIST") }} as t4
    on t1.seriescd = t4.item_classification_cd


/*
select * from 
    w_sum_list as t1
where   
    t1.modelcd_color = '104910311500Black'
    and t1.SITE_CD ='S031'
*/
/*
select 
*
from 
w_actual_month as t1  -- w_base w_actual_month


where
    -- t1.SCENARIO = '2024/01'
     t1.modelcd = '104910311501'
    and t1.SITE_CD ='S031'
    -- and t1.YYYYMM = 202403
    
*/
