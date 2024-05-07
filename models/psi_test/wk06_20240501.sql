{% set SCENARIO_ac_m = "2023/09" %}  -- 計画年月 Actual Month
{% set SCENARIO_base = "2023/11" %}  -- Base PSI

with  w_base as (  -- 1
    select *
    from it_test_db.dbt_4_yoshi.wk04_marts_a
    where scenario in ('{{SCENARIO_base}}')
),

w_actual_month as (  -- 2
    select *
    from it_test_db.dbt_4_yoshi.wk04_marts_a
    where scenario in ('{{SCENARIO_ac_m}}')
),

w_list as (
    select
        t1.*,
        coalesce(t2.lp_etd_in_quant, 0) as acm_lp_etd_in_quant,
        coalesce(t2.p_quant, 0) as acm_p_quant,
        coalesce(t2.li_quant, 0) as acm_li_quant,
        coalesce(t2.ls_quant, 0) as acm_ls_quant,
        coalesce(t2.i_quant, 0) as acm_i_quant
    from w_base as t1
    left join
        w_actual_month as t2
        -- SCENARIO の異なる 2つの差を出す
        on t1.planning_data = t2.planning_data
        and t1.item_cd = t2.item_cd
        and t1.yyyymm = t2.yyyymm
        and t1.bc_cd = t2.bc_cd
)

select
    *,
    lp_etd_in_quant - acm_lp_etd_in_quant as diff_lp_etd_in_quant,
    LI_QUANT - acm_P_QUANT as diff_P_QUANT
    -- case when ifnull(acm_P_QUANT, 0) = 0  then 0 else P_QUANT / acm_P_QUANT end as pr_P_QUANT_aa,

    -- case
    --     when ifnull(acm_lp_etd_in_quant, 0) = 0
    --     then 0
    --     else lp_etd_in_quant / acm_lp_etd_in_quant
    -- end as pr_lp_etd_in_quant,
from w_list
