with w_calc as (
    select
        YYYYMM,
        SITE_CD,
        BC_CD,
        ITEM_CD,
        sum(s_QUANTITY) as s_QUANTITY
    from 
        {{ ref('yos_S_monthly_quantity') }} 
    group by 
        YYYYMM,
        SITE_CD,
        BC_CD,
        ITEM_CD
)

select 
    t1.YYYYMM,
    t2.SITE_CD,
    t2.BC_CD,
    t2.ITEM_CD,
    t2.s_QUANTITY,
    t3.s_QUANTITY AS LAST_MONTH_S_QUANTITY,
    ifnull(t2.s_QUANTITY,0) - ifnull(t3.s_QUANTITY,0) as DIFF_S_QUANTITY
from
    {{ref('yos_month_calender')}} as t1
inner join
    w_calc AS t2 
    on t1.YYYYMM = t2.YYYYMM
inner join
    w_calc AS t3 
    on t1.last_month = t3.YYYYMM
        and t2.SITE_CD = t3.SITE_CD
        and t2.BC_CD = t3.BC_CD
        and t2.ITEM_CD = t3.ITEM_CD

