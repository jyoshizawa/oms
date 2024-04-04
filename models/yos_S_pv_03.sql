{% set ITEM_CDs = ["VP4586040000", "VP4596040000", "VP9120010000"] %}

select
    YYYYMM,
    SITE_CD,
    BC_CD,

    -- --  当月分 OK
    -- {% for ITEM_CD in ITEM_CDs %}
    -- sum(case when ITEM_CD = '{{ITEM_CD}}' then S_QUANTITY end) as {{ITEM_CD}}_QUANTITY
    -- {% if not loop.last %},{% endif %}
    -- {% endfor %},
    -- sum(S_QUANTITY) as total_QUANTITY

    -- 前月との差
    {% for ITEM_CD in ITEM_CDs %}
    ifnull( sum(case when ITEM_CD = '{{ITEM_CD}}' then LAST_MOMTH_DIFF_S_QUANTITY end) , 0 ) as {{ITEM_CD}}_diff_QUANTITY
    {% if not loop.last %},{% endif %}
    {% endfor %}

from 
    {{ ref('yos_S_pv_02') }} 
group by 
    YYYYMM,
    SITE_CD,
    BC_CD


