
{% set ITEM_CDs = ["VP4586040000", "VP4596040000", "VP9120010000"] %}

select
    YYYYMM,
    SITE_CD,
    BC_CD,
    -- {% for ITEM_CD in ITEM_CDs %}
    -- sum(case when ITEM_CD = '{{ITEM_CD}}' then s_QUANTITY end) as {{ITEM_CD}}_QUANTITYaa
    -- {% if not loop.last %},{% endif %}
    -- {% endfor %},
    {% for ITEM_CD in ITEM_CDs %}
     ifnull( sum(case when ITEM_CD = '{{ITEM_CD}}' then s_QUANTITY end) ,0) as {{ITEM_CD}}_QUANTITY
    {% if not loop.last %},{% endif %}
    {% endfor %},


    sum(s_QUANTITY) as total_QUANTITY
from 
    {{ ref('yos_S_monthly_quantity') }} 
group by 
    YYYYMM,
    SITE_CD,
    BC_CD


