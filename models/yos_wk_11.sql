-- jinja　だそう 

select
  PSI_ELEMENT,
    {% for ITEM_CD in ["VP4586040000", "VP4596040000", "VP9120010000"] %}
    sum(case when ITEM_CD = '{{ITEM_CD}}' then QUANTITY end) as {{ITEM_CD}}_QUANTITY,
    {% endfor %}
    sum(QUANTITY) as total_QUANTITY
from 
    -- it_test_db.test_schema.psi_month_2309_10_11
    {{ ref('yos_wk_02') }} 
group by 
    PSI_ELEMENT

