-- jinja　だそう  （Snowflakeで展開されるsqlに）最後のカンマ が入らないよ

{% set ITEM_CDs = ["VP4586040000", "VP4596040000", "VP9120010000"] %}

select
  PSI_ELEMENT,
    {% for ITEM_CD in ITEM_CDs %}
    sum(case when ITEM_CD = '{{ITEM_CD}}' then QUANTITY end) as {{ITEM_CD}}_QUANTITY
    {% if not loop.last %},{% endif %}
    {% endfor %},
    sum(QUANTITY) as total_QUANTITY
from 
    -- it_test_db.test_schema.psi_month_2309_10_11
    {{ ref('yos_wk_02') }} 
group by 
    PSI_ELEMENT

