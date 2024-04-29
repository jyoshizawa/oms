
-- パラメータの使用
{% set SCENARIO_column = "2023/09" %}
{% set ITEM_CDs = ("V110030BG000", "V110030RG000") %}  -- GT-7 黒 赤
{% set PSI_ELEMENTs = ['I', 'LI', 'LP ETD(IN)','LS','P'] %}    --PSI 　生産(P)？とか　在庫(I)、販売、とかかな

-- {% set yyyymm_column = 202309 %}

select 
    yyyymm,
    PSI_ELEMENT,
    ITEM_CD,
    sum(mp_quant) as mp_quant,
    sum(pre_quant) as pre_quant
from (
    select
        --  計画と事前割り当て、同じのが入ってるよう
        yyyymm,
        PSI_ELEMENT,
        ITEM_CD,
        case when PLANNING_DATA = 'Monthly Plan' then quantity else 0 end as mp_quant,
        case when PLANNING_DATA = 'PreAllocation' then quantity else 0 end as pre_quant,
    from 
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
    where 
        SCENARIO = '{{ SCENARIO_column }}'
        and ITEM_CD in {{ITEM_CDs}}
        and PLANNING_DATA = 'Monthly Plan'   -- 「PreAllocation」と同じようなので、1つに絞る
)
group by
    yyyymm,
    PSI_ELEMENT,
    ITEM_CD

order by 1,2,3


-- V110030BG000 