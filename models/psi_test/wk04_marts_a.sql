-- marts
-- wk04_marts_a   wk04_20240430

select 
    PLANNING_DATA,
	SCENARIO,
	ITEM_CD,
    coalesce(t2.MATERIAL_NAME, t1.ITEM_CD) as ITEM_NAME,
	YYYYMM,
	BC_CD,
	LP_ETD_IN_QUANT,
	P_QUANT,
	LI_QUANT,
	LS_QUANT,
	I_QUANT
from 
    {{ref('wk03_staging')}} as t1
left join
    IT_TEST_DB.TEST_SCHEMA.SOH_MASTER_NATERUAL as t2  -- アイテム名 ※別の確認で使ったテーブルなので注意！
on 
    t1.ITEM_CD = trim(t2.MATERIAL_CODE)


-- where ITEM_CD = 'V110030BG000'
-- and YYYYMM between 202304 and  202401
-- and SCENARIO in ( '2023/09')

-- {% set SCENARIO_start_column = '2023/09' %}  -- 計画年月 Actual Month
-- {% set SCENARIO_end_column = '2023/11' %}    -- Base PSI
-- {% set ITEM_CDs = ('V110030BG000', '-') %}  -- GT-7 黒 赤 ("V110030BG000", "V110030RG000")
-- {% set yyyymm_start_column = 202304 %}
-- {% set yyyymm_end_column = 202403 %}

-- where 
--     SCENARIO in ('{{ SCENARIO_start_column }}', '{{ SCENARIO_end_column }}' )
--     and ITEM_CD in {{ITEM_CDs}}
--     and PLANNING_DATA = 'Monthly Plan'   -- 「PreAllocation」と同じようなので、1つに絞る
--     and YYYYMM between {{yyyymm_start_column}} and  {{yyyymm_end_column}}
