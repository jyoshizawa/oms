{% set SCENARIO_ac_m = "2023/09" %}     -- 計画年月 Actual Month
{% set SCENARIO_base = "2023/11" %}     -- Base PSI

-- select TO_DATE('2013-05-17') -- to_date('2023/09' || '/01')
-- select to_date(replace('2023/09','/','-') || '-01')

-- select to_date(replace('{{ SCENARIO_ac_m }}','/','-') || '-01')

select to_date(left('{{ SCENARIO_ac_m }}',4) || '-' || right('{{ SCENARIO_ac_m }}',2)  || '-01')

