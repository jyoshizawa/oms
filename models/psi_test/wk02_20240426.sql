
-- パラメータの使用
--  ↓実行するときの使い方例
-- dbt run --vars SCENARIO_column: "2023/11", yyyymm_column: 202312

{% set SCENARIO_column = "2023/09" %}
{% set ITEM_CDs = ("V110030BG000", "V110030RG000") %}  -- GT-7 黒 赤
{% set PSI_ELEMENTs = [ 'P', 'I', 'LI', 'LS'] %}    --PSI 　生産(P)？とか　在庫(I)、販売、とかかな "LP ETD(IN)",  
--  jinjya で、指定した文字列に「（」があると、カラム名を作るところでエラーになる
--      P:  生産、　BC_CD 1033
--      I: 在庫か、　BC_CD 1033
--      LI: 在庫目標
--      LS: 売り上げ目標
--      LP ETD: 　SCENARIO=YYYYMM は 割り当て（LI＋LS） 　在庫予定？？

    select
        -- PLANNING_DATA,
        ITEM_CD,
        yyyymm,
        BC_CD,
        {% for PSI_ELEMENT in PSI_ELEMENTs %}
        ifnull( sum(case when PSI_ELEMENT = '{{PSI_ELEMENT}}' then quantity else 0 end) ,0) as {{PSI_ELEMENT}}_quant
        {% if not loop.last %},{% endif %}
        {% endfor %},
        ifnull( sum(case when left(PSI_ELEMENT,6) = 'LP ETD' then quantity else 0 end) ,0)  as LP_ETD_quant
    from 
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
    where 
        SCENARIO = '{{ SCENARIO_column }}'
        and ITEM_CD in {{ITEM_CDs}}
        and PLANNING_DATA = 'Monthly Plan'   -- 「PreAllocation」と同じようなので、1つに絞る
        -- and BC_CD = 1034  -- 01_*** みたいな国のことだと思われ
    group by
        ITEM_CD,
        yyyymm,
        BC_CD

order by 1,2,3

