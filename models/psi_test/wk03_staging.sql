
-- PLANNING_DATA, SCENARIO, ITEM_CD, YYYYMM, BC_CD 別 PSI_ELEMENT毎の集計
-- IT_TEST_DB.DBT_4_YOSHI.wk03_staging
-- staging です

{% set PSI_ELEMENTs = [  
            { 'id':'k_I', 'name':'(KIT)I' },
            { 'id':'k_I_un', 'name':'(KIT)I Unsaleable' },
            { 'id':'k_LI', 'name':'(KIT)LI' },
            { 'id':'k_LI_un', 'name':'(KIT)LI Unsaleable' },
            { 'id':'k_LP_ETD_in', 'name':'(KIT)LP ETD(IN)' },
            { 'id':'k_LS', 'name':'(KIT)LS' },
            { 'id':'k_P', 'name':'(KIT)P' },
            { 'id':'Adjustment', 'name':'Adjustment' },
            { 'id':'Bulk_I', 'name':'Bulk I' },
            { 'id':'Bulk_Re', 'name':'Bulk Required' },
            { 'id':'I', 'name':'I' },
            { 'id':'I_un', 'name':'I Unsaleable' },
            { 'id':'LI', 'name':'LI' },
            { 'id':'LI_un', 'name':'LI Unsaleable' },
            { 'id':'LP_ETD_in', 'name':'LP ETD(IN)' },
            { 'id':'LS', 'name':'LS' },
            { 'id':'P', 'name':'P' }
    ] %}   

    select
        PLANNING_DATA,
        SCENARIO,
        ITEM_CD,
        YYYYMM,
        BC_CD,
        {% for PSI_ELEMENT in PSI_ELEMENTs %}
        ifnull( sum(case when PSI_ELEMENT = '{{PSI_ELEMENT.name}}' then quantity else 0 end) ,0) as {{PSI_ELEMENT.id}}_quant
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from 
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
    group by
        PLANNING_DATA,
        SCENARIO,
        ITEM_CD,
        YYYYMM,
        BC_CD


