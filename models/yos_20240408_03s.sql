
with w_list as (
    select 
        PLANNING_DATA   as "プランニング",
        SCENARIO        as "シナリオ",
        cast(replace( SCENARIO,'/','-') || '-01' as timestamp) as date_SCENARIO,  --日付型
        cast(replace( SCENARIO,'/','-') || '-01' as timestamp) as "シナリオ_日付型",  --日付型 セマンティックレイヤー
        case 
            when right( SCENARIO,2) in ('01','02','03') then  cast( cast(left(SCENARIO,4) as int) -1 AS varchar)
            else left(SCENARIO,4)
        end ||
        case 
            when right( SCENARIO,2) in ('04','05','06') then 'Q1'
            when right( SCENARIO,2) in ('07','08','09') then 'Q2'
            when right( SCENARIO,2) in ('10','11','12') then 'Q3'
            when right( SCENARIO,2) in ('01','02','03') then 'Q4'
            else 'Q?'
        end as q_SCENARIO,  --Q1～Q4

        YYYYMM  AS "年月_yyyymm",
        cast(left(to_varchar(yyyymm),4) || '-' || right(to_varchar(yyyymm),2) || '-01' as timestamp) as date_YYYYMM,

        case 
            when right( cast(YYYYMM as varchar),2) in ('01','02','03') then  cast( cast(left(cast(YYYYMM as varchar),4) as int) -1 AS varchar)
            else left(cast(YYYYMM as varchar),4)
        end ||
        case 
            when right( cast(YYYYMM as varchar),2) in ('04','05','06') then 'Q1'
            when right( cast(YYYYMM as varchar),2) in ('07','08','09') then 'Q2'
            when right( cast(YYYYMM as varchar),2) in ('10','11','12') then 'Q3'
            when right( cast(YYYYMM as varchar),2) in ('01','02','03') then 'Q4'
            else 'Q?'
        end as q_YYYYMM,  --Q1～Q4


        ITEM_CD         as "ITEMコード",
        PSI_ELEMENT     as "PSI",
        QUANTITY        as "個数"
    from
        IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
    where 
        PLANNING_DATA ='Monthly Plan'
        and ITEM_CD = 'V741002BE000'
        and PSI_ELEMENT = 'P'

)

select * from w_list
limit 200

-- order by  
--     SCENARIO,
--     YYYYMM
