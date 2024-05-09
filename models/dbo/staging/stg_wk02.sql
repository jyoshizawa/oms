

/* View_PSISalesForcast */ 

-- Snowflake用 日付情報
with w_current_date_info as (
    select 
        *
    from 
       IT_TEST_DB.DBT_4_YOSHI.STG_DATE_CALENDER
),

w_data_list as (
    -- ◇◇◇◇
    -- 4月～前月までのPSIシナリオ取得 
    SELECT 
        t1.PLANNING_DATA, 
        t1.SCENARIO, 
        t1.YYYYMM, 
        t1.BC_CD, 
        t1.SITE_CD, 
        t2.PLAN_ITEM_CD, 
        t1.PSI_ELEMENT, 
        SUM(t1.QUANTITY) AS QTY
    from
        -- IT_TEST_DB.DBT_4_YOSHI.DBO_PSI_MONTH as t1
        {{ source('dbo', 'DBO_PSI_MONTH') }} as t1
    left join
        -- IT_TEST_DB.DBT_4_YOSHI.DBO_ITEM_APPLICATION as t2
        {{ source('dbo', 'DBO_ITEM_APPLICATION') }} as t2
    on 
        t2.ITEM_CD = t1.ITEM_CD
    where
        t1.PLANNING_DATA = 'Monthly Plan' 
        and t1.PSI_ELEMENT IN ('LS', 'LP ETD(IN)')
        and t1.YYYYMM > (select SART_YYYYMM from  w_current_date_info)
        and t1.SCENARIO between (select sart_SCENARIO from  w_current_date_info) 
                        and     (select end_SCENARIO from  w_current_date_info)
    group by
        t1.PLANNING_DATA, t1.SCENARIO, t1.YYYYMM, t1.BC_CD, t1.SITE_CD, t2.PLAN_ITEM_CD, t1.PSI_ELEMENT
    having
        SUM(t1.QUANTITY) <> 0


    -- 最新PSIシナリオ取得
    union all
    select
        'Monthly Plan' AS PLANNING_DATA, 
        (select end_SCENARIO from  w_current_date_info) AS SCENARIO, 
        t1.YYYYMM, 
        t1.BC_CD, 
        t1.SITE_CD, 
        t2.PLAN_ITEM_CD, 
        t1.PSI_ELEMENT, 
        SUM(t1.QUANTITY) AS QTY
    from
        -- IT_TEST_DB.DBT_4_YOSHI.DBO_PSI_LATEST as t1
        {{ source('dbo', 'DBO_PSI_LATEST') }} as t1
    left join
        -- IT_TEST_DB.DBT_4_YOSHI.DBO_ITEM_APPLICATION as t2
        {{ source('dbo', 'DBO_ITEM_APPLICATION') }} as t2
    on 
        t2.ITEM_CD = t1.ITEM_CD
    where
        t1.PSI_ELEMENT IN ('LS', 'LP ETD(IN)')
        and t1.YYYYMM >= (select sart_YYYYMM from  w_current_date_info)  -- 期初～
    group by  
        t1.YYYYMM, t1.BC_CD, t1.SITE_CD, t2.PLAN_ITEM_CD, t1.PSI_ELEMENT
    having    
        SUM(t1.QUANTITY) <> 0


    /*-- 実績取得 -----------------------------------------------------------*/ 
    union all 
    SELECT            
        'Result' AS PLANNING_DATA, 
        T1.SCENARIO, 
        T0.YYYYMM, 
        T0.BC_CD, 
        T0.SITE_CD, 
        T0.PLAN_ITEM_CD, 
        T0.PSI_ELEMENT, 
        -- CASE WHEN T1.SCENARIO > LEFT(T0.YYYYMM, 4) + '/' + SUBSTRING(T0.YYYYMM, 5, 2) 
        --                     THEN T0.QTY ELSE 0 END AS QTY
        case when replace(T1.SCENARIO,'/','') > t0.YYYYMM then T0.QTY else 0 end as QTY
    FROM (
        /* LP ETD(IN)実績の取得 --*/ 
        SELECT 
                T0.YYYYMM, 
                T3.BC_CD, 
                CASE WHEN T2.DATA_VALUE IS NULL THEN T0.SITE_CD ELSE T2.DATA_VALUE END AS SITE_CD, 
                CASE WHEN T1.PLAN_ITEM_CD = '' THEN T0.ITEM_CD ELSE T1.PLAN_ITEM_CD END AS PLAN_ITEM_CD, 
                'LP ETD(IN)' AS PSI_ELEMENT, 
                SUM(T0.QTY) AS QTY
        FROM (
                
            /*海外出荷-------------------------------*/ 
            SELECT 
                T000.MATNR AS ITEM_CD, 
                T000.KUNNR_SH, 
                T001.SITE_CD, 
                left(T000.FKDAT,6) AS YYYYMM,  -- QTY_SALESの計算  -- <Snowflake用に書き換え>
                (
                    CASE WHEN PSTYV IN  
                    (
                    select   DATA_VALUE  
                    from     {{ source('dbo', 'DBO_DATA_CONDITION') }} 
                    where    SYS_NAME = 'MINERVA' 
                                AND DATA_NAME = 'PSTYV' 
                                AND CONDITION = 'DATA' 
                                AND CONDITION_VALUE = 'WLTSEIKY01' 
                                AND DELETE_FLAG = 0
                        ) 
                    THEN 0 
                    ELSE CASE SHKZG WHEN 'X' THEN FKIMG * - 1   /* 返品明細（SHKZG）＝'X'*/ 
                    ELSE FKIMG END END) AS QTY
            from
                {{ source('dbo', 'DBO_WLTSEIKY01') }}  as T000
            inner join
                {{ source('dbo', 'DBO_BC') }} as T001 
            on T001.SOLD_TO = T000.KUNNR_JU
            where              
                T000.FKDAT between (select yyyymmdd_startday from  w_current_date_info)  
                            and  (select yyyymmdd_lastdady from  w_current_date_info)  
                and T000.DELETE_FLAG = '' 
                and T000.SPART IN (
                    select            
                        DATA_VALUE
                    from
                        -- DATA_CONDITION
                        {{ source('dbo', 'DBO_DATA_CONDITION') }}
                    where
                        SYS_NAME = 'MINERVA' 
                        AND DATA_NAME = 'MATERIAL_TYPE' 
                        AND CONDITION = 'ITEM_CD' 
                        AND CONDITION_VALUE = '' 
                        AND DELETE_FLAG = 0
                )
                            
            /*国内LP実績：香港→東京(Jupiter伝票)----*/ 
            union all
            select
                T001.MATNR AS ITEM_CD, 
                '' AS KUNNR_SH, 
                'S001' AS SITE_CD, 
                left(T001.WADAT_IST,6) AS YYYYMM,   -- <Snowflake用に書き換え>
                SUM(T001.LGMNG) AS QTY
            from
                {{ source('dbo', 'DBO_WBTKOBAI01') }} as T000 
                -- [dbo].[WBTKOBAI01] AS T000 
            inner join
                {{ source('dbo', 'DBO_WLTSYUKK01') }} as T001 
                    --    [dbo].[WLTSYUKK01] AS T001 
            on
                T000.EBELN = T001.VGBEL 
                AND T000.EBELP = T001.VGPOS 
                AND T000.AUART = 'UB  ' 
                AND T000.RESWK = '6030' /*供給プラント(From)*/ 
                AND T000.WERKS = '6020' /*プラント(To)*/ 
                AND T000.SPART <> '20' 
                AND T000.DELETE_FLAG <> 'X'
            where             
                T001.WADAT_IST between (select yyyymmdd_startday from  w_current_date_info)  
                                and  (select yyyymmdd_lastdady from  w_current_date_info)  
                and T001.WBSTK = 'C' 
                and T001.DELETE_FLAG <> 'X'
            group by       
                T001.MATNR, 
                left(T001.WADAT_IST,6)  -- <Snowflake用に書き換え>
                    
            /*国内LP実績：ベトナム→日本(ByD伝票)----*/ 
            union all
            select
                T000.MATNR AS ITEM_CD, 
                T000.KUNNR_SH, 'S001' AS SITE_CD, 
                left(T000.WADAT_IST,6) AS YYYYMM,   -- <Snowflake用に書き換え>
                CASE T000.SHKZG WHEN 'X' THEN T000.LGMNG * - 1 ELSE T000.LGMNG END AS QTY  /* 返品明細（SHKZG）＝'X'*/ 
            from
                -- [dbo].[WLTSYUKK01] AS T000 
                {{ source('dbo', 'DBO_WLTSYUKK01') }} as T000
            LEFT JOIN
                -- [dbo].[ITEM_APPLICATION] AS T001
                {{ source('dbo', 'DBO_ITEM_APPLICATION') }} as T001 
            on 
                T001.ITEM_CD = T000.MATNR
            where
                T000.WADAT_IST between (select yyyymmdd_startday from  w_current_date_info)  
                                and  (select yyyymmdd_lastdady from  w_current_date_info)  
                AND T000.WERKS = 'M103' 
                AND T000.KUNNR_SH = '000000S101' 
                AND T000.WADAT_IST <> '00000000' 
                AND T001.DESTINATION_VER IN ('J', 'W') 
                AND T001.ITEM_CATEGORY_CD1 IN ('1', '2')
        ) as T0   -- ★★★
        inner join
            {{ source('dbo', 'DBO_ITEM_APPLICATION') }}  as t1
        on 
            T1.ITEM_CD = T0.ITEM_CD 
        left join
            {{ source('dbo', 'DBO_DATA_CONDITION') }}  as t2
                    -- [dbo].[DATA_CONDITION] AS T2 
        ON 
            T2.SYS_NAME = 'PSI_APAC_CONV_LP' 
            AND T2.DATA_NAME = 'PLANNING_SITE' 
            AND T2.CONDITION_VALUE = T0.KUNNR_SH 
        left join
            {{ source('dbo', 'DBO_BC') }}  as t3
        on
            T3.SITE_CD = CASE WHEN T2.DATA_VALUE IS NULL THEN T0.SITE_CD ELSE T2.DATA_VALUE END

        GROUP BY        
            T0.YYYYMM, 
            T3.BC_CD, 
            CASE WHEN T2.DATA_VALUE IS NULL THEN T0.SITE_CD ELSE T2.DATA_VALUE END, 
                CASE WHEN T1.PLAN_ITEM_CD = '' THEN T0.ITEM_CD ELSE T1.PLAN_ITEM_CD END


        /* LS実績の取得 --*/ 
        union all
        select
            cast(year(T00.SALES_DATE) as varchar) 
            || case when len(cast(Month(T00.SALES_DATE) as varchar)) =1 then '0' else '' end 
            || cast(Month(T00.SALES_DATE) as varchar) AS YYYYMM,   -- <Snowflake用に書き換え>
            CASE WHEN T03.BC_CD IS NULL THEN T00.BC_CD ELSE cast(T03.BC_CD as varchar) end as BC_CD,
            CASE WHEN T03.BC_CD IS NULL THEN T04.SITE_CD ELSE T03.SITE_CD end as SITE_CD,
            CASE WHEN T01.PLAN_ITEM_CD = '' THEN T00.ITEM_CD ELSE T01.PLAN_ITEM_CD END, 
            'LS' AS PSI_ELEMENT, 
            SUM(T00.QTY_SALES) AS QTY
        from
            {{ source('dbo', 'DBO_SALES_RESULT_MONTH') }} as T00
        left join
            {{ source('dbo', 'DBO_ITEM_APPLICATION') }} as T01
        on 
            T01.DELETE_FLAG = 0 
            AND T01.ITEM_CD = T00.ITEM_CD 
        left join
            {{ source('dbo', 'DBO_DATA_CONDITION') }} as T02
        on 
            T02.SYS_NAME = 'PSI_APAC_CONV_LS' 
            and T02.DATA_NAME = 'PLANNING_SITE' 
            and T02.CONDITION_VALUE = T00.CUSTOMER_CD 
            and T00.BC_CD = '1303'
        left join
            {{ source('dbo', 'DBO_PLANNING_SITE') }} as T03
        on
            T03.SITE_CD = T02.DATA_VALUE 
        left join
            {{ source('dbo', 'DBO_PLANNING_SITE') }} as T04
        on
            T04.BC_CD = T00.BC_CD
            and SUBSTRING(T01.ITEM_CLASSIFICATION_CD, 5, 2) IN ('10')  -- ◇Snowflake  ↓の whereから移動した
        where
            T00.SALES_DATE between (select yyyymmdd_startday from  w_current_date_info)  
                                and  (select yyyymmdd_lastdady from  w_current_date_info)  
        group by
            -- FORMAT(EOMONTH(T00.SALES_DATE), 'yyyyMM'), 
            cast(year(T00.SALES_DATE) as varchar) 
            || case when len(cast(Month(T00.SALES_DATE) as varchar)) =1 then '0' else '' end 
            || cast(Month(T00.SALES_DATE) as varchar) , 
            CASE WHEN T03.BC_CD IS NULL THEN T00.BC_CD ELSE cast(T03.BC_CD as varchar) END, 
            CASE WHEN T03.BC_CD IS NULL THEN T04.SITE_CD ELSE T03.SITE_CD end, 
            CASE WHEN T01.PLAN_ITEM_CD = '' THEN T00.ITEM_CD ELSE T01.PLAN_ITEM_CD END

        ) as t0,

        (
            SELECT DISTINCT 
                T00.SCENARIO
            FROM              
                -- [dbo].[PSI_MONTH] AS T00
                {{ source('dbo', 'DBO_PSI_MONTH') }} as T00
            where
                T00.YYYYMM between (select yyyymmdd_startday from  w_current_date_info)  
                                and  (select yyyymmdd_lastdady from  w_current_date_info)  
                -- T00.YYYYMM >= FORMAT((CASE WHEN Month(GETDATE() - 1) > '9' THEN DATEFROMPARTS(Year(GETDATE() - 1), 4, 1) ELSE DATEFROMPARTS(Year(GETDATE() - 1) - 1, 4, 1) END), 
                --                   'yyyyMM') 
                -- AND T00.SCENARIO >= FORMAT(DATEFROMPARTS(Year(GETDATE() - 1) - 1, 4, 1), 'yyyy/MM') 
                AND T00.PLANNING_DATA = 'Monthly Plan' 
                AND T00.PSI_ELEMENT IN ('LS', 'LP ETD(IN)')
    ) AS T1 

    -- ◇◇◇◇
)

SELECT            
   'FY' + CONVERT(VARCHAR, CASE WHEN SUBSTRING(A.YYYYMM, 5, 2) > '03' THEN YEAR(EOMONTH(A.YYYYMM + '01')) + 1 ELSE YEAR(EOMONTH(A.YYYYMM + '01')) END) AS PSI_PERIOD, 
   A.SCENARIO, 
   A.PSI_ELEMENT, 
   A.PSI_ELEMENT + A.SCENARIO AS PSI_ELM_SCENARIO, 
   LEFT(A.YYYYMM, 4) + '/' + SUBSTRING(A.YYYYMM, 5, 2) AS YYYYMM, 
   A.BC_CD, 
   A.SITE_CD, 
   G.SITE_TXT, 
   F.BUSINESS_SEG_CD, 
   F.BUSINESS_SEG_TXT, 
   LEFT(B.ITEM_CLASSIFICATION_CD, 7) AS ITEM_CLASSIFICATION_CD7, 
   CASE LEFT(B.ITEM_CLASSIFICATION_CD, 7) 
      WHEN '1049100' THEN 'FTS' 
      WHEN '1049103' THEN 'MFTS CAMERA' 
      WHEN '1049104' THEN 'MFTS LENS' 
      WHEN '1049105' THEN 'MFTS ACCESSORIES' 
    ELSE F.BUSINESS_SEG_TXT END AS ITEM_CLASSIFICATION_7, 
    LEFT(B.ITEM_CLASSIFICATION_CD, 8) AS ProductTypeCD, 
   C.DESCRIPTION AS ProductType, 
   LEFT(B.ITEM_CLASSIFICATION_CD, 10) AS SeriesCD, 
   D .DESCRIPTION AS Series, 
   LEFT(B.ITEM_CLASSIFICATION_CD, 12)  AS ModelCD, 
   E.DESCRIPTION AS Model, 
   LEFT(B.ITEM_CLASSIFICATION_CD, 12) + B.COLOR_NAME_EN AS ModelCD_COLOR, 
   E.DESCRIPTION + ' ' + B.COLOR_NAME_EN AS Model_COLOR, 
   A.PLAN_ITEM_CD, 
   B.ITEM_TXT/*,SUM(A.QTY) AS QTY*/ , 
   A.QTY
FROM      
    w_data_list as  A   -- ★★★★★★

left join
   {{ source('dbo', 'DBO_ITEM_APPLICATION') }} as B  --[dbo].[ITEM_APPLICATION] AS B 
on 
    B.DELETE_FLAG = 0 
    AND B.ITEM_CD = A.PLAN_ITEM_CD 
left join
    {{ source('dbo', 'DBO_NUM_ITEM_CLASSIFICATION') }} as c  -- [dbo].[NUM_ITEM_CLASSIFICATION] AS C 
on 
    C.DELETE_FLAG = 0 AND C.ITEM_CLASSIFICATION_CD = LEFT(B.ITEM_CLASSIFICATION_CD, 8) AND C.ZZKAISO = '3' 
left join
   {{ source('dbo', 'DBO_NUM_ITEM_CLASSIFICATION') }} as d  --  [dbo].[NUM_ITEM_CLASSIFICATION] AS D 
on 
    D .DELETE_FLAG = 0 
    AND D .ITEM_CLASSIFICATION_CD = LEFT(B.ITEM_CLASSIFICATION_CD, 10) 
    AND D .ZZKAISO = '4' 
left join
   {{ source('dbo', 'DBO_NUM_ITEM_CLASSIFICATION') }} as e  --[dbo].[NUM_ITEM_CLASSIFICATION] AS E 
on 
    E.DELETE_FLAG = 0 
    AND E.ITEM_CLASSIFICATION_CD = LEFT(B.ITEM_CLASSIFICATION_CD, 12) 
    AND E.ZZKAISO = '5' 
left join
   {{ source('dbo', 'DBO_BUSINESS_SEGMENT') }} as f  -- [dbo].[BUSINESS_SEGMENT] AS F 
on 
    F.DELETE_FLAG = 0 
    AND RTRIM(F.BUSINESS_SEG_CD) = LEFT(B.ITEM_CLASSIFICATION_CD, 4) 
left join
   {{ source('dbo', 'DBO_PLANNING_SITE') }} as G   --PLANNING_SITE AS G 
on 
    G.SITE_CD = A.SITE_CD
where             
    B.ITEM_CATEGORY_CD2 NOT IN ('3')

    
