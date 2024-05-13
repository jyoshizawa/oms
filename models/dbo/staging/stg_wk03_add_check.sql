select 

--   'FY' + CONVERT(VARCHAR, CASE WHEN SUBSTRING(A.YYYYMM, 5, 2) > '03' THEN YEAR(EOMONTH(A.YYYYMM + '01')) + 1 ELSE YEAR(EOMONTH(A.YYYYMM + '01')) END) AS PSI_PERIOD, 
    'FY' || 
    cast( 
        case when right(cast(A.YYYYMM as varchar),2)  > '3' then 
            cast(left(cast(A.YYYYMM as varchar),4) as int) + 1 
            else cast(left(cast(A.YYYYMM as varchar),4) as int) end 
    as varchar ) AS PSI_PERIOD, 
    YYYYMM,
from
    IT_TEST_DB.DBT_4_YOSHI.STG_VIEW_PSISALESFORCAST as A
where
    SCENARIO = '2024/04'
    and YYYYMM  in ( 202312,202401,202402,202403 )
    and MODELCD =  '104910311300'
