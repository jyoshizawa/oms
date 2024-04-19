select 
    *,

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
        else 'Qx'
    end as q_YYYYMM,  --Q1～Q4

from
    IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
