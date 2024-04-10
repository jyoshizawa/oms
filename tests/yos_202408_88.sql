select distinct
    PLANNING_DATA    ,
    YYYYMM,
    SCENARIO
from
    IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_TOP10K

order by 1,2,3



-- select 
--     PLANNING_DATA,
--     -- SCENARIO,
--     REVISION,
--     PSI_ELEMENT,
-- 	SITE_CD,
--     BC_CD,
--     count(1)
-- from
--     IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_TOP10K

-- group by
--     PLANNING_DATA,
--     -- SCENARIO,
--     REVISION,
--     PSI_ELEMENT,
-- 	SITE_CD,
--     BC_CD

