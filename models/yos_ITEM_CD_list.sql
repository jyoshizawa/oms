select distinct
    ITEM_CD,
    min(YYYYMM) AS start_YYYYMM
from
  IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
group by
    ITEM_CD
order by 
    ITEM_CD
