select distinct
    PSI_ELEMENT,
    case when SITE_CD is null then '-' else SITE_CD end AS SITE_CD,
    BC_CD,
    -- BULK_PARENTS_CD
from
  IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11

-- order by 1,2,3
