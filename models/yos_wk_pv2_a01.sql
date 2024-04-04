-- yos_S_pv_02 の答え合わせ

select distinct
    YYYYMM,
    ITEM_CD,
    PSI_ELEMENT,
    case when SITE_CD is null then '-' else SITE_CD end AS SITE_CD,
    BC_CD,
    sum(QUANTITY) as s_QUANTITY
from
  IT_TEST_DB.TEST_SCHEMA.PSI_MONTH_2309_10_11
where 
    ITEM_CD = 'VP4586040000'
    and BC_CD = 1033
    and YYYYMM >= 202304
group by
    YYYYMM,
    ITEM_CD,
    PSI_ELEMENT,
    case when SITE_CD is null then '-' else SITE_CD end,
    BC_CD

order by 
    YYYYMM, PSI_ELEMENT
