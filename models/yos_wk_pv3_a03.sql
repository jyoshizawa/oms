-- yos_S_pv_03 の答え合わせ  ※Excel csvで　ざっと大丈夫そう



select
    YYYYMM,
    SITE_CD,
    BC_CD,
    VP4586040000_diff_QUANTITY
from 
--  {{ ref('yos_S_pv_03') }} 
  IT_TEST_DB.DBT_4_YOSHI.YOS_S_PV_03
where 
    -- ITEM_CD = 'VP4586040000'
    BC_CD = 1033
    and YYYYMM >= 202210

order by 
    YYYYMM
