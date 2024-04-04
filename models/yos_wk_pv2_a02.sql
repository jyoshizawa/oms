-- yos_S_pv_02 の答え合わせ  yos_wk_pv2_a01  大丈夫そう

select * 
from 
 {{ ref('yos_S_pv_02') }} 

where 
    ITEM_CD = 'VP4586040000'
    and BC_CD = 1033
    and YYYYMM >= 202210

-- order by 
--     YYYYMM
