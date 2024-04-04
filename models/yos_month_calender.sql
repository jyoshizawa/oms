select 
    YYYYMM,
    last_month
from (
    -- lag関数　2個出ちゃう、、なんでだろ？？ 　　→ whereで応急処置
    select distinct
            YYYYMM,
            ifnull( lag(YYYYMM) ignore nulls over (order by YYYYMM), YYYYMM) as last_month,
            min(YYYYMM) over (order by YYYYMM) as min_month
        from 
            {{ ref('yos_S_monthly_quantity') }} 
)
where 
    YYYYMM <> last_month
    or YYYYMM = min_month
    
order by 1