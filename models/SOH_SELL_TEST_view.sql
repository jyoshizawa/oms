
-- SOH_SELL_TEST はデータをcsvで作成したもの

select 
    *,
    -- 日付型 
    split_part(WEEKLY_DATE,'/',1) || '-' || 
        lpad(split_part(WEEKLY_DATE,'/',2), 2,'0') || '-' || 
        lpad(split_part(WEEKLY_DATE,'/',3),2,'0') as date_week
from 
    IT_TEST_DB.TEST_SCHEMA.SOH_SELL_TEST
