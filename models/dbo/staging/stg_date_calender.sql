-- 実行日　の 1年の日付　　※毎日自動実行できればいいかも

select
    date_today,     -- 実行日
    date_last_day,
    date_start,     -- 期初 YYYY-04-01
    date_last_month,  -- 実行日前日の、前月末日
    left(replace(cast(date_start as varchar), '-',''),6) sart_YYYYMM,               -- 期初 YYYY04
    left(replace(cast(date_start as varchar), '-','/'),7) sart_SCENARIO,   -- SCENARIO 期初　YYYY/04
    left(replace(cast(dateadd(month,-1,date_last_day) as varchar), '-','/'),7) end_SCENARIO,    -- 前日の前月  YYYY/MM    
    replace(cast(date_start as varchar), '-','') as yyyymmdd_startday,
    replace(cast(date_last_month as varchar), '-','') as yyyymmdd_lastmonth   --前日の前月
from (
    select
        current_date() as date_today,
        current_date() -1 as date_last_day,
        last_day(dateadd(month, -1, current_date() -1 )) as date_last_month,
        date(
            cast( 
                year(case when month(current_date()) > 9 then current_date() else  dateadd(Year,-1, current_date()) end)  as varchar) 
            || '-04-01'
        ) as date_start
)
