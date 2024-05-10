-- 実行日　の 1年の日付　　※毎日自動実行できればいいかも

select
    date_today,     -- 実行日
    date_last_day,
    date_start,     -- 期初 YYYY-04-01
    left(replace(cast(date_start as varchar), '-',''),6) sart_YYYYMM,               -- 期初 YYYY04
    left(replace(cast(date_start as varchar), '-','/'),7) sart_SCENARIO,   -- SCENARIO 期初　YYYY/04
    left(replace(cast(dateadd(month,-1,date_last_day) as varchar), '-','/'),7) end_SCENARIO,    -- 前日の前月  YYYY/MM    
    replace(cast(date_start as varchar), '-','') as yyyymmdd_startday,
    replace(cast(date_last_day as varchar), '-','') as yyyymmdd_lastdady,

    -- dateadd(year,1,date_start) as date_end, -- 次の期初 YYYY-04-01
    -- left(replace(cast(dateadd(year,1,date_start) as varchar), '-',''),6) end_YYYYMM,-- 次の期初 YYYY04

from (
    select
        current_date() as date_today,
        current_date() -1 as date_last_day,
        date(
            cast( 
                year(case when month(current_date()) > 9 then current_date() else  dateadd(Year,-1, current_date()) end)  as varchar) 
            || '-04-01'
        ) as date_start
)
