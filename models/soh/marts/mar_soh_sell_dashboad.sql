
-- 販売・在庫サンプル用sql 
-- 参照テーブル　MASTER_NATERUAL、TEST_SOH_SELLL
-- 　※いろいろ作っていてsqlがごちゃごちゃしているので注意

with w_data_time as (
    select *
    from (
        select distinct
            RETAILER_NAME,
            WEEKLY_DATE,
            max(UPDATE_TIME) as max_UPDATE_TIME
        from 
            TEST_SOH_SELLL
        group by
            RETAILER_NAME,
            WEEKLY_DATE
    )
    where 
        RETAILER_NAME is not null
        and WEEKLY_DATE is not null
)

select 
    RETAILER_NAME,
    WEEKLY_DATE,
    PRODUCT_CODE,
    MATERIAL_name,
    sum(QTYON_HAND) as weekly_QTYON_HAND,
    sum(QTY_SOLD) as weekly_QTY_SOLD
from (
    select distinct
        t1.RETAILER_NAME,
        t1.WEEKLY_DATE,
        t2.PRODUCT_CODE,
        t3.MATERIAL_name,
        t2.QTYON_HAND,
        t2.QTY_SOLD
    from 
        w_data_time as t1
    inner join
        test_soh_selll as t2 
    on 
        t1.RETAILER_NAME = t2.RETAILER_NAME
        and t1.WEEKLY_DATE = t2.WEEKLY_DATE

    left join
        master_naterual as t3
    on 
        t2.PRODUCT_CODE = t3.MATERIAL_CODE
)
where
    PRODUCT_CODE is not null
group by    
    RETAILER_NAME,
    WEEKLY_DATE,
    PRODUCT_CODE,
    MATERIAL_name

