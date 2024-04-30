
-- 販売・在庫サンプル用sql 
-- 参照テーブル　TEST_SCHEMA  MASTER_NATERUAL、SOH_SELL_TEST

with w_data_time as (
    select *
    from (
        select distinct
            RETAILER_NAME,
            WEEKLY_DATE,
            max(UPDATE_TIME) as max_UPDATE_TIME
        from 
            -- IT_TEST_DB.TEST_SCHEMA.SOH_SELL_TEST
            {{ source('soh', 'soh_sell_test') }}
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
    sum(QTY_SOLD) as weekly_QTY_SOLD,
 -- 日付型 
    split_part(WEEKLY_DATE,'/',1) || '-' || 
        lpad(split_part(WEEKLY_DATE,'/',2), 2,'0') || '-' || 
        lpad(split_part(WEEKLY_DATE,'/',3),2,'0') as date_week,
    PRODUCT_CODE || '_' || RETAILER_NAME as product_and_retailer  -- カラムを結合

from (
    select distinct
        t1.RETAILER_NAME,
        t1.WEEKLY_DATE,
        t2.PRODUCT_CODE,
        coalesce(t3.MATERIAL_name,t2.PRODUCT_CODE ) as MATERIAL_name,
        t2.QTYON_HAND,
        t2.QTY_SOLD
    from 
        w_data_time as t1
    inner join
        -- IT_TEST_DB.TEST_SCHEMA.SOH_SELL_TEST as t2
        {{ source('soh', 'soh_sell_test') }} as t2
    on 
        t1.RETAILER_NAME = t2.RETAILER_NAME
        and t1.WEEKLY_DATE = t2.WEEKLY_DATE

    left join
        -- IT_TEST_DB.TEST_SCHEMA.SOH_MASTER_NATERUAL as t3
        {{ source('soh', 'soh_master_naterual') }} as t3
    on 
        t2.PRODUCT_CODE = t3.MATERIAL_CODE
)
where
    PRODUCT_CODE is not null
    and WEEKLY_DATE >= '2023/1/1'
    and PRODUCT_CODE not like '%E+%'   -- 4.55E+12 等 csv作成時のデータ変換ミス、非表示にしておく
group by    
    RETAILER_NAME,
    WEEKLY_DATE,
    PRODUCT_CODE,
    MATERIAL_name

