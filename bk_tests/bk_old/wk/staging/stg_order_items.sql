
-- 参考
--  https://github.com/dbt-labs/jaffle-sl-template/blob/main/models/staging/stg_order_items.sql
--  カラム名は適当に合わせた

with source as (
    select * from {{ source('eco', 'lineitem') }}
    -- select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.lineitem
),

renamed as (

    select distinct 
        ----------  ids
        L_PARTKEY || L_ORDERKEY || L_SUPPKEY || L_RECEIPTDATE as order_item_id,
        L_ORDERKEY as order_id,
        L_SHIPMODE as product_id

    from source

)

select * from renamed
