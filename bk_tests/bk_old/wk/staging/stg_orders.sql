
-- 参考
-- https://github.com/dbt-labs/jaffle-sl-template/blob/main/models/staging/stg_orders.sql
-- 　※カラム名は適当に変換した

with source as (
    select * from {{ source('eco', 'orders') }}
),

renamed as (

    select

        ----------  ids
        O_ORDERKEY as order_id,
        O_ORDERSTATUS as location_id,
        O_CUSTKEY as customer_id,

        ---------- numerics
        (O_TOTALPRICE / 100.0) as order_total,
        ( (O_TOTALPRICE * 1.1 ) / 100.0) as tax_paid,

        ---------- timestamps
        {{dbt.date_trunc('day','O_ORDERDATE')}} as ordered_at

    from source

)

select * from renamed