-- 参考
-- 　https://github.com/dbt-labs/jaffle-sl-template/blob/main/models/staging/stg_supplies.sql


with source as (
    select * from {{ source('eco', 'supplier') }}
),

renamed as (

    select

        ----------  ids
        -- { { dbt_utils.generate_surrogate_key(['S_SUPPKEY', 'S_NATIONKEY']) }} as supply_uuid,
        S_SUPPKEY || '_' || S_NATIONKEY AS supply_uuid,  -- 仮設定
        S_SUPPKEY as supply_id,
        S_NATIONKEY as product_id,

        ---------- text
        S_NAME as supply_name,

        ---------- numerics
        (S_ACCTBAL / 100.0) as supply_cost,

        ---------- booleans
        -- 1 as is_perishable_supply

    from source

)

select * from renamed