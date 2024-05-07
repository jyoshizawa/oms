
--  参考
-- 　https://github.com/dbt-labs/jaffle-sl-template/blob/main/models/staging/stg_customers.sql

with source as (
    select * from {{ source('eco', 'customer') }}
    -- select * from 
    --     SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
),

renamed as (
    select
        C_CUSTKEY as customer_id,
        C_NAME  as customer_name
    from source
)

select * from renamed
