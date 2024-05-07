with

source as (

    select * from {{ source('eco', 'region') }}

    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line
    -- where ordered_at <= {   { var('truncate_timespan_to') }}
),

renamed as (

    select

        ----------  ids
        R_REGIONKEY as location_id,

        ---------- text
        ifnull(R_NAME,'---' || R_REGIONKEY) as location_name,

        -- ---------- numerics
        -- tax_rate,  　　-- 　サンプルにはなかった、、、

        -- ---------- timestamps
        -- {{dbt.date_trunc('day', 'opened_at')}} as opened_at

    from source

)

select * from renamed