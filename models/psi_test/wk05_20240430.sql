{% set SCENARIO_start = "2023/01" %}
{% set SCENARIO_end = "2023/11" %}

with w_d_list as (
    {{
        dbt.date_spine(
            'month',
            "to_date('01/01/2022','mm/dd/yyyy')",
            "to_date('01/01/2027','mm/dd/yyyy')"
        )
    }}
)

w_term_month as (
    select
        to_char(
            cast(left('{{SCENARIO_start}}',4) as int) +
            case when right( '{{SCENARIO_start}}' ,2) in ('01','02','03') then -1 else 0 end
        ) || '-04-01' as start_year_month,

        to_char(
            cast(left('{{SCENARIO_start}}',4) as int) +
            case when right( '{{SCENARIO_start}}' ,2) in ('01','02','03') then 0 else 1 end
        ) || '-03-01' as end_year_month
)

select
    

-- select 
--     cast(date_month as date) as date_month
-- from 
--     w_d_list
