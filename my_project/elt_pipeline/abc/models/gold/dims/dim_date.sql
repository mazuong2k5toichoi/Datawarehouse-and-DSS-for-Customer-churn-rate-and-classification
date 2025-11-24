-- models/gold/dims/dim_date.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dim']
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

dates as (
    select
        date_day as date,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dow from date_day) as day_of_week,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Month') as month_name,
        case when extract(dow from date_day) in (0,6) then true else false end as is_weekend
    from date_spine
),

with_sk as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['date']) }} as date_sk
    from dates
)

select *
from with_sk