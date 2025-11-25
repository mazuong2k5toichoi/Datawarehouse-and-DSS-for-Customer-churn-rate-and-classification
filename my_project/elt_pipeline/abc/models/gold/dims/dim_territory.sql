-- models/gold/dims/dim_territory.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dim']
    )
}}

with territory_details as (
    select
        ht.territory_id,
        std.name as territory_name,
        std.country_region_code,
        std."group" as territory_group,
        std.sales_ytd,
        std.sales_last_year,
        std.cost_ytd,
        std.cost_last_year
    from {{ ref('hub_territory') }} ht
    left join {{ ref('sat_territory_details') }} std on ht.territory_hk = std.territory_hk
),

with_sk as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['territory_id']) }} as territory_sk
    from territory_details
)

select *
from with_sk