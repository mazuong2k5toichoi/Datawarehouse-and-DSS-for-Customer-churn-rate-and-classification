-- models/gold/dims/dim_address.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dim']
    )
}}

with address_details as (
    select
        ha.address_id,
        sad.address_line1,
        sad.address_line2,
        sad.city,
        sad.postal_code,
        sad.state_province_id
    from {{ ref('hub_address') }} ha
    left join {{ ref('sat_address_details') }} sad on ha.address_hk = sad.address_hk
)



select *
from address_details