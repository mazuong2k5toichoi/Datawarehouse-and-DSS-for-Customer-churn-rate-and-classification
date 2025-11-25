-- models/gold/dims/dim_customer.sql
{{
    config(
        materialized='table',
        tags=['gold', 'dim']
    )
}}

with customer_base as (
    select
        hc.customer_hk,
        hc.customer_id,
        lcp.person_hk,
        lct.territory_hk
    from {{ ref('hub_customer') }} hc
    left join {{ ref('link_customer_person') }} lcp on hc.customer_hk = lcp.customer_hk
    left join {{ ref('link_customer_territory') }} lct on hc.customer_hk = lct.customer_hk
),

person_details as (
    select
        hp.person_hk,
        hp.business_entity_id,
        spd.first_name,
        spd.last_name,
        spd.person_type,
        spc.email_address
    from {{ ref('hub_person') }} hp
    left join {{ ref('sat_person_details') }} spd on hp.person_hk = spd.person_hk
    left join {{ ref('sat_person_contact') }} spc on hp.person_hk = spc.person_hk
),

territory_details as (
    select
        ht.territory_hk,
        ht.territory_id,
        std.name as territory_name,
        std.country_region_code,
        std."group" as territory_group
    from {{ ref('hub_territory') }} ht
    left join {{ ref('sat_territory_details') }} std on ht.territory_hk = std.territory_hk
),

customer_address as (
    select
        lpa.person_hk,
        sad.address_line1,
        sad.city,
        sad.postal_code
    from {{ ref('link_person_address') }} lpa
    join {{ ref('sat_address_details') }} sad on lpa.address_hk = sad.address_hk
),

combined as (
    select
        cb.customer_id,
        pd.business_entity_id,
        pd.first_name,
        pd.last_name,
        pd.email_address,
        td.territory_name,
        td.country_region_code,
        td.territory_group,
        ca.address_line1,
        ca.city,
        ca.postal_code
    from customer_base cb
    left join person_details pd on cb.person_hk = pd.person_hk
    left join territory_details td on cb.territory_hk = td.territory_hk
    left join customer_address ca on pd.person_hk = ca.person_hk
)


select *
from combined