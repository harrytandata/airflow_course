{{
    config(
        materialized = 'table'
    )
}}
with regions as (

    select * from {{ ref('base_region') }}

)
select 
    r.region_key,
    r.region_name
from
    regions r
order by
    r.region_key