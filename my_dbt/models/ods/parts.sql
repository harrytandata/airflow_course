{{
    config(
        materialized = 'table'
    )
}}
with parts as (

    select * from {{ ref('base_part') }}

)
select 
    p.part_key,
    p.part_name,
    p.part_manufacturer_name,
    p.part_brand_name,
    p.part_type_name,
    p.part_size,
    p.part_container_desc,
    p.retail_price
from
    parts p
order by
    p.part_key