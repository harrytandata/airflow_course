{{
    config(
        materialized = 'ephemeral'
    )
}}
select
    p_partkey as part_key,
    p_name as part_name,
    p_mfgr as part_manufacturer_name,
    p_brand as part_brand_name,
    p_type as part_type_name,
    p_size as part_size,
    p_container as part_container_desc,
    p_retailprice as retail_price,
    p_comment as part_comment
from
    {{ source('tpch', 'part') }}
