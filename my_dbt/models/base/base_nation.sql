{{
    config(
        materialized = 'ephemeral'
    )
}}
select
    n_nationkey as nation_key,
    n_name as nation_name,
    n_regionkey as region_key,
    n_comment as nation_comment
from
    {{ source('tpch', 'nation') }}
