{{
    config(
        materialized = 'ephemeral'
    )
}}
select
    ps_partkey as part_key,
    ps_suppkey as supplier_key,
    ps_availqty as supplier_availabe_quantity,
    ps_supplycost{{ money() }} as supplier_cost_amount,
    ps_comment as part_supplier_comment
from
    {{ source('tpch', 'partsupp') }}
