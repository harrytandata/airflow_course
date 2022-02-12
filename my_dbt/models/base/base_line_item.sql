{{
    config(
        materialized = 'ephemeral'
    )
}}
select
    l_orderkey as order_key,
    l_partkey as part_key,
    l_suppkey as supplier_key,
    l_linenumber as order_line_number,
    l_quantity as quantity,
    l_extendedprice{{ money() }} as extended_price,
    l_discount{{ money() }} as discount_percentage,
    l_tax{{ money() }} as tax_rate,
    l_returnflag as return_status_code,
    l_linestatus as order_line_status_code,
    l_shipdate as ship_date,
    l_commitdate as commit_date,
    l_receiptdate as receipt_date,
    l_shipinstruct as ship_instructions_desc,
    l_shipmode as ship_mode_name,
    l_comment as order_line_comment
from
    {{ source('tpch', 'lineitem') }}
