{{
    config(
        materialized = 'table'
    )
}}
with suppliers as (

    select * from {{ ref('suppliers') }}

),
parts as (
    
    select * from {{ ref('parts') }}

),
parts_suppliers as (

    select * from {{ ref('parts_suppliers') }}

),
nations as (

    select * from {{ ref('nations') }}
),
regions as (

    select * from {{ ref('regions') }}

),
final as (

    select 
        ps.part_supplier_key,

        p.part_key,
        p.part_name,
        p.part_manufacturer_name,
        p.part_brand_name,
        p.part_type_name,
        p.part_size,
        p.part_container_desc,
        p.retail_price,

        s.supplier_key,
        s.supplier_name,
        supplier_address,
        s.supplier_phone_number,
        s.supplier_account_balance,
        n.nation_key as supplier_nation_key,
        n.nation_name as supplier_nation_name,
        r.region_key as supplier_region_key,
        r.region_name as supplier_region_name,

        ps.supplier_availabe_quantity,
        ps.supplier_cost_amount
    from
        parts p
        join
        parts_suppliers ps
            on p.part_key = ps.part_key
        join
        suppliers s
            on ps.supplier_key = s.supplier_key
        join
        nations n
            on s.nation_key = n.nation_key
        join
        regions r
            on n.region_key = r.region_key
)
select 
    f.*,
    {{ dbt_housekeeping() }}
from
    final f
order by
    f.part_key,
    f.supplier_key