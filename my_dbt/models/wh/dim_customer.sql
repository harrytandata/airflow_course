{{
    config(
        materialized = 'table'
    )
}}
with customers as (

    select * from {{ ref('customers') }}

),
nations as (

    select * from {{ ref('nations') }}
),
regions as (

    select * from {{ ref('regions') }}

),
final as (
    select 
        c.customer_key,
        c.customer_name,
        c.customer_address,
        n.nation_key as customer_nation_key,
        n.nation_name as customer_nation_name,
        r.region_key as customer_region_key,
        r.region_name as customer_region_name,
        c.customer_phone_number,
        c.customer_account_balance,
        c.customer_market_segment_name
    from
        customers c
        join
        nations n
            on c.nation_key = n.nation_key
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
    f.customer_key
