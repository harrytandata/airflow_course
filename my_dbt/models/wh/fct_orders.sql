{{
    config(
        materialized = 'table'
    )
}}
with orders as (
    
    select * from {{ ref('orders') }}

),
orders_items as (
    
    select * from {{ ref('orders_items') }}

),
order_item_summary as (

    select 
        o.order_key,
        sum(o.gross_item_sales_amount) as gross_item_sales_amount,
        sum(o.item_discount_amount) as item_discount_amount,
        sum(o.item_tax_amount) as item_tax_amount,
        sum(o.net_item_sales_amount) as net_item_sales_amount
    from orders_items o
    group by
        1
),
final as (

    select 

        o.order_key, 
        o.order_date,
        o.customer_key,
        o.order_status_code,
        o.order_priority_code,
        o.order_clerk_name,
        o.shipping_priority,
                
        1 as order_count,                
        s.gross_item_sales_amount,
        s.item_discount_amount,
        s.item_tax_amount,
        s.net_item_sales_amount
    from
        orders o
        join
        order_item_summary s
            on o.order_key = s.order_key
)
select 
    f.*,
    {{ dbt_housekeeping() }}
from
    final f
order by
    f.order_date