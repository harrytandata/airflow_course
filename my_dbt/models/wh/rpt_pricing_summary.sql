{{
    config(
        materialized = 'table'
    )
}}

/*
Per TPC-H Spec: 
2.4.1 Pricing Summary Report Query (Q1)
*/

select 
    f.return_status_code,
    f.order_line_status_code,
    sum(f.quantity) as quantity,
    sum(f.gross_item_sales_amount) as gross_item_sales_amount,
    sum(f.discounted_item_sales_amount) as discounted_item_sales_amount,
    sum(f.net_item_sales_amount) as net_item_sales_amount,

    avg(f.quantity) as avg_quantity,
    avg(f.base_price) as avg_base_price,
    avg(f.discount_percentage) as avg_discount_percentage,

    sum(f.order_item_count) as order_item_count
    
from
    {{ ref('fct_orders_items') }} f
where
    f.ship_date <= {{ dbt_utils.dateadd('day', -90, var('max_ship_date')) }}
group by
    1,2    