{{
    config(
        materialized = 'table'
    )
}}
with suppliers as (

    select * from {{ ref('base_supplier') }}

)
select 
    s.supplier_key,
    s.supplier_name,
    s.supplier_address,
    s.nation_key,
    s.supplier_phone_number,
    s.supplier_account_balance
from
    suppliers s
order by
    s.supplier_key