{{
    config(
        required_tests=None
    )
}}
-- config added to make this model bypass required tests
select
    customer_id,
    order_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_date']) }} as primary_key,
    count(*) as c
from {{ ref("stg_jaffle_shop__orders")}}
group by 1,2