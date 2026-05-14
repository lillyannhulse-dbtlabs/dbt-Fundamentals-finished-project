select 
    customer_id,
    customer_name
from {{ source('jaffle_shop', 'customers') }}