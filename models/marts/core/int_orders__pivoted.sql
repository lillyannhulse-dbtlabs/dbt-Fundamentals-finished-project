with payments as 
(
    select * 
    from {{ ref("stg_stripe__payments")}}
    where status = 'success'
)
, pivoted as 
(
    select order_id,
        {# creating a list for the jinja loop#}
        {%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}
        {# to control whitespace in the sql use '-' on either side of the jinja delimiter#}
        {% for payment_method in payment_methods %}

             sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}
            {# controlling the sql to remove the trailing commar as it will cause problems in the sql#}
             {%- if not loop.last -%}
                ,
             {%- endif -%}
             
        {% endfor %}
        

    from payments
    group by 1
)
select * 
from pivoted