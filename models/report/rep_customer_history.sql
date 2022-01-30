{{
    config(
        enabled=True,
        materialized = 'view'
    )
}}

with pit_cust as(
    select hc.customer_pk
        ,sc.first_name
        ,sc.last_name
        ,sc.email
        ,sc.effective_from
        ,coalesce(lead(sc.effective_from) over(partition by sc.customer_pk order by sc.effective_from)-interval '1 second'
            ,'5999-12-31 00:00:00'::timestamp) as effective_to
from {{ ref('hub_customer')}} as hc 
    left join {{ref('sat_customer_details')}} as sc 
on hc.customer_pk=sc.customer_pk
)

select t.* 
    ,'{{ var('given_dttm') }}'::timestamp(0) as given_dttm
from pit_cust t
where 1=1
    and '{{ var('given_dttm') }}'::timestamp between effective_from and effective_to
