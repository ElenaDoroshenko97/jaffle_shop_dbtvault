with rep_order_cnt_week as (

    select 
        date_trunc('week', sod.order_date) as order_week
        ,sod.status
        ,{{ dbt_utils.surrogate_key(["date_trunc('week', sod.order_date)", 'status']) }} as id
        ,count(sod.order_pk) as cnt_order
    from {{ ref('hub_order') }} as ho
    left join {{ ref('sat_order_details') }} as sod 
    on ho.order_pk=sod.order_pk
    -- inner join {{ ref('dim_day') }} as d 
    -- on ord.order_date=d.date 
    group by sod.status
        ,date_trunc('week', sod.order_date)
)

select * from rep_order_cnt_week