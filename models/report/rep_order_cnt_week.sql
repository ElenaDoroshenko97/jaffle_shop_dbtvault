with rep_order_cnt_week as (

    select ord.status
        ,d.week_id
        ,count(ord.order_pk) as cnt_order
    from {{ ref('sat_order_details') }} as ord 
    inner join {{ ref('dim_day') }} as d 
    on ord.order_date=d.date 
    group by ord.status
        ,d.week_id
)

select * from rep_order_cnt_week