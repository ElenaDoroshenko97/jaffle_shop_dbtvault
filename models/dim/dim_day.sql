with dim_day as (
    select date::date,
        extract('isodow' from date) as dow,
        to_char(date, 'dy') as day,
        extract('week' from date) as week,
        extract('year' from date)*100+extract('week' from date) as week_id,
        extract('year' from date) as year,
        extract('day' from (date + interval '2 month - 1 day')) = 29 as leap
        from generate_series(date '2012-01-01',
            date '2025-12-31',
            interval '1 day')
        as t(date)
        )

select * from dim_day
order by date desc