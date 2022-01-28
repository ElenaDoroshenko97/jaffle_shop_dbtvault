with dim_week as (
    select
        extract('year' from date) as year,
        extract('week' from date) as week,
        extract('year' from date)*100+extract('week' from date) as week_id

        from generate_series(date '2012-01-01',
            date '2025-12-31',
            interval '1 week')
        as t(date)
)

select * from dim_week