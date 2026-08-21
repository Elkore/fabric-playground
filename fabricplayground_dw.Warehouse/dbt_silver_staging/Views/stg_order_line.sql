-- Auto Generated (Do not modify) 2EB04EB0BEB369A5ADA4550BDA4B12BE37B17A6CF53D9F7E5B1E8EF55A274B70
create view [dbt_silver_staging].[stg_order_line] as 

with source as (

    select *
    from [fabricplayground].[persistent_sales].[order_line]

),

ranked as (

    select
        *,
        row_number() over (
            partition by order_line_id
            order by
                modified_at desc,
                _ingested_at desc
        ) as rn

    from source

)

select
    order_line_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    modified_at,
    _source_date,
    _load_type,
    _ingested_at,
    _batch_id

from ranked

where rn = 1;