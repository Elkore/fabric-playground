-- Auto Generated (Do not modify) 6A23254755D2D408AE3A165F04C5D99FC681D6BC09024EF47394304247530718
create view [dbt_silver_staging].[stg_orders] as 

with source as (

    select *
    from [fabricplayground].[persistent_sales].[orders]

),

ranked as (

    select
        *,
        row_number() over (
            partition by order_id
            order by
                modified_at desc,
                _ingested_at desc
        ) as rn

    from source

)

select
    order_id,
    customer_id,
    order_date,
    lower(trim(order_status)) as order_status,
    upper(trim(currency)) as currency,
    modified_at,
    _source_date,
    _load_type,
    _ingested_at,
    _batch_id

from ranked

where rn = 1;