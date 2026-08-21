{{ config(
    materialized='view',
    schema='dbt_silver_staging'
) }}

with source as (

    select *
    from {{ source('sales', 'order_line') }}

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

where rn = 1