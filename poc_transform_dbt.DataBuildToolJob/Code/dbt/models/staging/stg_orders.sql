{{ config(
    materialized='view',
    schema='dbt_silver_staging'
) }}

with source as (

    select *
    from {{ source('sales', 'orders') }}

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

where rn = 1