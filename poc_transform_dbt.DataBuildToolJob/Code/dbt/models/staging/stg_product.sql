{{ config(
    materialized='view',
    schema='dbt_silver_staging'
) }}

with source as (

    select *
    from {{ source('product', 'product') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by product_id
            order by
                modified_at desc,
                _ingested_at desc
        ) as rn

    from source

)

select
    product_id,
    trim(product_name) as product_name,
    trim(category) as category,
    unit_price,
    is_active,
    created_at,
    modified_at,
    _source_date,
    _load_type,
    _ingested_at,
    _batch_id

from ranked

where rn = 1