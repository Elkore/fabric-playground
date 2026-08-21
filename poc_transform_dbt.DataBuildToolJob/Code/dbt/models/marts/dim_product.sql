{{ config(
    materialized='table',
    schema='dbt_gold'
) }}

select
    convert(
        varchar(64),
        hashbytes(
            'SHA2_256',
            concat('product|', cast(product_id as varchar(50)))
        ),
        2
    ) as product_key,

    product_id,
    product_name,
    category,
    unit_price,
    is_active,
    created_at,
    modified_at

from {{ ref('stg_product') }}