{{ config(
    materialized='table',
    schema='dbt_gold'
) }}

select
    convert(
        varchar(64),
        hashbytes(
            'SHA2_256',
            concat('customer|', cast(customer_id as varchar(50)))
        ),
        2
    ) as customer_key,

    customer_id,
    first_name,
    last_name,
    email,
    city,
    status as customer_status,
    created_at,
    modified_at

from {{ ref('stg_customer') }}