{{ config(
    materialized='view',
    schema='dbt_silver_staging'
) }}

with source as (

    select *
    from {{ source('crm', 'customer') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by
                modified_at desc,
                _ingested_at desc
        ) as rn

    from source

)

select
    customer_id,
    trim(first_name)       as first_name,
    trim(last_name)        as last_name,
    lower(trim(email))     as email,
    city,
    status,
    created_at,
    modified_at,

    -- technical metadata
    _source_date,
    _load_type,
    _ingested_at,
    _batch_id

from ranked

where rn = 1