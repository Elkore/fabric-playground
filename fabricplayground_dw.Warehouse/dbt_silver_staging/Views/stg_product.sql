-- Auto Generated (Do not modify) D4E55E1A59CCFB2FD7054ADDE9828ACD6AB9EBAC6DE1B77727F6D76443AAB159
create view [dbt_silver_staging].[stg_product] as 

with source as (

    select *
    from [fabricplayground].[persistent_product].[product]

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

where rn = 1;