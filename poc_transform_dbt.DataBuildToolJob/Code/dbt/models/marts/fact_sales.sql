{{ config(
    materialized='incremental',
    schema='dbt_gold',
    unique_key='order_line_id'
) }}

with sales as (

    select *
    from {{ ref('int_sales_enriched') }}
-- gör fakta inkrementell
    {% if is_incremental() %}

    where order_line_modified_at >
        (
            select coalesce(
                max(order_line_modified_at),
                cast('1900-01-01' as datetime2)
            )
            from {{ this }}
        )

    {% endif %}

),

customers as (

    select *
    from {{ ref('dim_customer') }}

),

products as (

    select *
    from {{ ref('dim_product') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

)

select
    s.order_line_id,
    s.order_id,

    d.date_key,
    c.customer_key,
    p.product_key,

    s.customer_id,
    s.product_id,

    s.quantity,
    s.unit_price,
    s.discount_pct,

    s.gross_amount,
    s.discount_amount,
    s.net_amount,

    s.currency,
    s.order_status,

    s.order_modified_at,
    s.order_line_modified_at

from sales s

inner join customers c
    on s.customer_id = c.customer_id

inner join products p
    on s.product_id = p.product_id

inner join dates d
    on s.order_date = d.order_date