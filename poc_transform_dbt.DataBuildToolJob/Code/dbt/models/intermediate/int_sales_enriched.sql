{{ config(
    materialized='view',
    schema='dbt_silver_intermediate'
) }}

with orders as (

    select *
    from {{ ref('stg_orders') }}

),

order_lines as (

    select *
    from {{ ref('stg_order_line') }}

),

customers as (

    select *
    from {{ ref('stg_customer') }}

),

products as (

    select *
    from {{ ref('stg_product') }}

),

enriched as (

    select
        -- Order
        o.order_id,
        o.order_date,
        o.order_status,
        o.currency,

        -- Customer
        o.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.status as customer_status,

        -- Order line
        ol.order_line_id,
        ol.product_id,
        ol.quantity,
        ol.unit_price,
        ol.discount_pct,

        -- Product
        p.product_name,
        p.category,

        -- Calculated measures
        cast(
            ol.quantity * ol.unit_price
            as decimal(18,2)
        ) as gross_amount,

        cast(
            ol.quantity * ol.unit_price
            * coalesce(ol.discount_pct, 0) / 100.0
            as decimal(18,2)
        ) as discount_amount,

        cast(
            ol.quantity * ol.unit_price
            * (1 - coalesce(ol.discount_pct, 0) / 100.0)
            as decimal(18,2)
        ) as net_amount,

        -- Technical
        o.modified_at as order_modified_at,
        ol.modified_at as order_line_modified_at

    from order_lines ol

    inner join orders o
        on ol.order_id = o.order_id

    inner join customers c
        on o.customer_id = c.customer_id

    inner join products p
        on ol.product_id = p.product_id

)

select *
from enriched