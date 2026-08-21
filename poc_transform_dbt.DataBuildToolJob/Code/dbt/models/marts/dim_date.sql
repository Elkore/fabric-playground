{{ config(
    materialized='table',
    schema='dbt_gold'
) }}

with dates as (

    select distinct
        order_date
    from {{ ref('stg_orders') }}

)

select
    -- cast(format(order_date, 'yyyyMMdd') as int) as date_key, -- funkar inte härfor
    year(order_date) * 10000
        + month(order_date) * 100
        + day(order_date) as date_key,
    order_date,
    year(order_date) as year,
    month(order_date) as month_number,
    day(order_date) as day_of_month,
    cast(datename(month, order_date) as varchar(30)) as month_name,
    cast(datename(weekday, order_date) as varchar(30)) as weekday_name

from dates