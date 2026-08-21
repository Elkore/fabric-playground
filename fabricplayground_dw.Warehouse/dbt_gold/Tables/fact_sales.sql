CREATE TABLE [dbt_gold].[fact_sales] (

	[order_line_id] int NULL, 
	[order_id] int NULL, 
	[date_key] int NULL, 
	[customer_key] varchar(64) NULL, 
	[product_key] varchar(64) NULL, 
	[customer_id] int NULL, 
	[product_id] int NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(12,2) NULL, 
	[discount_pct] decimal(5,2) NULL, 
	[gross_amount] decimal(18,2) NULL, 
	[discount_amount] decimal(18,2) NULL, 
	[net_amount] decimal(18,2) NULL, 
	[currency] varchar(8000) NULL, 
	[order_status] varchar(8000) NULL, 
	[order_modified_at] datetime2(6) NULL, 
	[order_line_modified_at] datetime2(6) NULL
);