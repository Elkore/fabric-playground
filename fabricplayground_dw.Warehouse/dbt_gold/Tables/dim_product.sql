CREATE TABLE [dbt_gold].[dim_product] (

	[product_key] varchar(64) NULL, 
	[product_id] int NULL, 
	[product_name] varchar(8000) NULL, 
	[category] varchar(8000) NULL, 
	[unit_price] decimal(12,2) NULL, 
	[is_active] bit NULL, 
	[created_at] datetime2(6) NULL, 
	[modified_at] datetime2(6) NULL
);