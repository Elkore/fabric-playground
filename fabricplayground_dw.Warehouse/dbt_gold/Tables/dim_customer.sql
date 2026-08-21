CREATE TABLE [dbt_gold].[dim_customer] (

	[customer_key] varchar(64) NULL, 
	[customer_id] int NULL, 
	[first_name] varchar(8000) NULL, 
	[last_name] varchar(8000) NULL, 
	[email] varchar(8000) NULL, 
	[city] varchar(8000) NULL, 
	[customer_status] varchar(8000) NULL, 
	[created_at] datetime2(6) NULL, 
	[modified_at] datetime2(6) NULL
);