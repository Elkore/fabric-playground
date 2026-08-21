CREATE TABLE [dbt_gold].[dim_date] (

	[date_key] int NULL, 
	[order_date] date NULL, 
	[year] int NULL, 
	[month_number] int NULL, 
	[day_of_month] int NULL, 
	[month_name] varchar(30) NULL, 
	[weekday_name] varchar(30) NULL
);