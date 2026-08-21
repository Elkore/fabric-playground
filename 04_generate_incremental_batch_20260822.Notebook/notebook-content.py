# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2548e105-7312-4786-b780-7a7daf967a18",
# META       "default_lakehouse_name": "fabricplayground",
# META       "default_lakehouse_workspace_id": "55978fac-a8b3-4bf8-8cef-5c8a33580e6b",
# META       "known_lakehouses": [
# META         {
# META           "id": "2548e105-7312-4786-b780-7a7daf967a18"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# 04 - Generate incremental batch 2026-08-22
from pyspark.sql.types import *
from datetime import datetime, date
from decimal import Decimal
import uuid

try:
    from notebookutils import mssparkutils
except ImportError:
    import mssparkutils

LANDING_ROOT = "Files/landing"

def ensure_dir(path):
    try:
        mssparkutils.fs.mkdirs(path)
    except Exception:
        pass

def write_single_parquet(df, target_file):
    target_dir = target_file.rsplit("/", 1)[0]
    ensure_dir(target_dir)

    temp_dir = f"{target_dir}/_tmp_{uuid.uuid4().hex}"
    df.coalesce(1).write.mode("overwrite").parquet(temp_dir)

    parts = [
        f.path for f in mssparkutils.fs.ls(temp_dir)
        if f.name.startswith("part-") and f.name.endswith(".parquet")
    ]

    if len(parts) != 1:
        raise RuntimeError(f"Expected one parquet part, found {len(parts)}")

    try:
        mssparkutils.fs.rm(target_file, True)
    except Exception:
        pass

    mssparkutils.fs.mv(parts[0], target_file, True)
    mssparkutils.fs.rm(temp_dir, True)

    print(f"Wrote {target_file} | rows={df.count()}")

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("modified_at", TimestampType(), True),
])

product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DecimalType(12,2), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("modified_at", TimestampType(), True),
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), True),
    StructField("order_status", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("modified_at", TimestampType(), True),
])

order_line_schema = StructType([
    StructField("order_line_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(12,2), True),
    StructField("discount_pct", DecimalType(5,2), True),
    StructField("modified_at", TimestampType(), True),
])

df_customer = spark.createDataFrame([
    (1023, "Nova", "Holm", "nova.holm@example.se", "Stockholm", "active",
     datetime(2026,8,21), datetime(2026,8,22,8,0)),
    (1024, "Ella", "Dahl", "ella.dahl@example.se", "Göteborg", "active",
     datetime(2026,8,22), datetime(2026,8,22,8,0)),
], customer_schema)

df_product = spark.createDataFrame([
    (509, "USB-C Cable", "Accessory", Decimal("179.00"), True,
     datetime(2025,1,1), datetime(2026,8,22,8,5)),
    (511, "Portable SSD 1TB", "Storage", Decimal("1299.00"), True,
     datetime(2026,8,22), datetime(2026,8,22,8,5)),
], product_schema)

df_orders = spark.createDataFrame([
    (20024, 1023, date(2026,8,21), "returned", "SEK", datetime(2026,8,22,8,15)),
    (20025, 1024, date(2026,8,22), "completed", "SEK", datetime(2026,8,22,8,20)),
], orders_schema)

df_order_line = spark.createDataFrame([
    # Update existing line: quantity 2 -> 3
    (30027, 20024, 509, 3, Decimal("199.00"), Decimal("0.00"),
     datetime(2026,8,22,8,15)),
    # New lines
    (30028, 20025, 511, 1, Decimal("1299.00"), Decimal("0.00"),
     datetime(2026,8,22,8,20)),
    (30029, 20025, 503, 2, Decimal("499.00"), Decimal("10.00"),
     datetime(2026,8,22,8,20)),
], order_line_schema)

files = [
    (df_customer, f"{LANDING_ROOT}/crm/customer/customer_20260822_incremental.parquet"),
    (df_product, f"{LANDING_ROOT}/product/product/product_20260822_incremental.parquet"),
    (df_orders, f"{LANDING_ROOT}/sales/orders/orders_20260822_incremental.parquet"),
    (df_order_line, f"{LANDING_ROOT}/sales/order_line/order_line_20260822_incremental.parquet"),
]

for df, path in files:
    write_single_parquet(df, path)

print("Incremental batch 2026-08-22 created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
