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

# Fabric Notebook 01 - Generate sample Parquet source files
# Purpose:
#   Create controlled source data for a Fabric + dbt POC.
#   The data contains one initial FULL load and two INCREMENTAL batches.
#
# Assumption:
#   A Lakehouse is attached to the notebook as the default Lakehouse.
#
# Output:
#   Files/landing/<source>/<table>/<file>.parquet

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, date
import uuid

spark = SparkSession.builder.getOrCreate()

try:
    from notebookutils import mssparkutils
except ImportError:
    # Older Fabric runtimes may expose mssparkutils directly.
    import mssparkutils


# ============================================================
# Helpers
# ============================================================

LANDING_ROOT = "Files/landing"


def ensure_dir(path: str):
    """Create a directory if it does not exist."""
    try:
        mssparkutils.fs.mkdirs(path)
    except Exception:
        pass


def write_single_parquet(df, target_file: str):
    """
    Spark normally writes parquet as a folder with one or more part files.
    This helper writes a single partition to a temporary folder and then
    renames the generated part-*.parquet file to the requested filename.
    """

    target_dir = target_file.rsplit("/", 1)[0]
    ensure_dir(target_dir)

    temp_dir = f"{target_dir}/_tmp_{uuid.uuid4().hex}"

    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .parquet(temp_dir)
    )

    files = mssparkutils.fs.ls(temp_dir)

    parquet_parts = [
        f.path for f in files
        if f.name.startswith("part-") and f.name.endswith(".parquet")
    ]

    if len(parquet_parts) != 1:
        raise RuntimeError(
            f"Expected exactly one parquet part file in {temp_dir}, "
            f"found {len(parquet_parts)}."
        )

    # Remove previous file if it exists.
    try:
        mssparkutils.fs.rm(target_file, True)
    except Exception:
        pass

    mssparkutils.fs.mv(parquet_parts[0], target_file, True)
    mssparkutils.fs.rm(temp_dir, True)

    print(f"Wrote: {target_file} | rows={df.count()}")


# ============================================================
# Schemas
# ============================================================

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
    StructField("unit_price", DecimalType(12, 2), True),
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
    StructField("unit_price", DecimalType(12, 2), True),
    StructField("discount_pct", DecimalType(5, 2), True),
    StructField("modified_at", TimestampType(), True),
])


# ============================================================
# 1. CUSTOMER - FULL LOAD
# ============================================================

customer_full = [
    (1001, "Anna",   "Andersson",  "ANNA.ANDERSSON@example.se", "Stockholm",   "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1002, "Erik",   "Johansson",  "erik.johansson@example.se", "Göteborg",    "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1003, "Sara",   "Karlsson",   "sara.karlsson@example.se",  "Malmö",       "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1004, "Johan",  "Nilsson",    "johan.nilsson@example.se",  "Uppsala",     "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1005, "Maria",  "Eriksson",   "maria.eriksson@example.se", "Västerås",    "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1006, "Oskar",  "Larsson",    "oskar.larsson@example.se",  "Örebro",      "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1007, "Emma",   "Olsson",     "emma.olsson@example.se",    "Linköping",   "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1008, "Lars",   "Persson",    "lars.persson@example.se",   "Helsingborg", "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1009, "Sofia",  "Svensson",   "sofia.svensson@example.se", "Jönköping",   "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1010, "Nils",   "Gustafsson", "nils.gustafsson@example.se","Norrköping",  "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1011, "Elin",   "Pettersson", "elin.pettersson@example.se","Lund",        "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1012, "Karl",   "Jonsson",    "karl.jonsson@example.se",   "Umeå",        "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1013, "Maja",   "Jansson",    "maja.jansson@example.se",   "Gävle",       "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1014, "Anders", "Hansson",    "anders.hansson@example.se", "Borås",       "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1015, "Linda",  "Bengtsson",  "linda.bengtsson@example.se","Södertälje",  "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1016, "Per",    "Lindberg",   "per.lindberg@example.se",   "Karlstad",    "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1017, "Ida",    "Lindström",  "ida.lindstrom@example.se",  "Eskilstuna",  "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1018, "Gustav", "Lundberg",   "gustav.lundberg@example.se","Växjö",       "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1019, "Hanna",  "Berg",       "hanna.berg@example.se",     "Halmstad",    "inactive", datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (1020, "Mikael", "Bergström",  "mikael.bergstrom@example.se","Sundsvall",  "active",   datetime(2025,1,1), datetime(2026,8,1,8,0)),
]

df_customer_full = spark.createDataFrame(customer_full, customer_schema)


# ============================================================
# 2. PRODUCT - FULL LOAD
# ============================================================

from decimal import Decimal

product_full = [
    (501, "Laptop Pro 14",       "Computer",  Decimal("14999.00"), True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (502, "USB-C Dock",          "Accessory", Decimal("1899.00"),  True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (503, "Wireless Mouse",      "Accessory", Decimal("499.00"),   True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (504, "Mechanical Keyboard", "Accessory", Decimal("1099.00"),  True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (505, "27in Monitor",        "Monitor",   Decimal("3299.00"),  True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (506, "Webcam HD",           "Accessory", Decimal("699.00"),   True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (507, "Headset Pro",         "Accessory", Decimal("1299.00"),  True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (508, "Laptop Stand",        "Accessory", Decimal("599.00"),   True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (509, "USB-C Cable",         "Accessory", Decimal("199.00"),   True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
    (510, "Travel Adapter",      "Accessory", Decimal("349.00"),   True, datetime(2025,1,1), datetime(2026,8,1,8,0)),
]

df_product_full = spark.createDataFrame(product_full, product_schema)


# ============================================================
# 3. ORDERS - FULL LOAD
# ============================================================

orders_full = [
    (20001,1001,date(2026,7,1),"completed","SEK",datetime(2026,7,1,12,0)),
    (20002,1002,date(2026,7,2),"completed","SEK",datetime(2026,7,2,12,0)),
    (20003,1003,date(2026,7,3),"completed","SEK",datetime(2026,7,3,12,0)),
    (20004,1001,date(2026,7,4),"shipped","SEK",datetime(2026,7,4,12,0)),
    (20005,1004,date(2026,7,5),"completed","SEK",datetime(2026,7,5,12,0)),
    (20006,1005,date(2026,7,6),"completed","SEK",datetime(2026,7,6,12,0)),
    (20007,1006,date(2026,7,7),"completed","SEK",datetime(2026,7,7,12,0)),
    (20008,1007,date(2026,7,8),"shipped","SEK",datetime(2026,7,8,12,0)),
    (20009,1008,date(2026,7,9),"completed","SEK",datetime(2026,7,9,12,0)),
    (20010,1009,date(2026,7,10),"completed","SEK",datetime(2026,7,10,12,0)),
    (20011,1010,date(2026,7,11),"completed","SEK",datetime(2026,7,11,12,0)),
    (20012,1011,date(2026,7,12),"completed","SEK",datetime(2026,7,12,12,0)),
    (20013,1012,date(2026,7,13),"completed","SEK",datetime(2026,7,13,12,0)),
    (20014,1013,date(2026,7,14),"completed","SEK",datetime(2026,7,14,12,0)),
    (20015,1014,date(2026,7,15),"shipped","SEK",datetime(2026,7,15,12,0)),
    (20016,1015,date(2026,7,16),"completed","SEK",datetime(2026,7,16,12,0)),
    (20017,1016,date(2026,7,17),"completed","SEK",datetime(2026,7,17,12,0)),
    (20018,1017,date(2026,7,18),"completed","SEK",datetime(2026,7,18,12,0)),
    (20019,1018,date(2026,7,19),"completed","SEK",datetime(2026,7,19,12,0)),
    (20020,1020,date(2026,7,20),"completed","SEK",datetime(2026,7,20,12,0)),
]

df_orders_full = spark.createDataFrame(orders_full, orders_schema)


# ============================================================
# 4. ORDER_LINE - FULL LOAD
# ============================================================

order_line_full = [
    (30001,20001,501,1,Decimal("14999.00"),Decimal("0.00"),datetime(2026,7,1,12,0)),
    (30002,20001,502,1,Decimal("1899.00"), Decimal("5.00"),datetime(2026,7,1,12,0)),
    (30003,20002,503,2,Decimal("499.00"),  Decimal("0.00"),datetime(2026,7,2,12,0)),
    (30004,20003,505,1,Decimal("3299.00"), Decimal("0.00"),datetime(2026,7,3,12,0)),
    (30005,20004,504,1,Decimal("1099.00"), Decimal("0.00"),datetime(2026,7,4,12,0)),
    (30006,20005,506,1,Decimal("699.00"),  Decimal("0.00"),datetime(2026,7,5,12,0)),
    (30007,20006,507,1,Decimal("1299.00"), Decimal("10.00"),datetime(2026,7,6,12,0)),
    (30008,20007,508,1,Decimal("599.00"),  Decimal("0.00"),datetime(2026,7,7,12,0)),
    (30009,20008,509,3,Decimal("199.00"),  Decimal("0.00"),datetime(2026,7,8,12,0)),
    (30010,20009,510,2,Decimal("349.00"),  Decimal("0.00"),datetime(2026,7,9,12,0)),
    (30011,20010,503,1,Decimal("499.00"),  Decimal("5.00"),datetime(2026,7,10,12,0)),
    (30012,20011,502,1,Decimal("1899.00"), Decimal("0.00"),datetime(2026,7,11,12,0)),
    (30013,20012,505,2,Decimal("3299.00"), Decimal("0.00"),datetime(2026,7,12,12,0)),
    (30014,20013,506,1,Decimal("699.00"),  Decimal("0.00"),datetime(2026,7,13,12,0)),
    (30015,20014,507,1,Decimal("1299.00"), Decimal("0.00"),datetime(2026,7,14,12,0)),
    (30016,20015,501,1,Decimal("14999.00"),Decimal("5.00"),datetime(2026,7,15,12,0)),
    (30017,20016,508,2,Decimal("599.00"),  Decimal("0.00"),datetime(2026,7,16,12,0)),
    (30018,20017,509,4,Decimal("199.00"),  Decimal("0.00"),datetime(2026,7,17,12,0)),
    (30019,20018,504,1,Decimal("1099.00"), Decimal("10.00"),datetime(2026,7,18,12,0)),
    (30020,20019,503,1,Decimal("499.00"),  Decimal("0.00"),datetime(2026,7,19,12,0)),
    (30021,20020,505,1,Decimal("3299.00"), Decimal("0.00"),datetime(2026,7,20,12,0)),
]

df_order_line_full = spark.createDataFrame(order_line_full, order_line_schema)


# ============================================================
# 5. INCREMENTAL BATCH - 2026-08-20
# ============================================================

customer_inc_20260820 = [
    # Updates
    (1001,"Anna","Andersson","ANNA.ANDERSSON@example.se","Solna","active",
     datetime(2025,1,1),datetime(2026,8,20,9,15)),
    (1007,"Emma","Olsson","emma.olsson.new@example.se","Linköping","active",
     datetime(2025,1,1),datetime(2026,8,20,9,15)),
    (1012,"Karl","Jonsson","karl.jonsson@example.se","Umeå","inactive",
     datetime(2025,1,1),datetime(2026,8,20,9,15)),

    # Inserts
    (1021,"Alva","Nyström","alva.nystrom@example.se","Stockholm","active",
     datetime(2026,8,20),datetime(2026,8,20,9,15)),
    (1022,"Leo","Ek","leo.ek@example.se","Malmö","active",
     datetime(2026,8,20),datetime(2026,8,20,9,15)),
]

df_customer_inc_20260820 = spark.createDataFrame(
    customer_inc_20260820, customer_schema
)


product_inc_20260820 = [
    (502,"USB-C Dock","Accessory",Decimal("1799.00"),True,
     datetime(2025,1,1),datetime(2026,8,20,9,20)),
    (505,"27in Monitor","Monitor",Decimal("3099.00"),True,
     datetime(2025,1,1),datetime(2026,8,20,9,20)),
]

df_product_inc_20260820 = spark.createDataFrame(
    product_inc_20260820, product_schema
)


orders_inc_20260820 = [
    (20021,1021,date(2026,8,20),"completed","SEK",datetime(2026,8,20,10,0)),
    (20022,1003,date(2026,8,20),"shipped","SEK",datetime(2026,8,20,10,5)),
    (20023,1022,date(2026,8,20),"completed","SEK",datetime(2026,8,20,10,10)),
]

df_orders_inc_20260820 = spark.createDataFrame(
    orders_inc_20260820, orders_schema
)


order_line_inc_20260820 = [
    (30022,20021,501,1,Decimal("14999.00"),Decimal("0.00"),datetime(2026,8,20,10,0)),
    (30023,20021,502,1,Decimal("1799.00"), Decimal("5.00"),datetime(2026,8,20,10,0)),
    (30024,20022,505,2,Decimal("3099.00"), Decimal("0.00"),datetime(2026,8,20,10,5)),
    (30025,20023,503,1,Decimal("499.00"),  Decimal("0.00"),datetime(2026,8,20,10,10)),
]

df_order_line_inc_20260820 = spark.createDataFrame(
    order_line_inc_20260820, order_line_schema
)


# ============================================================
# 6. INCREMENTAL BATCH - 2026-08-21
# ============================================================

customer_inc_20260821 = [
    # Second update of same customer -> useful for SCD2
    (1001,"Anna","Andersson","anna.andersson@example.se","Solna","inactive",
     datetime(2025,1,1),datetime(2026,8,21,8,30)),

    # New customer
    (1023,"Nova","Holm","nova.holm@example.se","Uppsala","active",
     datetime(2026,8,21),datetime(2026,8,21,8,30)),
]

df_customer_inc_20260821 = spark.createDataFrame(
    customer_inc_20260821, customer_schema
)


orders_inc_20260821 = [
    # Update existing order from previous incremental batch
    (20022,1003,date(2026,8,20),"completed","SEK",datetime(2026,8,21,8,45)),

    # New order
    (20024,1023,date(2026,8,21),"completed","SEK",datetime(2026,8,21,9,0)),
]

df_orders_inc_20260821 = spark.createDataFrame(
    orders_inc_20260821, orders_schema
)


order_line_inc_20260821 = [
    (30026,20024,506,1,Decimal("699.00"),Decimal("0.00"),datetime(2026,8,21,9,0)),
    (30027,20024,509,2,Decimal("199.00"),Decimal("0.00"),datetime(2026,8,21,9,0)),
]

df_order_line_inc_20260821 = spark.createDataFrame(
    order_line_inc_20260821, order_line_schema
)


# ============================================================
# 7. WRITE ALL PARQUET FILES
# ============================================================

files_to_write = [
    (
        df_customer_full,
        f"{LANDING_ROOT}/crm/customer/customer_20260801_full.parquet"
    ),
    (
        df_product_full,
        f"{LANDING_ROOT}/product/product/product_20260801_full.parquet"
    ),
    (
        df_orders_full,
        f"{LANDING_ROOT}/sales/orders/orders_20260801_full.parquet"
    ),
    (
        df_order_line_full,
        f"{LANDING_ROOT}/sales/order_line/order_line_20260801_full.parquet"
    ),

    (
        df_customer_inc_20260820,
        f"{LANDING_ROOT}/crm/customer/customer_20260820_incremental.parquet"
    ),
    (
        df_product_inc_20260820,
        f"{LANDING_ROOT}/product/product/product_20260820_incremental.parquet"
    ),
    (
        df_orders_inc_20260820,
        f"{LANDING_ROOT}/sales/orders/orders_20260820_incremental.parquet"
    ),
    (
        df_order_line_inc_20260820,
        f"{LANDING_ROOT}/sales/order_line/order_line_20260820_incremental.parquet"
    ),

    (
        df_customer_inc_20260821,
        f"{LANDING_ROOT}/crm/customer/customer_20260821_incremental.parquet"
    ),
    (
        df_orders_inc_20260821,
        f"{LANDING_ROOT}/sales/orders/orders_20260821_incremental.parquet"
    ),
    (
        df_order_line_inc_20260821,
        f"{LANDING_ROOT}/sales/order_line/order_line_20260821_incremental.parquet"
    ),
]

for df, path in files_to_write:
    write_single_parquet(df, path)


# ============================================================
# 8. VALIDATION
# ============================================================

print("\nCreated sample source files successfully.\n")

for root in [
    f"{LANDING_ROOT}/crm/customer",
    f"{LANDING_ROOT}/product/product",
    f"{LANDING_ROOT}/sales/orders",
    f"{LANDING_ROOT}/sales/order_line",
]:
    print(root)
    for f in mssparkutils.fs.ls(root):
        print("   ", f.name)


# Optional sanity check:
print("\nCustomer incremental batch 2026-08-21:")
display(
    spark.read.parquet(
        f"{LANDING_ROOT}/crm/customer/customer_20260821_incremental.parquet"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
