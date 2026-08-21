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

# MARKDOWN ********************

# # 03 – Bronze Raw → Persistent Delta (improved observability)
# 
# Purpose:
# 
# `Files/bronze-raw/<source>/<table>/yyyy/mm/dd/*.parquet`
# → append-only Delta tables under
# `Tables/persistent_<source>/<table>`
# 
# Examples:
# - `persistent_crm.customer`
# - `persistent_product.product`
# - `persistent_sales.orders`
# - `persistent_sales.order_line`
# 
# The notebook is idempotent at file level: a Parquet file already registered as `SUCCESS`
# in `persistent_meta.ingest_log` is not loaded again.
# 
# The ingest log captures:
# - source/table/file
# - source date and load type
# - batch id
# - started/loaded timestamps
# - rows ingested
# - status
# - error message


# PARAMETERS CELL ********************

# PARAMETERS / CONFIG

BRONZE_RAW_ROOT = "Files/bronze-raw"

ENTITIES = [
    {"source": "crm",     "table": "customer"},
    {"source": "product", "table": "product"},
    {"source": "sales",   "table": "orders"},
    {"source": "sales",   "table": "order_line"},
]

META_SCHEMA = "persistent_meta"
META_TABLE = "ingest_log"

# Keep False for the POC so unexpected schema drift fails loudly.
ALLOW_SCHEMA_EVOLUTION = False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
import re
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)

try:
    from notebookutils import mssparkutils
except ImportError:
    import mssparkutils

batch_id = str(uuid.uuid4())
print("Batch ID:", batch_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helpers
# 
# Expected file names:
# 
# - `<table>_YYYYMMDD_full.parquet`
# - `<table>_YYYYMMDD_incremental.parquet`
# 
# Archive layout:
# 
# `Files/bronze-raw/<source>/<table>/yyyy/mm/dd/<file>.parquet`


# CELL ********************

FILE_PATTERN = re.compile(
    r"^(?P<table>.+)_(?P<source_date>\d{8})_(?P<load_type>full|incremental)\.parquet$",
    re.IGNORECASE
)

def utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def list_parquet_files_recursive(path: str):
    result = []

    try:
        items = mssparkutils.fs.ls(path)
    except Exception:
        return result

    for item in items:
        if item.isDir:
            result.extend(list_parquet_files_recursive(item.path))
        elif item.name.lower().endswith(".parquet"):
            result.append(item)

    return result


def parse_file_metadata(filename: str, expected_table: str):
    match = FILE_PATTERN.match(filename)

    if not match:
        raise ValueError(f"Unexpected filename: {filename}")

    values = match.groupdict()

    if values["table"].lower() != expected_table.lower():
        raise ValueError(
            f"Filename table '{values['table']}' does not match configured "
            f"table '{expected_table}'."
        )

    return {
        "source_date": datetime.strptime(
            values["source_date"], "%Y%m%d"
        ).date(),
        "load_type": values["load_type"].lower()
    }


def target_schema(source: str):
    return f"persistent_{source}"


def target_table_name(source: str, table: str):
    return f"{target_schema(source)}.{table}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create schemas and ingest log
# 
# For a schema-enabled Fabric Lakehouse, `saveAsTable("persistent_crm.customer")`
# creates the Delta table under `Tables/persistent_crm/customer`.


# CELL ********************

for entity in ENTITIES:
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {target_schema(entity['source'])}"
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

ingest_log_schema = StructType([
    StructField("source_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("source_date", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("loaded_at", TimestampType(), True),
    StructField("rows_ingested", LongType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
])

meta_full_name = f"{META_SCHEMA}.{META_TABLE}"

if not spark.catalog.tableExists(meta_full_name):
    empty_log = spark.createDataFrame([], ingest_log_schema)

    (
        empty_log.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(meta_full_name)
    )

print("Metadata table:", meta_full_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

loaded_files = {
    row["source_file"]
    for row in (
        spark.table(meta_full_name)
        .filter(F.col("status") == "SUCCESS")
        .select("source_file")
        .distinct()
        .collect()
    )
}

print(f"Files already loaded successfully: {len(loaded_files)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load archived Parquet files
# 
# Persistent Delta is append-only. No business-key deduplication is done here.
# 
# Technical columns added to each persistent table:
# - `_source_file`
# - `_source_date`
# - `_load_type`
# - `_ingested_at`
# - `_batch_id`


# CELL ********************

results = []

for entity in ENTITIES:
    source = entity["source"]
    table = entity["table"]

    source_root = f"{BRONZE_RAW_ROOT}/{source}/{table}"
    target = target_table_name(source, table)

    files = sorted(
        list_parquet_files_recursive(source_root),
        key=lambda x: x.path
    )

    print("\n" + "=" * 80)
    print(f"{source}.{table} -> {target}")
    print(f"Archived parquet files found: {len(files)}")

    for file_info in files:
        source_file = file_info.path
        filename = file_info.name

        if source_file in loaded_files:
            print(f"SKIP    {filename} (already loaded)")
            results.append((source, table, filename, "SKIPPED"))
            continue

        started_at = utc_now_naive()
        source_date = None
        load_type = None
        rows_ingested = None

        try:
            metadata = parse_file_metadata(filename, table)

            source_date = metadata["source_date"].isoformat()
            load_type = metadata["load_type"]

            df = spark.read.parquet(source_file)

            # Count before write so we know exactly how many source rows
            # this file contributes to the persistent table.
            rows_ingested = df.count()

            ingest_ts = utc_now_naive()

            df_out = (
                df
                .withColumn("_source_file", F.lit(source_file))
                .withColumn(
                    "_source_date",
                    F.lit(source_date).cast("date")
                )
                .withColumn("_load_type", F.lit(load_type))
                .withColumn(
                    "_ingested_at",
                    F.lit(ingest_ts).cast("timestamp")
                )
                .withColumn("_batch_id", F.lit(batch_id))
            )

            writer = (
                df_out.write
                .format("delta")
                .mode("append")
            )

            if ALLOW_SCHEMA_EVOLUTION:
                writer = writer.option("mergeSchema", "true")

            writer.saveAsTable(target)

            loaded_at = utc_now_naive()

            success_row = [(
                source,
                table,
                source_file,
                source_date,
                load_type,
                batch_id,
                started_at,
                loaded_at,
                rows_ingested,
                "SUCCESS",
                None
            )]

            (
                spark.createDataFrame(
                    success_row,
                    ingest_log_schema
                )
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(meta_full_name)
            )

            loaded_files.add(source_file)

            print(
                f"SUCCESS {filename} | "
                f"{rows_ingested} rows"
            )

            results.append(
                (source, table, filename, "SUCCESS")
            )

        except Exception as exc:
            loaded_at = utc_now_naive()
            error_message = str(exc)

            failed_row = [(
                source,
                table,
                source_file,
                source_date,
                load_type,
                batch_id,
                started_at,
                loaded_at,
                rows_ingested,
                "FAILED",
                error_message[:4000]
            )]

            (
                spark.createDataFrame(
                    failed_row,
                    ingest_log_schema
                )
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(meta_full_name)
            )

            print(f"FAILED  {filename}")
            print(error_message)

            results.append(
                (source, table, filename, "FAILED")
            )

            # Fail fast so orchestration/pipeline sees the failure.
            raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation


# CELL ********************

print("Persistent table row counts:")

for entity in ENTITIES:
    target = target_table_name(
        entity["source"],
        entity["table"]
    )

    if spark.catalog.tableExists(target):
        count = spark.table(target).count()
        print(f"{target:35} {count:>6} rows")
    else:
        print(f"{target:35} MISSING")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Customer 1001 history:")

display(
    spark.table("persistent_crm.customer")
    .filter(F.col("customer_id") == 1001)
    .orderBy("modified_at")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Order 20022 history:")

display(
    spark.table("persistent_sales.orders")
    .filter(F.col("order_id") == 20022)
    .orderBy("modified_at")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Ingest log:")

display(
    spark.table(meta_full_name)
    .orderBy(
        F.col("started_at").desc(),
        "source_name",
        "table_name"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Potential ingestion anomalies:")

display(
    spark.table(meta_full_name)
    .filter(
        (F.col("status") != "SUCCESS")
        | (F.col("rows_ingested") == 0)
    )
    .orderBy(F.col("started_at").desc())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
