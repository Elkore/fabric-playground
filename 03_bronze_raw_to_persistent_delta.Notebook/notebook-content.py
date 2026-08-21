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

# Fabric Notebook 03 - Bronze Raw -> Persistent Delta
# Attach the schema-enabled Lakehouse as the default Lakehouse.

from datetime import datetime, timezone
import re
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

try:
    from notebookutils import mssparkutils
except ImportError:
    import mssparkutils

BRONZE_RAW_ROOT = "Files/bronze-raw"

ENTITIES = [
    {"source": "crm",     "table": "customer"},
    {"source": "product", "table": "product"},
    {"source": "sales",   "table": "orders"},
    {"source": "sales",   "table": "order_line"},
]

META_SCHEMA = "persistent_meta"
META_TABLE = "ingest_log"
ALLOW_SCHEMA_EVOLUTION = False
batch_id = str(uuid.uuid4())

FILE_PATTERN = re.compile(
    r"^(?P<table>.+)_(?P<source_date>\d{8})_(?P<load_type>full|incremental)\.parquet$",
    re.IGNORECASE,
)


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
            f"Filename table '{values['table']}' does not match configured table '{expected_table}'."
        )

    return {
        "source_date": datetime.strptime(values["source_date"], "%Y%m%d").date(),
        "load_type": values["load_type"].lower(),
    }


def target_schema(source: str):
    return f"persistent_{source}"


def target_table_name(source: str, table: str):
    return f"{target_schema(source)}.{table}"


# 1. Create source-specific persistent schemas and metadata schema.
for entity in ENTITIES:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema(entity['source'])}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {META_SCHEMA}")

# 2. Create file-level ingest log.
ingest_log_schema = StructType([
    StructField("source_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("source_date", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("batch_id", StringType(), False),
    StructField("loaded_at", TimestampType(), False),
    StructField("status", StringType(), False),
])

meta_full_name = f"{META_SCHEMA}.{META_TABLE}"

if not spark.catalog.tableExists(meta_full_name):
    (
        spark.createDataFrame([], ingest_log_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(meta_full_name)
    )

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

print("Batch ID:", batch_id)
print("Already loaded files:", len(loaded_files))

# 3. Load each archived Parquet file exactly once into append-only Delta.
results = []

for entity in ENTITIES:
    source = entity["source"]
    table = entity["table"]
    source_root = f"{BRONZE_RAW_ROOT}/{source}/{table}"
    target = target_table_name(source, table)

    files = sorted(list_parquet_files_recursive(source_root), key=lambda x: x.path)

    print("\n" + "=" * 80)
    print(f"{source}.{table} -> {target}")
    print(f"Files found: {len(files)}")

    for file_info in files:
        source_file = file_info.path
        filename = file_info.name

        if source_file in loaded_files:
            print(f"SKIP    {filename} (already loaded)")
            results.append((source, table, filename, "SKIPPED"))
            continue

        metadata = parse_file_metadata(filename, table)
        ingest_ts = datetime.now(timezone.utc).replace(tzinfo=None)

        df = spark.read.parquet(source_file)

        df_out = (
            df
            .withColumn("_source_file", F.lit(source_file))
            .withColumn("_source_date", F.lit(metadata["source_date"].isoformat()).cast("date"))
            .withColumn("_load_type", F.lit(metadata["load_type"]))
            .withColumn("_ingested_at", F.lit(ingest_ts).cast("timestamp"))
            .withColumn("_batch_id", F.lit(batch_id))
        )

        writer = df_out.write.format("delta").mode("append")
        if ALLOW_SCHEMA_EVOLUTION:
            writer = writer.option("mergeSchema", "true")

        writer.saveAsTable(target)

        log_row = [(
            source,
            table,
            source_file,
            metadata["source_date"].isoformat(),
            metadata["load_type"],
            batch_id,
            ingest_ts,
            "SUCCESS",
        )]

        (
            spark.createDataFrame(log_row, ingest_log_schema)
            .write.format("delta")
            .mode("append")
            .saveAsTable(meta_full_name)
        )

        loaded_files.add(source_file)
        row_count = df_out.count()
        print(f"SUCCESS {filename} | {row_count} rows")
        results.append((source, table, filename, "SUCCESS"))

# 4. Validation.
print("\nPersistent table counts")
for entity in ENTITIES:
    target = target_table_name(entity["source"], entity["table"])
    if spark.catalog.tableExists(target):
        print(f"{target:35} {spark.table(target).count():>6} rows")
    else:
        print(f"{target:35} MISSING")

print("\nCustomer 1001 history")
display(
    spark.table("persistent_crm.customer")
    .filter(F.col("customer_id") == 1001)
    .orderBy("modified_at")
)

print("\nOrder 20022 history")
display(
    spark.table("persistent_sales.orders")
    .filter(F.col("order_id") == 20022)
    .orderBy("modified_at")
)

print("\nIngest log")
display(
    spark.table(meta_full_name)
    .orderBy("loaded_at", "source_name", "table_name")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
