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

# Fabric Notebook 02 - Landing to Bronze Raw
# Purpose:
#   Move newly arrived Parquet files from transient Landing to immutable Bronze Raw.
#
# Pattern:
#   1. Discover configured source/table folders in Landing
#   2. Parse load date from filename
#   3. Copy file to Bronze Raw partitioned as yyyy/mm/dd
#   4. Validate target exists and size matches
#   5. Delete Landing file only after successful validation
#
# Assumption:
#   A Lakehouse is attached to this notebook as the default Lakehouse.
#
# Input example:
#   Files/landing/crm/customer/customer_20260820_incremental.parquet
#
# Output example:
#   Files/bronze-raw/crm/customer/2026/08/20/customer_20260820_incremental.parquet

from datetime import datetime
import re

try:
    from notebookutils import mssparkutils
except ImportError:
    import mssparkutils


# ============================================================
# Configuration
# ============================================================

LANDING_ROOT = "Files/landing"
BRONZE_RAW_ROOT = "Files/bronze-raw"

# Metadata-driven configuration.
# Add new entities here without changing the processing logic.
ENTITIES = [
    {"source": "crm",     "table": "customer"},
    {"source": "product", "table": "product"},
    {"source": "sales",   "table": "orders"},
    {"source": "sales",   "table": "order_line"},
]

# Supported file naming convention:
# <table>_YYYYMMDD_full.parquet
# <table>_YYYYMMDD_incremental.parquet
FILE_PATTERN = re.compile(
    r"^(?P<table>.+)_(?P<load_date>\d{8})_(?P<load_type>full|incremental)\.parquet$",
    re.IGNORECASE
)


# ============================================================
# Helpers
# ============================================================

def ensure_dir(path: str):
    """Create directory when missing."""
    try:
        mssparkutils.fs.mkdirs(path)
    except Exception:
        pass


def list_files(path: str):
    """
    Return only files from a Lakehouse folder.
    If the folder doesn't exist, return an empty list.
    """
    try:
        return [x for x in mssparkutils.fs.ls(path) if not x.isDir]
    except Exception:
        return []


def get_file_info(path: str):
    """
    Return file metadata from parent directory listing.
    """
    parent = path.rsplit("/", 1)[0]
    filename = path.rsplit("/", 1)[1]

    for item in mssparkutils.fs.ls(parent):
        if item.name == filename:
            return item

    return None


def parse_filename(filename: str):
    """
    Parse table, load date and load type from filename.
    """
    match = FILE_PATTERN.match(filename)

    if not match:
        raise ValueError(
            f"Filename does not match expected convention: {filename}"
        )

    values = match.groupdict()
    load_date = datetime.strptime(values["load_date"], "%Y%m%d")

    return {
        "table": values["table"],
        "load_date": load_date,
        "load_type": values["load_type"].lower(),
    }


def build_bronze_target(source: str, table: str, filename: str, load_date: datetime):
    """
    Build yyyy/mm/dd Bronze Raw target path.
    """
    return (
        f"{BRONZE_RAW_ROOT}/{source}/{table}/"
        f"{load_date:%Y}/{load_date:%m}/{load_date:%d}/"
        f"{filename}"
    )


def copy_and_validate(source_path: str, target_path: str):
    """
    Copy source -> target and verify:
      - target exists
      - source and target sizes match

    Returns a dict with validation details.
    """
    target_dir = target_path.rsplit("/", 1)[0]
    ensure_dir(target_dir)

    # Bronze Raw is immutable.
    # If exact file already exists, do not overwrite it.
    existing_target = get_file_info(target_path)

    if existing_target is not None:
        source_info = get_file_info(source_path)

        if source_info is None:
            raise RuntimeError(f"Source disappeared before copy: {source_path}")

        if source_info.size != existing_target.size:
            raise RuntimeError(
                "Bronze Raw target already exists but file sizes differ.\n"
                f"Source: {source_path} ({source_info.size} bytes)\n"
                f"Target: {target_path} ({existing_target.size} bytes)"
            )

        return {
            "copied": False,
            "already_exists": True,
            "source_size": source_info.size,
            "target_size": existing_target.size,
        }

    source_info_before = get_file_info(source_path)

    if source_info_before is None:
        raise RuntimeError(f"Source file not found: {source_path}")

    copy_result = mssparkutils.fs.cp(source_path, target_path)

    # Some runtimes return bool; others may not.
    if copy_result is False:
        raise RuntimeError(
            f"Copy returned False: {source_path} -> {target_path}"
        )

    target_info = get_file_info(target_path)

    if target_info is None:
        raise RuntimeError(
            f"Target not found after copy: {target_path}"
        )

    if source_info_before.size != target_info.size:
        raise RuntimeError(
            "Validation failed: source/target sizes differ.\n"
            f"Source: {source_info_before.size} bytes\n"
            f"Target: {target_info.size} bytes"
        )

    return {
        "copied": True,
        "already_exists": False,
        "source_size": source_info_before.size,
        "target_size": target_info.size,
    }


# ============================================================
# Processing
# ============================================================

results = []

for entity in ENTITIES:
    source = entity["source"]
    table = entity["table"]

    landing_dir = f"{LANDING_ROOT}/{source}/{table}"
    files = list_files(landing_dir)

    print(f"\nProcessing {source}.{table}")
    print(f"Landing folder: {landing_dir}")
    print(f"Files found: {len(files)}")

    for file_info in files:
        filename = file_info.name
        source_path = file_info.path

        result = {
            "source": source,
            "table": table,
            "filename": filename,
            "status": None,
            "message": None,
            "target_path": None,
        }

        try:
            metadata = parse_filename(filename)

            # Extra safety:
            # table encoded in filename should match configured table.
            if metadata["table"].lower() != table.lower():
                raise ValueError(
                    f"Filename table '{metadata['table']}' does not match "
                    f"configured table '{table}'."
                )

            target_path = build_bronze_target(
                source=source,
                table=table,
                filename=filename,
                load_date=metadata["load_date"],
            )

            result["target_path"] = target_path

            validation = copy_and_validate(
                source_path=source_path,
                target_path=target_path,
            )

            # Delete from Landing only after target validation.
            delete_result = mssparkutils.fs.rm(source_path, False)

            if delete_result is False:
                raise RuntimeError(
                    f"Bronze copy validated but Landing delete returned False: "
                    f"{source_path}"
                )

            result["status"] = "SUCCESS"

            if validation["already_exists"]:
                result["message"] = (
                    "Identical Bronze Raw file already existed; "
                    "Landing file removed after size validation."
                )
            else:
                result["message"] = (
                    f"Copied and validated "
                    f"({validation['target_size']} bytes), then removed from Landing."
                )

            print(f"  SUCCESS: {filename}")
            print(f"       -> {target_path}")

        except Exception as exc:
            # Important: leave failed file in Landing for investigation/retry.
            result["status"] = "FAILED"
            result["message"] = str(exc)

            print(f"  FAILED: {filename}")
            print(f"       {exc}")

        results.append(result)


# ============================================================
# Summary
# ============================================================

print("\n" + "=" * 80)
print("LANDING -> BRONZE RAW SUMMARY")
print("=" * 80)

success_count = sum(1 for r in results if r["status"] == "SUCCESS")
failed_count = sum(1 for r in results if r["status"] == "FAILED")

print(f"Processed : {len(results)}")
print(f"Successful: {success_count}")
print(f"Failed    : {failed_count}")

for r in results:
    print(
        f"{r['status']:7} | "
        f"{r['source']}.{r['table']} | "
        f"{r['filename']} | "
        f"{r['message']}"
    )


# ============================================================
# Fail notebook when one or more files failed
# ============================================================

if failed_count > 0:
    raise RuntimeError(
        f"{failed_count} file(s) failed. "
        "Failed files remain in Landing for retry."
    )

print("\nAll Landing files were successfully archived to Bronze Raw.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
