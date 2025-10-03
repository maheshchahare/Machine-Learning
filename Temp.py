# Databricks notebook: Salesforce -> Unity Catalog Delta (Spark-only, optimized)
# Author: <you>
# Notes:
# - Store Salesforce secrets in Databricks Secret Scope.
# - This notebook expects delta and requests packages on the cluster.
# - Designed for sequential processing; add concurrency only after validating API quotas.

# -------------------------
# 0. Imports & Spark session
# -------------------------
import os, io, uuid, time, json, tempfile
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
from typing import List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# -------------------------
# 1. CONFIG - edit before running
# -------------------------
CONFIG = {
    "secret_scope": "sf_scope",               # Databricks secret scope
    "sf_client_id_key": "sf_client_id",
    "sf_client_secret_key": "sf_client_secret",
    "sf_refresh_token_key": "sf_refresh_token",
    "sf_instance_url_key": "sf_instance_url",# e.g. https://yourInstance.my.salesforce.com
    "api_version": "v60.0",
    "catalog": "your_catalog",               # Unity Catalog
    "meta_schema": "salesforce_meta",
    "bronze_schema": "salesforce_bronze",
    "temp_dbfs_dir": "/tmp/salesforce_ingest",
    "concurrent_objects": 3,                 # keep small
    "bulk_poll_interval_secs": 5,
    "rest_page_size": 2000,                  # SF default page size
    "default_lookback_days": 7               # first incremental fallback
}

# Example objects config (for 50 objects load from JSON/Delta instead)
OBJECTS = [
    {
        "object_api_name": "Account",
        "columns": ["Id", "Name", "Industry", "SystemModstamp", "IsDeleted"],
        "checkpoint_field": "SystemModstamp",
        "initial_load": True,
        "target_table": f"{CONFIG['catalog']}.{CONFIG['bronze_schema']}.account"
    },
    {
        "object_api_name": "Contact",
        "columns": ["Id", "FirstName", "LastName", "Email", "SystemModstamp", "IsDeleted"],
        "checkpoint_field": "SystemModstamp",
        "initial_load": False,
        "target_table": f"{CONFIG['catalog']}.{CONFIG['bronze_schema']}.contact"
    },
    # ... add more objects or load dynamically ...
]

# -------------------------
# 2. Load secrets & build base URLs
# -------------------------
def _secret(key: str) -> str:
    return dbutils.secrets.get(CONFIG["secret_scope"], key)

SF_CLIENT_ID = _secret(CONFIG["sf_client_id_key"])
SF_CLIENT_SECRET = _secret(CONFIG["sf_client_secret_key"])
SF_REFRESH_TOKEN = _secret(CONFIG["sf_refresh_token_key"])
SF_INSTANCE_URL = _secret(CONFIG["sf_instance_url_key"]).rstrip("/")
API_VERSION = CONFIG["api_version"]
BASE_URL = SF_INSTANCE_URL + f"/services/data/{API_VERSION}/"

# -------------------------
# 3. HTTP session with retries + token refresh
# -------------------------
session = requests.Session()
retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE"],
    backoff_factor=1
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.headers.update({"Content-Type": "application/json"})

def refresh_access_token():
    token_url = "https://login.salesforce.com/services/oauth2/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
        "refresh_token": SF_REFRESH_TOKEN
    }
    r = session.post(token_url, data=payload, timeout=30)
    r.raise_for_status()
    j = r.json()
    access_token = j.get("access_token")
    instance_url = j.get("instance_url", SF_INSTANCE_URL)
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    global SF_INSTANCE_URL, BASE_URL
    SF_INSTANCE_URL = instance_url.rstrip("/")
    BASE_URL = SF_INSTANCE_URL + f"/services/data/{API_VERSION}/"
    return access_token

# init token
try:
    refresh_access_token()
except Exception as e:
    raise RuntimeError("Failed to obtain Salesforce access token. Check secrets.") from e

def sf_request(method: str, url: str, **kwargs) -> requests.Response:
    """
    Wrapper that handles 401 refresh, 429 Retry-After, and uses session retries.
    Returns requests.Response (caller should handle resp.raise_for_status or content).
    """
    for attempt in range(4):
        resp = session.request(method, url, timeout=120, **kwargs)
        if resp.status_code == 401:
            # refresh token and retry
            refresh_access_token()
            continue
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", "60"))
            time.sleep(wait)
            continue
        return resp
    # final attempt's response
    resp.raise_for_status()
    return resp

# -------------------------
# 4. SF describe → Spark schema mapping
# -------------------------
SF_TO_SPARK = {
    "string": StringType(),
    "textarea": StringType(),
    "phone": StringType(),
    "email": StringType(),
    "url": StringType(),
    "boolean": BooleanType(),
    "int": LongType(),
    "double": DoubleType(),
    "currency": DoubleType(),
    "percent": DoubleType(),
    "date": DateType(),
    "datetime": TimestampType(),
    "reference": StringType(),
    "id": StringType()
}

def get_object_describe(object_api_name: str) -> Dict[str, Any]:
    url = urljoin(BASE_URL, f"sobjects/{object_api_name}/describe/")
    resp = sf_request("GET", url)
    resp.raise_for_status()
    return resp.json()

def build_spark_schema(describe_json: Dict[str, Any], selected_columns: List[str]) -> StructType:
    fields = []
    fields_meta = {f["name"]: f for f in describe_json.get("fields", [])}
    for col in selected_columns:
        meta = fields_meta.get(col)
        if not meta:
            # fallback
            fields.append(StructField(col, StringType(), True))
            continue
        sf_type = meta.get("type")
        spark_type = SF_TO_SPARK.get(sf_type, StringType())
        fields.append(StructField(col, spark_type, True))
    # keep ingestion metadata columns
    fields.append(StructField("_ingested_at", TimestampType(), True))
    fields.append(StructField("_source_object", StringType(), True))
    return StructType(fields)

# -------------------------
# 5. Metadata & error tables (Delta) - create if not exists
# -------------------------
META_TABLE = f"{CONFIG['catalog']}.{CONFIG['meta_schema']}.ingestion_metadata"
ERROR_TABLE = f"{CONFIG['catalog']}.{CONFIG['meta_schema']}.ingestion_errors"

# create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG['catalog']}.{CONFIG['meta_schema']}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CONFIG['catalog']}.{CONFIG['bronze_schema']}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
  object_name STRING,
  last_checkpoint TIMESTAMP,
  last_run TIMESTAMP,
  status STRING,
  row_count BIGINT,
  error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ERROR_TABLE} (
  object_name STRING,
  run_id STRING,
  error_time TIMESTAMP,
  error_message STRING,
  payload STRING
) USING DELTA
""")

def get_last_checkpoint(object_name: str) -> Optional[datetime]:
    q = f"SELECT last_checkpoint FROM {META_TABLE} WHERE object_name = '{object_name}' ORDER BY last_run DESC LIMIT 1"
    rows = spark.sql(q).collect()
    if not rows:
        return None
    return rows[0][0]

def write_metadata(object_name: str, last_checkpoint: Optional[datetime], status: str, row_count: int, error_message: Optional[str]=None):
    now = datetime.now(timezone.utc)
    rec = [(object_name, last_checkpoint, now, status, row_count, error_message)]
    df = spark.createDataFrame(rec, schema=["object_name","last_checkpoint","last_run","status","row_count","error_message"])
    df.write.format("delta").mode("append").saveAsTable(META_TABLE)

def log_error(object_name: str, run_id: str, error_message: str, payload: Optional[str]=None):
    rec = [(object_name, run_id, datetime.now(timezone.utc), error_message, payload)]
    df = spark.createDataFrame(rec, schema=["object_name","run_id","error_time","error_message","payload"])
    df.write.format("delta").mode("append").saveAsTable(ERROR_TABLE)

# -------------------------
# 6. Bulk API v2: submit, poll, download CSV → Spark
# -------------------------
def bulk_query_to_delta(object_api_name: str, soql: str, schema: StructType, target_table: str) -> int:
    jobs_url = urljoin(BASE_URL, "jobs/query")
    payload = {"query": soql}
    r = sf_request("POST", jobs_url, json=payload)
    r.raise_for_status()
    job = r.json()
    job_id = job["id"]
    status_url = jobs_url + f"/{job_id}"
    # poll
    while True:
        sresp = sf_request("GET", status_url)
        sresp.raise_for_status()
        status = sresp.json()
        state = status.get("state")
        if state in ("JobComplete", "Failed", "Aborted"):
            break
        time.sleep(CONFIG["bulk_poll_interval_secs"])
    if state != "JobComplete":
        raise RuntimeError(f"Bulk job {job_id} failed with state {state}: {status}")
    # download CSV results
    results_url = status_url + "/results"
    # stream to local file then mv to DBFS to avoid driver OOM
    local_tmp = f"/tmp/sf_{object_api_name}_{job_id}.csv"
    with sf_request("GET", results_url, stream=True) as resp:
        resp.raise_for_status()
        with open(local_tmp, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
    dbfs_path = f"{CONFIG['temp_dbfs_dir']}/{object_api_name}/{job_id}.csv"
    dbutils.fs.mkdirs(os.path.dirname(f"dbfs:{dbfs_path}"))
    # move local file into DBFS
    dbutils.fs.mv(f"file:{local_tmp}", f"dbfs:{dbfs_path}")
    # read CSV into Spark using schema (header expected)
    df = spark.read.option("header", True).schema(schema).csv(f"dbfs:{dbfs_path}")
    df = df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_object", F.lit(object_api_name))
    # write overwrite (initial load)
    df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    return df.count()

# -------------------------
# 7. REST incremental: paginated query → page-by-page Spark merge
# -------------------------
def rest_query_paginate_and_merge(object_api_name: str, soql: str, schema: StructType, target_table: str, key_col: str, checkpoint_field: str) -> int:
    query_url = urljoin(BASE_URL, "query")
    params = {"q": soql}
    total = 0
    resp = sf_request("GET", query_url, params=params)
    resp.raise_for_status()
    page = resp.json()
    while True:
        records = page.get("records", [])
        if records:
            run_id = uuid.uuid4().hex
            local_tmp = f"/tmp/{object_api_name}_page_{run_id}.json"
            # write newline-delimited JSON to local tmp file
            with open(local_tmp, "w", encoding="utf8") as fh:
                for r in records:
                    if "attributes" in r:
                        r.pop("attributes", None)
                    fh.write(json.dumps(r) + "\n")
            dbfs_tmp = f"{CONFIG['temp_dbfs_dir']}/{object_api_name}/pages/{run_id}.json"
            dbutils.fs.mkdirs(os.path.dirname(f"dbfs:{dbfs_tmp}"))
            dbutils.fs.mv(f"file:{local_tmp}", f"dbfs:{dbfs_tmp}")
            page_df = spark.read.schema(schema).json(f"dbfs:{dbfs_tmp}")
            page_df = page_df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_object", F.lit(object_api_name))
            # merge page
            merge_upsert(page_df, target_table, key_col, [f.name for f in schema.fields if f.name != "_ingested_at" and f.name != "_source_object"] + ["_ingested_at", "_source_object"])
            # handle deletes if present
            if "IsDeleted" in page_df.columns:
                apply_deletes(page_df, target_table, key_col)
            cnt = page_df.count()
            total += cnt
        # next page
        if page.get("done", True):
            break
        next_rel = page.get("nextRecordsUrl")
        if not next_rel:
            break
        next_full = urljoin(SF_INSTANCE_URL, next_rel)
        page_resp = sf_request("GET", next_full)
        page_resp.raise_for_status()
        page = page_resp.json()
    return total

# -------------------------
# 8. Delta merge helper (explicit mapping)
# -------------------------
def merge_upsert(updates_df, target_table_name: str, key_col: str, column_list: List[str]):
    """
    If target_table doesn't exist, create it from updates_df.
    Otherwise use DeltaTable.merge with explicit set mapping.
    """
    if not spark._jsparkSession.catalog().tableExists(target_table_name):
        updates_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
        return
    dt = DeltaTable.forName(spark, target_table_name)
    # build mapping dict for update (exclude key)
    set_dict = {c: f"u.{c}" for c in column_list if c != key_col}
    dt.alias("t").merge(updates_df.alias("u"), f"t.{key_col} = u.{key_col}") \
      .whenMatchedUpdate(set=set_dict) \
      .whenNotMatchedInsertAll() \
      .execute()

# -------------------------
# 9. Deletes handling helper
# -------------------------
def apply_deletes(df, target_table: str, key_col: str):
    if "IsDeleted" not in df.columns:
        return
    deleted_keys_df = df.filter(F.col("IsDeleted") == True).select(key_col).distinct()
    if deleted_keys_df.rdd.isEmpty():
        return
    tmp_view = f"__deleted_keys_{uuid.uuid4().hex}"
    deleted_keys_df.createOrReplaceTempView(tmp_view)
    # delete via SQL (Delta supports DELETE)
    spark.sql(f"DELETE FROM {target_table} WHERE {key_col} IN (SELECT {key_col} FROM {tmp_view})")
    spark.catalog.dropTempView(tmp_view)

# -------------------------
# 10. Utilities: safe SOQL builders
# -------------------------
def sf_dt_to_string(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def build_soql(object_name: str, columns: List[str], where: Optional[str] = None) -> str:
    cols = ", ".join(columns)
    soql = f"SELECT {cols} FROM {object_name}"
    if where:
        soql = f"{soql} WHERE {where}"
    return soql

# -------------------------
# 11. Main process per object
# -------------------------
def process_object(cfg: Dict[str, Any]):
    name = cfg["object_api_name"]
    cols = cfg["columns"]
    cp_field = cfg["checkpoint_field"]
    initial_load = cfg.get("initial_load", False)
    target = cfg["target_table"]
    key_col = "Id"
    run_id = uuid.uuid4().hex
    print(f"Processing {name} run_id={run_id} initial_load={initial_load}")

    try:
        descr = get_object_describe(name)
        schema = build_spark_schema(descr, cols)

        if initial_load:
            soql = build_soql(name, cols)
            row_count = bulk_query_to_delta(name, soql, schema, target)
            # compute max checkpoint if cp_field exists
            max_cp = None
            if cp_field:
                r = spark.sql(f"SELECT max({cp_field}) as mx FROM {target}").collect()
                max_cp = r[0]["mx"] if r and r[0]["mx"] is not None else None
            write_metadata(name, max_cp, "SUCCESS", row_count, None)
            print(f"Initial load done {name} rows={row_count}")
        else:
            last_cp = get_last_checkpoint(name)
            if last_cp:
                ts = sf_dt_to_string(last_cp)
                where = f"{cp_field} > {repr(ts)}"
            else:
                # safe lookback to avoid huge initial REST scan
                cut = datetime.now(timezone.utc) - timedelta(days=CONFIG["default_lookback_days"])
                where = f"{cp_field} >= {repr(sf_dt_to_string(cut))}"
            soql = build_soql(name, cols, where)
            processed = rest_query_paginate_and_merge(name, soql, schema, target, key_col, cp_field)
            # update metadata
            max_cp = None
            if cp_field:
                r = spark.sql(f"SELECT max({cp_field}) as mx FROM {target}").collect()
                max_cp = r[0]["mx"] if r and r[0]["mx"] is not None else None
            write_metadata(name, max_cp, "SUCCESS", processed, None)
            print(f"Incremental done {name} processed={processed}")

    except Exception as e:
        write_metadata(name, None, "FAILED", 0, str(e))
        log_error(name, run_id, str(e), payload=None)
        print(f"FAILED {name}: {e}")

# -------------------------
# 12. Runner loop (sequential)
# -------------------------
# ensure temp dir exists
dbutils.fs.mkdirs(CONFIG["temp_dbfs_dir"])

for o in OBJECTS:
    process_object(o)

print("All objects processed (sequential).")
