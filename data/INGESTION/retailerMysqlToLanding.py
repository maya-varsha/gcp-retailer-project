import csv
import io
import json
import os
import tempfile
import datetime
import decimal

import mysql.connector

from google.cloud import storage
from google.cloud import bigquery


# ============================================================
# CONFIGURATION
# ============================================================

GCS_BUCKET = "datalake-project-bkt-22082026"

CONFIG_FILE = "configs/retailer_config.csv"

LANDING_PREFIX = "landing/retailer-db"
ARCHIVE_PREFIX = "landing/retailer-db/archive"
LOG_PREFIX = "temp/pipeline_logs"

BQ_PROJECT = "project-bd10f83d-812d-48fb-93c"

BQ_DATASET = "temp_dataset_maya"

BQ_AUDIT_TABLE = (
    f"{BQ_PROJECT}.{BQ_DATASET}.audit_log"
)

BQ_LOG_TABLE = (
    f"{BQ_PROJECT}.{BQ_DATASET}.pipeline_logs"
)


# ============================================================
# CLOUD SQL MYSQL CONFIGURATION
# ============================================================

MYSQL_CONFIG = {
    "host": "34.56.79.236",
    "port": 3306,
    "database": "retailerDB",
    "user": "myuser",
    "password": "Jdsports@1234",
    "connection_timeout": 30
}


# ============================================================
# EXTRACTION SETTINGS
# ============================================================

FETCH_SIZE = 5000


# ============================================================
# GOOGLE CLOUD CLIENTS
# ============================================================

storage_client = storage.Client(
    project=BQ_PROJECT
)

bq_client = bigquery.Client(
    project=BQ_PROJECT
)


# ============================================================
# LOGGING
# ============================================================

log_entries = []


def log_event(event_type, message, table=None):

    timestamp = (
        datetime.datetime.utcnow()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    entry = {
        "timestamp": timestamp,
        "event_type": event_type,
        "message": message,
        "table": table
    }

    log_entries.append(entry)

    print(
        f"[{timestamp}] "
        f"{event_type} - "
        f"{message}"
    )


# ============================================================
# UNIVERSAL JSON CONVERTER
# ============================================================

def make_json_safe(value):

    if value is None:
        return None

    # --------------------------------------------------------
    # DECIMAL
    # --------------------------------------------------------

    if isinstance(value, decimal.Decimal):

        # Convert Decimal to float
        return float(value)

    # --------------------------------------------------------
    # DATETIME
    # --------------------------------------------------------

    if isinstance(value, datetime.datetime):

        return value.isoformat(
            sep=" "
        )

    # --------------------------------------------------------
    # DATE
    # --------------------------------------------------------

    if isinstance(value, datetime.date):

        return value.isoformat()

    # --------------------------------------------------------
    # TIME
    # --------------------------------------------------------

    if isinstance(value, datetime.time):

        return value.isoformat()

    # --------------------------------------------------------
    # BYTES
    # --------------------------------------------------------

    if isinstance(value, bytes):

        try:

            return value.decode(
                "utf-8"
            )

        except Exception:

            return value.hex()

    # --------------------------------------------------------
    # EVERYTHING ELSE
    # --------------------------------------------------------

    return value


# ============================================================
# READ CONFIG FILE FROM GCS
# ============================================================

def read_config_file():

    bucket = storage_client.bucket(
        GCS_BUCKET
    )

    blob = bucket.blob(
        CONFIG_FILE
    )

    content = blob.download_as_text(
        encoding="utf-8"
    )

    reader = csv.DictReader(
        io.StringIO(content)
    )

    rows = list(reader)

    if not reader.fieldnames:

        raise RuntimeError(
            "Config file has no header."
        )

    columns = [
        column.strip()
        for column in reader.fieldnames
    ]

    required_columns = [
        "database",
        "datasource",
        "tablename",
        "loadtype",
        "watermark",
        "is_active",
        "targetpath"
    ]

    missing = [
        column
        for column in required_columns
        if column not in columns
    ]

    if missing:

        raise RuntimeError(
            "Missing config columns: "
            + ", ".join(missing)
        )

    print()
    print("==========================================")
    print("CONFIGURATION FILE")
    print("==========================================")

    print("Columns found:")

    for column in columns:

        print(
            f"  {column}"
        )

    print("==========================================")
    print()

    log_event(
        "SUCCESS",
        (
            f"Successfully read config file. "
            f"Found {len(rows)} rows."
        )
    )

    return rows


# ============================================================
# CREATE MYSQL CONNECTION
# ============================================================

def create_mysql_connection():

    try:

        log_event(
            "INFO",
            "Connecting to Cloud SQL MySQL..."
        )

        connection = mysql.connector.connect(

            host=MYSQL_CONFIG["host"],

            port=MYSQL_CONFIG["port"],

            database=MYSQL_CONFIG["database"],

            user=MYSQL_CONFIG["user"],

            password=MYSQL_CONFIG["password"],

            connection_timeout=30,

            ssl_disabled=False,

            use_pure=True
        )

        if connection.is_connected():

            log_event(
                "SUCCESS",
                "Cloud SQL MySQL connection successful."
            )

            return connection

        return None

    except mysql.connector.Error as e:

        log_event(
            "ERROR",
            f"MySQL connection failed: {str(e)}"
        )

        return None


# ============================================================
# GET LATEST WATERMARK
# ============================================================

def get_latest_watermark(table):

    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = @table
        AND status = 'SUCCESS'
    """

    try:

        job_config = (
            bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "table",
                        "STRING",
                        table
                    )
                ]
            )
        )

        result = bq_client.query(
            query,
            job_config=job_config
        ).result()

        for row in result:

            if row.latest_timestamp:

                return row.latest_timestamp

    except Exception as e:

        log_event(
            "WARNING",
            (
                f"Could not read watermark for "
                f"{table}: {str(e)}"
            ),
            table
        )

    return "2026-08-22 00:00:00"


# ============================================================
# BUILD SQL QUERY
# ============================================================

def build_query(
    table,
    load_type,
    watermark_column
):

    load_type = (
        str(load_type)
        .strip()
        .lower()
    )

    # --------------------------------------------------------
    # FULL LOAD
    # --------------------------------------------------------

    if load_type == "full load":

        return (
            f"SELECT * FROM `{table}`",
            None
        )

    # --------------------------------------------------------
    # INCREMENTAL
    # --------------------------------------------------------

    if load_type == "incremental":

        if not watermark_column:

            raise ValueError(
                f"Watermark column missing for {table}"
            )

        last_watermark = (
            get_latest_watermark(table)
        )

        if hasattr(
            last_watermark,
            "strftime"
        ):

            last_watermark = (
                last_watermark.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

        log_event(
            "INFO",
            (
                f"Latest watermark for "
                f"{table}: {last_watermark}"
            ),
            table
        )

        return (
            f"SELECT * "
            f"FROM `{table}` "
            f"WHERE `{watermark_column}` > %s",
            (
                last_watermark,
            )
        )

    raise ValueError(
        f"Unsupported load type: {load_type}"
    )


# ============================================================
# ARCHIVE EXISTING FILES
# ============================================================

def archive_existing_files(table):

    bucket = storage_client.bucket(
        GCS_BUCKET
    )

    prefix = (
        f"{LANDING_PREFIX}/{table}/"
    )

    blobs = bucket.list_blobs(
        prefix=prefix
    )

    existing_files = []

    for blob in blobs:

        if "/archive/" in blob.name:
            continue

        if blob.name.endswith(".json"):

            existing_files.append(blob)

    if not existing_files:

        log_event(
            "INFO",
            "No existing files to archive.",
            table
        )

        return

    for blob in existing_files:

        filename = (
            blob.name.split("/")[-1]
        )

        name_without_extension = (
            filename.rsplit(".", 1)[0]
        )

        parts = (
            name_without_extension.split("_")
        )

        if len(parts) < 2:
            continue

        date_part = parts[-1]

        if len(date_part) != 8:
            continue

        day = date_part[:2]
        month = date_part[2:4]
        year = date_part[4:]

        archive_path = (
            f"{ARCHIVE_PREFIX}/"
            f"{table}/"
            f"{year}/"
            f"{month}/"
            f"{day}/"
            f"{filename}"
        )

        bucket.copy_blob(
            blob,
            bucket,
            archive_path
        )

        blob.delete()

        log_event(
            "SUCCESS",
            (
                f"Archived {filename} to "
                f"{archive_path}"
            ),
            table
        )


# ============================================================
# MYSQL -> GCS
# ============================================================

def extract_mysql_to_gcs(
    table,
    load_type,
    watermark_column,
    target_path
):

    connection = None
    cursor = None
    temp_path = None

    try:

        # ----------------------------------------------------
        # BUILD QUERY
        # ----------------------------------------------------

        query, parameters = build_query(
            table,
            load_type,
            watermark_column
        )

        log_event(
            "INFO",
            f"SQL query prepared for {table}",
            table
        )

        # ----------------------------------------------------
        # CONNECT
        # ----------------------------------------------------

        connection = (
            create_mysql_connection()
        )

        if connection is None:

            log_event(
                "ERROR",
                (
                    f"Skipping {table} because "
                    "MySQL connection failed."
                ),
                table
            )

            return 0

        # ----------------------------------------------------
        # CURSOR
        # ----------------------------------------------------

        cursor = connection.cursor(
            buffered=False
        )

        # ----------------------------------------------------
        # EXECUTE
        # ----------------------------------------------------

        if parameters:

            cursor.execute(
                query,
                parameters
            )

        else:

            cursor.execute(
                query
            )

        # ----------------------------------------------------
        # COLUMN NAMES
        # ----------------------------------------------------

        columns = [
            column[0]
            for column in cursor.description
        ]

        # ----------------------------------------------------
        # TEMP FILE
        # ----------------------------------------------------

        temporary_file = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            delete=False
        )

        temp_path = (
            temporary_file.name
        )

        record_count = 0

        # ----------------------------------------------------
        # STREAM DATA
        # ----------------------------------------------------

        while True:

            rows = cursor.fetchmany(
                FETCH_SIZE
            )

            if not rows:
                break

            for row in rows:

                record = {}

                for index, value in enumerate(row):

                    record[
                        columns[index]
                    ] = make_json_safe(
                        value
                    )

                # IMPORTANT:
                # default=str guarantees that any
                # unexpected MySQL type does not
                # break the pipeline.

                json_record = json.dumps(
                    record,
                    ensure_ascii=False,
                    default=str
                )

                temporary_file.write(
                    json_record
                )

                temporary_file.write("\n")

                record_count += 1

        temporary_file.close()

        # ----------------------------------------------------
        # NO DATA
        # ----------------------------------------------------

        if record_count == 0:

            log_event(
                "INFO",
                (
                    "No records found. "
                    "No GCS file created."
                ),
                table
            )

            return 0

        # ----------------------------------------------------
        # GCS FILE PATH
        # ----------------------------------------------------

        today = (
            datetime.datetime.utcnow()
            .strftime("%d%m%Y")
        )

        target_path = (
            str(target_path)
            .strip()
            .strip("/")
        )

        filename = (
            f"{table}_{today}.json"
        )

        gcs_path = (
            f"{target_path}/{filename}"
        )

        # ----------------------------------------------------
        # UPLOAD
        # ----------------------------------------------------

        bucket = storage_client.bucket(
            GCS_BUCKET
        )

        blob = bucket.blob(
            gcs_path
        )

        blob.upload_from_filename(
            temp_path,
            content_type="application/json"
        )

        log_event(
            "SUCCESS",
            (
                f"{record_count} records successfully "
                f"written to "
                f"gs://{GCS_BUCKET}/{gcs_path}"
            ),
            table
        )

        return record_count

    except Exception as e:

        log_event(
            "ERROR",
            (
                f"Error processing {table}: "
                f"{str(e)}"
            ),
            table
        )

        return 0

    finally:

        if cursor is not None:

            try:
                cursor.close()
            except Exception:
                pass

        if connection is not None:

            try:
                connection.close()
            except Exception:
                pass

        if temp_path:

            try:

                if os.path.exists(temp_path):

                    os.remove(temp_path)

            except Exception:
                pass


# ============================================================
# WRITE AUDIT TO BIGQUERY
# ============================================================

def write_audit(
    table,
    load_type,
    record_count
):

    # IMPORTANT:
    # BigQuery insert_rows_json expects JSON-compatible
    # values. Therefore datetime is converted to string.

    timestamp = (
        datetime.datetime.utcnow()
        .isoformat()
    )

    row = {

        "tablename": table,

        "load_type": load_type,

        "record_count": int(
            record_count
        ),

        "load_timestamp": timestamp,

        "status": "SUCCESS"
    }

    try:

        errors = (
            bq_client.insert_rows_json(
                BQ_AUDIT_TABLE,
                [row]
            )
        )

        if errors:

            log_event(
                "WARNING",
                (
                    f"Audit insert failed: "
                    f"{errors}"
                ),
                table
            )

        else:

            log_event(
                "SUCCESS",
                "Audit log updated in BigQuery.",
                table
            )

    except Exception as e:

        log_event(
            "WARNING",
            (
                f"Could not write audit record: "
                f"{str(e)}"
            ),
            table
        )


# ============================================================
# PROCESS TABLE
# ============================================================

def process_table(row):

    table = str(
        row.get(
            "tablename",
            ""
        )
    ).strip()

    load_type = str(
        row.get(
            "loadtype",
            ""
        )
    ).strip()

    watermark = str(
        row.get(
            "watermark",
            ""
        )
    ).strip()

    target_path = str(
        row.get(
            "targetpath",
            ""
        )
    ).strip()

    if not table:

        log_event(
            "ERROR",
            "tablename is missing in config."
        )

        return

    log_event(
        "INFO",
        (
            f"Configuration loaded: "
            f"database={row.get('database')}, "
            f"datasource={row.get('datasource')}, "
            f"table={table}, "
            f"loadtype={load_type}, "
            f"watermark={watermark}, "
            f"targetpath={target_path}"
        ),
        table
    )

    log_event(
        "INFO",
        (
            f"Processing table '{table}' | "
            f"Load type: {load_type}"
        ),
        table
    )

    try:

        archive_existing_files(
            table
        )

        record_count = (
            extract_mysql_to_gcs(
                table,
                load_type,
                watermark,
                target_path
            )
        )

        if record_count > 0:

            write_audit(
                table,
                load_type,
                record_count
            )

    except Exception as e:

        log_event(
            "ERROR",
            (
                f"Table '{table}' failed: "
                f"{str(e)}"
            ),
            table
        )


# ============================================================
# SAVE LOGS TO GCS
# ============================================================

def save_logs_to_gcs():

    if not log_entries:
        return

    timestamp = (
        datetime.datetime.utcnow()
        .strftime("%Y%m%d%H%M%S")
    )

    filename = (
        f"pipeline_log_{timestamp}.json"
    )

    path = (
        f"{LOG_PREFIX}/{filename}"
    )

    json_data = json.dumps(
        log_entries,
        indent=4,
        default=str
    )

    bucket = storage_client.bucket(
        GCS_BUCKET
    )

    blob = bucket.blob(
        path
    )

    blob.upload_from_string(
        json_data,
        content_type="application/json"
    )

    print()
    print(
        "Pipeline logs saved to:"
    )

    print(
        f"gs://{GCS_BUCKET}/{path}"
    )


# ============================================================
# SAVE LOGS TO BIGQUERY
# ============================================================

def save_logs_to_bigquery():

    if not log_entries:
        return

    # Convert every value to JSON-safe values.
    safe_logs = []

    for entry in log_entries:

        safe_entry = {
            key: make_json_safe(value)
            for key, value in entry.items()
        }

        safe_logs.append(
            safe_entry
        )

    try:

        errors = (
            bq_client.insert_rows_json(
                BQ_LOG_TABLE,
                safe_logs
            )
        )

        if errors:

            print(
                f"BigQuery logging warning: {errors}"
            )

        else:

            print(
                "Pipeline logs saved to BigQuery."
            )

    except Exception as e:

        print(
            f"BigQuery logging warning: {e}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print(
        "=========================================="
    )

    print(
        "Retailer MySQL -> GCS pipeline started"
    )

    print(
        "Spark is NOT used."
    )

    print(
        "=========================================="
    )

    try:

        config_rows = (
            read_config_file()
        )

        active_tables = 0

        for row in config_rows:

            is_active = str(
                row.get(
                    "is_active",
                    ""
                )
            ).strip().lower()

            if is_active not in (
                "1",
                "true",
                "yes"
            ):

                continue

            active_tables += 1

            process_table(
                row
            )

        log_event(
            "SUCCESS",
            (
                f"Pipeline completed. "
                f"Active tables processed: "
                f"{active_tables}"
            )
        )

    except Exception as e:

        log_event(
            "ERROR",
            f"Pipeline failed: {str(e)}"
        )

    finally:

        save_logs_to_gcs()

        save_logs_to_bigquery()


# ============================================================
# RUN
# ============================================================

main()