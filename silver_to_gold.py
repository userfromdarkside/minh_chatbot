import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time

S3_BUCKET = "dazhboards-lakehouse"
GLUE_DATABASE = "lakehouse_gold"
REGION = "ap-southeast-2"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_athena_query(sql):
    """Run a query on Athena and return results as a DataFrame."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "lakehouse_silver"},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    # Wait for query to complete
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise Exception(f"Athena query {state}: {reason}")
        time.sleep(1)

    # Fetch results
    paginator = athena.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=query_id)

    rows = []
    columns = None
    for page in pages:
        result_rows = page["ResultSet"]["Rows"]
        if columns is None:
            columns = [col["VarCharValue"] for col in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([col.get("VarCharValue", None) for col in row["Data"]])

    return pd.DataFrame(rows, columns=columns)


def upload_parquet(df, s3_key):
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    print(f"  Uploaded {len(df)} rows -> s3://{S3_BUCKET}/{s3_key}")


def register_glue_table(table_name, s3_location, columns):
    glue_columns = [{"Name": col, "Type": dtype} for col, dtype in columns]
    try:
        glue.delete_table(DatabaseName=GLUE_DATABASE, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": glue_columns,
                "Location": s3_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
                "Compressed": True,
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet", "compressionType": "snappy"},
        },
    )
    print(f"  Registered Glue table: {GLUE_DATABASE}.{table_name}")


# ---------------------------------------------------------------------------
# gold.revenue_summary
# Revenue + orders + pax breakdown by supplier and reseller
# ---------------------------------------------------------------------------

REVENUE_SUMMARY_SQL = """
SELECT
    supplier_name,
    reseller_name,
    source_channel,
    COUNT(*)                        AS order_count,
    SUM(total_amount)               AS total_revenue,
    SUM(total_paid)                 AS total_paid,
    SUM(refund_amount)              AS total_refunds,
    SUM(total_pax)                  AS total_pax,
    SUM(total_adults)               AS total_adults,
    SUM(total_students)             AS total_students,
    AVG(total_amount)               AS avg_order_value,
    AVG(lead_time)                  AS avg_lead_time_days
FROM orders
WHERE status = 'CONFIRMED'
GROUP BY supplier_name, reseller_name, source_channel
ORDER BY total_revenue DESC
"""

REVENUE_SUMMARY_SCHEMA = [
    ("supplier_name", "string"),
    ("reseller_name", "string"),
    ("source_channel", "string"),
    ("order_count", "bigint"),
    ("total_revenue", "double"),
    ("total_paid", "double"),
    ("total_refunds", "double"),
    ("total_pax", "bigint"),
    ("total_adults", "bigint"),
    ("total_students", "bigint"),
    ("avg_order_value", "double"),
    ("avg_lead_time_days", "double"),
]


# ---------------------------------------------------------------------------
# gold.booking_trends
# Monthly revenue + orders + pax, and breakdown by product
# ---------------------------------------------------------------------------

BOOKING_TRENDS_SQL = """
SELECT
    o.supplier_name,
    DATE_FORMAT(o.travel_date, '%Y-%m')   AS year_month,
    oi.product_name,
    COUNT(DISTINCT o.order_id)            AS order_count,
    SUM(o.total_amount)                   AS total_revenue,
    SUM(o.total_pax)                      AS total_pax,
    AVG(o.total_amount)                   AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'CONFIRMED'
GROUP BY o.supplier_name, DATE_FORMAT(o.travel_date, '%Y-%m'), oi.product_name
ORDER BY year_month, total_revenue DESC
"""

BOOKING_TRENDS_SCHEMA = [
    ("supplier_name", "string"),
    ("year_month", "string"),
    ("product_name", "string"),
    ("order_count", "bigint"),
    ("total_revenue", "double"),
    ("total_pax", "bigint"),
    ("avg_order_value", "double"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

REVENUE_SUMMARY_CASTS = {
    "order_count": int, "total_revenue": float, "total_paid": float,
    "total_refunds": float, "total_pax": int, "total_adults": int,
    "total_students": int, "avg_order_value": float, "avg_lead_time_days": float,
}

BOOKING_TRENDS_CASTS = {
    "order_count": int, "total_revenue": float,
    "total_pax": int, "avg_order_value": float,
}


def cast_df(df, casts):
    for col, dtype in casts.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float if dtype == float else "Int64")
    return df


def main():
    tables = [
        ("revenue_summary", REVENUE_SUMMARY_SQL, REVENUE_SUMMARY_SCHEMA, REVENUE_SUMMARY_CASTS),
        ("booking_trends",  BOOKING_TRENDS_SQL,  BOOKING_TRENDS_SCHEMA,  BOOKING_TRENDS_CASTS),
    ]

    for table_name, sql, schema, casts in tables:
        print(f"[{table_name}]")
        print(f"  Running Athena query...")
        df = run_athena_query(sql)
        df = cast_df(df, casts)

        s3_key = f"gold/{table_name}/{table_name}.parquet"
        s3_location = f"s3://{S3_BUCKET}/gold/{table_name}/"
        upload_parquet(df, s3_key)
        register_glue_table(table_name, s3_location, schema)
        print()

    print("Gold layer ready. Query in Athena using database: lakehouse_gold")


if __name__ == "__main__":
    main()
