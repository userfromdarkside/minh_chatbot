import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

INPUT_FILE = "hahei_orders_clean.json"
S3_BUCKET = "dazhboards-lakehouse"
S3_PREFIX = "silver"
GLUE_DATABASE = "lakehouse_silver"
REGION = "ap-southeast-2"

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def upload_parquet(df, s3_key):
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    print(f"  Uploaded {len(df)} rows -> s3://{S3_BUCKET}/{s3_key}")


def register_glue_table(table_name, s3_location, columns):
    """Create or update a Glue table definition."""
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
            "Parameters": {
                "classification": "parquet",
                "compressionType": "snappy",
            },
        },
    )
    print(f"  Registered Glue table: {GLUE_DATABASE}.{table_name}")


# ---------------------------------------------------------------------------
# silver.orders — 1 row per order
# ---------------------------------------------------------------------------

def build_orders(orders):
    rows = []
    for o in orders:
        rows.append({
            "order_id":           o.get("_id"),
            "order_number":       o.get("orderNumber"),
            "status":             o.get("status"),
            "source":             o.get("source"),
            "source_channel":     o.get("sourceChannel"),
            "booking_platform":   o.get("booking_platform"),
            "supplier_id":        o.get("supplierId"),
            "supplier_name":      o.get("supplierName"),
            "supplier_alias":     o.get("supplierAlias"),
            "reseller_id":        o.get("resellerId"),
            "reseller_name":      o.get("resellerName"),
            "reseller_alias":     o.get("resellerAlias"),
            "reseller_source":    o.get("resellerSource"),
            "reseller_reference": o.get("resellerReference"),
            "reseller_comments":  o.get("resellerComments"),
            "internal_notes":     o.get("internalNotes"),
            "total_amount":       o.get("totalAmount"),
            "total_currency":     o.get("totalCurrency"),
            "total_paid":         o.get("totalPaid"),
            "total_due":          o.get("totalDue"),
            "total_discount":     o.get("totalDiscount"),
            "refund_amount":      o.get("refund_amount"),
            "commission":         o.get("commission"),
            "commission_percent": o.get("commissionPercent"),
            "total_pax":          o.get("totalPax"),
            "total_adults":       o.get("totalAdults"),
            "total_students":     o.get("totalStudents"),
            "group_size":         o.get("groupSize"),
            "traveller_type":     o.get("traveller_type"),
            "lead_time":          o.get("leadTime"),
            "wavier_status":      o.get("wavier_status"),
            "payment_option":     o.get("paymentOption"),
            "travel_date":        o.get("travel_date"),
            "travel_start_date":  o.get("travelStartDate"),
            "travel_end_date":    o.get("travelEndDate"),
            "date_confirmed":     o.get("dateConfirmed"),
            "date_created":       o.get("dateCreated"),
            "date_paid":          o.get("datePaid"),
            "created_at":         o.get("createdAt"),
            "updated_at":         o.get("updatedAt"),
            "customer_first_name":    o.get("customer", {}).get("firstName"),
            "customer_last_name":     o.get("customer", {}).get("lastName"),
            "customer_name":          o.get("customer", {}).get("name"),
            "customer_email":         o.get("customer", {}).get("email"),
            "customer_email_masked":  o.get("customer", {}).get("email_masked", False),
            "customer_mobile":        o.get("customer", {}).get("mobile"),
            "customer_city":          o.get("customer", {}).get("city"),
            "customer_country_code":  o.get("customer", {}).get("countryCode"),
        })

    df = pd.DataFrame(rows)
    date_cols = ["travel_start_date", "travel_end_date", "date_confirmed",
                 "date_created", "date_paid", "created_at", "updated_at"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    df["travel_date"] = pd.to_datetime(df["travel_date"], errors="coerce")

    float_cols = ["total_amount", "total_paid", "total_due", "total_discount",
                  "refund_amount", "commission", "commission_percent", "lead_time"]
    for col in float_cols:
        df[col] = df[col].astype(float)

    int_cols = ["total_pax", "total_adults", "total_students"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


# ---------------------------------------------------------------------------
# silver.order_items — 1 row per item
# ---------------------------------------------------------------------------

def build_order_items(orders):
    rows = []
    for o in orders:
        for item in o.get("items", []):
            rows.append({
                "order_id":        o.get("_id"),
                "order_number":    o.get("orderNumber"),
                "supplier_name":   o.get("supplierName"),
                "reseller_name":   o.get("resellerName"),
                "travel_date":     o.get("travel_date"),
                "product_name":    item.get("productName"),
                "product_code":    item.get("productCode"),
                "start_time_local": item.get("startTimeLocal"),
                "end_time_local":   item.get("endTimeLocal"),
                "total_quantity":  item.get("totalQuantity"),
                "amount":          item.get("amount"),
                "subtotal":        item.get("subtotal"),
                "total_item_tax":  item.get("totalItemTax"),
                "extras":          json.dumps(item.get("extras", [])),
            })
    df = pd.DataFrame(rows)
    df["travel_date"] = pd.to_datetime(df["travel_date"], errors="coerce")
    for col in ["start_time_local", "end_time_local"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    for col in ["amount", "subtotal", "total_item_tax"]:
        df[col] = df[col].astype(float)
    df["total_quantity"] = pd.to_numeric(df["total_quantity"], errors="coerce").astype("Int64")
    return df


# ---------------------------------------------------------------------------
# silver.order_quantities — 1 row per quantity option per item
# ---------------------------------------------------------------------------

def build_order_quantities(orders):
    rows = []
    for o in orders:
        for item in o.get("items", []):
            for qty in item.get("quantities", []):
                rows.append({
                    "order_id":      o.get("_id"),
                    "order_number":  o.get("orderNumber"),
                    "supplier_name": o.get("supplierName"),
                    "reseller_name": o.get("resellerName"),
                    "travel_date":   o.get("travel_date"),
                    "product_name":  item.get("productName"),
                    "product_code":  item.get("productCode"),
                    "option_label":  qty.get("optionLabel"),
                    "option_price":  qty.get("optionPrice"),
                    "quantity":      qty.get("value"),
                    "line_total":    qty.get("optionPrice", 0) * qty.get("value", 0),
                })
    df = pd.DataFrame(rows)
    df["travel_date"] = pd.to_datetime(df["travel_date"], errors="coerce")
    for col in ["option_price", "line_total"]:
        df[col] = df[col].astype(float)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    return df


# ---------------------------------------------------------------------------
# silver.payments — 1 row per payment
# ---------------------------------------------------------------------------

def build_payments(orders):
    rows = []
    for o in orders:
        for pmt in o.get("payments", []):
            rows.append({
                "order_id":      o.get("_id"),
                "order_number":  o.get("orderNumber"),
                "supplier_name": o.get("supplierName"),
                "reseller_name": o.get("resellerName"),
                "travel_date":   o.get("travel_date"),
                "payment_type":  pmt.get("type"),
                "amount":        pmt.get("amount"),
                "currency":      pmt.get("currency"),
                "payment_date":  pmt.get("date"),
                "recipient":     pmt.get("recipient"),
            })
    df = pd.DataFrame(rows)
    df["travel_date"] = pd.to_datetime(df["travel_date"], errors="coerce")
    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce", utc=True)
    df["amount"] = df["amount"].astype(float)
    return df


# ---------------------------------------------------------------------------
# Glue schema definitions
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = [
    ("order_id", "string"), ("order_number", "string"), ("status", "string"),
    ("source", "string"), ("source_channel", "string"), ("booking_platform", "string"),
    ("supplier_id", "string"), ("supplier_name", "string"), ("supplier_alias", "string"),
    ("reseller_id", "string"), ("reseller_name", "string"), ("reseller_alias", "string"),
    ("reseller_source", "string"), ("reseller_reference", "string"), ("reseller_comments", "string"),
    ("internal_notes", "string"), ("total_amount", "double"), ("total_currency", "string"),
    ("total_paid", "double"), ("total_due", "double"), ("total_discount", "double"),
    ("refund_amount", "double"), ("commission", "double"), ("commission_percent", "double"),
    ("total_pax", "int"), ("total_adults", "int"), ("total_students", "int"),
    ("group_size", "string"), ("traveller_type", "string"), ("lead_time", "double"),
    ("wavier_status", "string"), ("payment_option", "string"), ("travel_date", "timestamp"),
    ("travel_start_date", "timestamp"), ("travel_end_date", "timestamp"),
    ("date_confirmed", "timestamp"), ("date_created", "timestamp"), ("date_paid", "timestamp"),
    ("created_at", "timestamp"), ("updated_at", "timestamp"),
    ("customer_first_name", "string"), ("customer_last_name", "string"),
    ("customer_name", "string"), ("customer_email", "string"),
    ("customer_email_masked", "boolean"), ("customer_mobile", "string"),
    ("customer_city", "string"), ("customer_country_code", "string"),
]

ORDER_ITEMS_SCHEMA = [
    ("order_id", "string"), ("order_number", "string"), ("supplier_name", "string"),
    ("reseller_name", "string"), ("travel_date", "timestamp"), ("product_name", "string"),
    ("product_code", "string"), ("start_time_local", "timestamp"), ("end_time_local", "timestamp"),
    ("total_quantity", "int"), ("amount", "double"), ("subtotal", "double"),
    ("total_item_tax", "double"), ("extras", "string"),
]

ORDER_QUANTITIES_SCHEMA = [
    ("order_id", "string"), ("order_number", "string"), ("supplier_name", "string"),
    ("reseller_name", "string"), ("travel_date", "timestamp"), ("product_name", "string"),
    ("product_code", "string"), ("option_label", "string"), ("option_price", "double"),
    ("quantity", "int"), ("line_total", "double"),
]

PAYMENTS_SCHEMA = [
    ("order_id", "string"), ("order_number", "string"), ("supplier_name", "string"),
    ("reseller_name", "string"), ("travel_date", "timestamp"), ("payment_type", "string"),
    ("amount", "double"), ("currency", "string"), ("payment_date", "timestamp"),
    ("recipient", "string"),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    with open(INPUT_FILE, "r") as f:
        orders = json.load(f)
    print(f"Loaded {len(orders)} orders\n")

    tables = [
        ("orders",           build_orders(orders),           ORDERS_SCHEMA),
        ("order_items",      build_order_items(orders),      ORDER_ITEMS_SCHEMA),
        ("order_quantities", build_order_quantities(orders), ORDER_QUANTITIES_SCHEMA),
        ("payments",         build_payments(orders),         PAYMENTS_SCHEMA),
    ]

    for table_name, df, schema in tables:
        print(f"[{table_name}]")
        s3_key = f"{S3_PREFIX}/{table_name}/supplier=hahei_explorer/{table_name}.parquet"
        s3_location = f"s3://{S3_BUCKET}/{S3_PREFIX}/{table_name}/supplier=hahei_explorer/"
        upload_parquet(df, s3_key)
        register_glue_table(table_name, s3_location, schema)
        print()

    print("Silver layer ready. Query in Athena using database: lakehouse_silver")


if __name__ == "__main__":
    main()
