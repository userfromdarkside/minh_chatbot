import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

INPUT_FILE = "hahei_orders_clean.json"
S3_BUCKET = "dazhboards-lakehouse"
S3_KEY = "bronze/orders/supplier=hahei_explorer/hahei_orders.parquet"


def flatten_customer(customer):
    """Flatten customer dict into top-level columns."""
    if not customer:
        return {}
    return {
        "customer_first_name": customer.get("firstName"),
        "customer_last_name": customer.get("lastName"),
        "customer_name": customer.get("name"),
        "customer_email": customer.get("email"),
        "customer_email_masked": customer.get("email_masked", False),
        "customer_mobile": customer.get("mobile"),
        "customer_city": customer.get("city"),
        "customer_country_code": customer.get("countryCode"),
    }


def flatten_order(order):
    """Flatten top-level order fields, keep items/payments as JSON strings."""
    flat = {
        "order_id":               order.get("_id"),
        "order_number":           order.get("orderNumber"),
        "status":                 order.get("status"),
        "source":                 order.get("source"),
        "source_channel":         order.get("sourceChannel"),
        "booking_platform":       order.get("booking_platform"),
        "supplier_id":            order.get("supplierId"),
        "supplier_name":          order.get("supplierName"),
        "supplier_alias":         order.get("supplierAlias"),
        "reseller_id":            order.get("resellerId"),
        "reseller_name":          order.get("resellerName"),
        "reseller_alias":         order.get("resellerAlias"),
        "reseller_source":        order.get("resellerSource"),
        "reseller_reference":     order.get("resellerReference"),
        "reseller_comments":      order.get("resellerComments"),
        "internal_notes":         order.get("internalNotes"),
        "total_amount":           order.get("totalAmount"),
        "total_currency":         order.get("totalCurrency"),
        "total_paid":             order.get("totalPaid"),
        "total_due":              order.get("totalDue"),
        "total_discount":         order.get("totalDiscount"),
        "refund_amount":          order.get("refund_amount"),
        "commission":             order.get("commission"),
        "commission_percent":     order.get("commissionPercent"),
        "total_pax":              order.get("totalPax"),
        "total_adults":           order.get("totalAdults"),
        "total_students":         order.get("totalStudents"),
        "group_size":             order.get("groupSize"),
        "traveller_type":         order.get("traveller_type"),
        "lead_time":              order.get("leadTime"),
        "wavier_status":          order.get("wavier_status"),
        "payment_option":         order.get("paymentOption"),
        "travel_date":            order.get("travel_date"),
        "travel_start_date":      order.get("travelStartDate"),
        "travel_end_date":        order.get("travelEndDate"),
        "date_confirmed":         order.get("dateConfirmed"),
        "date_created":           order.get("dateCreated"),
        "date_paid":              order.get("datePaid"),
        "created_at":             order.get("createdAt"),
        "updated_at":             order.get("updatedAt"),
        # Keep nested arrays as JSON strings — will be exploded in silver layer
        "items_json":             json.dumps(order.get("items", [])),
        "payments_json":          json.dumps(order.get("payments", [])),
    }

    # Merge flattened customer fields
    flat.update(flatten_customer(order.get("customer", {})))

    return flat


def main():
    with open(INPUT_FILE, "r") as f:
        orders = json.load(f)

    print(f"Loaded {len(orders)} orders")

    rows = [flatten_order(o) for o in orders]
    df = pd.DataFrame(rows)

    # Convert date columns to proper datetime type
    date_cols = [
        "travel_start_date", "travel_end_date", "date_confirmed",
        "date_created", "date_paid", "created_at", "updated_at"
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    df["travel_date"] = pd.to_datetime(df["travel_date"], errors="coerce").dt.date

    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")

    # Convert to Parquet in memory
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # Upload to S3
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    s3.upload_fileobj(buffer, S3_BUCKET, S3_KEY)
    print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
