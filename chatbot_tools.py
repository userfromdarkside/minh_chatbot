import boto3
import time
import json

REGION = "ap-southeast-2"
S3_BUCKET = "dazhboards-lakehouse"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
GLUE_DATABASE = "lakehouse_gold"

athena = boto3.client("athena", region_name=REGION)


# ---------------------------------------------------------------------------
# Athena helper
# ---------------------------------------------------------------------------

def _run_query(sql):
    """Execute an Athena query and return results as a list of dicts."""
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise Exception(f"Query {state}: {reason}")
        time.sleep(1)

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
            rows.append({
                col: val.get("VarCharValue", None)
                for col, val in zip(columns, row["Data"])
            })

    return rows


# ---------------------------------------------------------------------------
# Tool 1: get_revenue_summary
# ---------------------------------------------------------------------------

def get_revenue_summary(supplier_name: str, start_date: str = None, end_date: str = None) -> dict:
    """
    Returns revenue, order count, pax and reseller breakdown for a supplier.

    Args:
        supplier_name: e.g. "Hahei Explorer Cathedral Cove Boat Tour"
        start_date: optional, format YYYY-MM-DD
        end_date: optional, format YYYY-MM-DD

    Returns:
        dict with total summary and per-reseller breakdown
    """
    # Base query from gold table
    where = f"supplier_name = '{supplier_name}'"

    # If date filters provided, re-query silver for accuracy
    if start_date or end_date:
        date_filter = ""
        if start_date:
            date_filter += f" AND travel_date >= TIMESTAMP '{start_date} 00:00:00'"
        if end_date:
            date_filter += f" AND travel_date <= TIMESTAMP '{end_date} 23:59:59'"

        sql = f"""
        SELECT
            supplier_name,
            reseller_name,
            source_channel,
            COUNT(*)            AS order_count,
            SUM(total_amount)   AS total_revenue,
            SUM(total_paid)     AS total_paid,
            SUM(refund_amount)  AS total_refunds,
            SUM(total_pax)      AS total_pax,
            AVG(total_amount)   AS avg_order_value,
            AVG(lead_time)      AS avg_lead_time_days
        FROM lakehouse_silver.orders
        WHERE status = 'CONFIRMED'
          AND supplier_name = '{supplier_name}'
          {date_filter}
        GROUP BY supplier_name, reseller_name, source_channel
        ORDER BY total_revenue DESC
        """
    else:
        sql = f"""
        SELECT *
        FROM revenue_summary
        WHERE supplier_name = '{supplier_name}'
        ORDER BY total_revenue DESC
        """

    rows = _run_query(sql)

    # Build summary
    total_revenue = sum(float(r["total_revenue"] or 0) for r in rows)
    total_orders = sum(int(r["order_count"] or 0) for r in rows)
    total_pax = sum(int(r["total_pax"] or 0) for r in rows)

    return {
        "supplier_name": supplier_name,
        "period": {"start_date": start_date, "end_date": end_date},
        "summary": {
            "total_revenue": round(total_revenue, 2),
            "total_orders": total_orders,
            "total_pax": total_pax,
            "avg_order_value": round(total_revenue / total_orders, 2) if total_orders else 0,
        },
        "by_reseller": rows,
    }


# ---------------------------------------------------------------------------
# Tool 2: get_booking_trends
# ---------------------------------------------------------------------------

def get_booking_trends(supplier_name: str, group_by: str = "month") -> dict:
    """
    Returns booking trends grouped by month or by product.

    Args:
        supplier_name: e.g. "Hahei Explorer Cathedral Cove Boat Tour"
        group_by: "month" or "product"

    Returns:
        dict with trend data
    """
    if group_by not in ("month", "product"):
        raise ValueError("group_by must be 'month' or 'product'")

    sql = f"""
    SELECT *
    FROM booking_trends
    WHERE supplier_name = '{supplier_name}'
    ORDER BY {'year_month' if group_by == 'month' else 'total_revenue DESC'}
    """

    rows = _run_query(sql)

    if group_by == "month":
        # Aggregate across products per month
        monthly = {}
        for r in rows:
            ym = r["year_month"]
            if ym not in monthly:
                monthly[ym] = {"year_month": ym, "order_count": 0, "total_revenue": 0.0, "total_pax": 0}
            monthly[ym]["order_count"] += int(r["order_count"] or 0)
            monthly[ym]["total_revenue"] += float(r["total_revenue"] or 0)
            monthly[ym]["total_pax"] += int(r["total_pax"] or 0)

        trends = list(monthly.values())
        for t in trends:
            t["total_revenue"] = round(t["total_revenue"], 2)

    else:
        # Aggregate across months per product
        products = {}
        for r in rows:
            pname = r["product_name"]
            if pname not in products:
                products[pname] = {"product_name": pname, "order_count": 0, "total_revenue": 0.0, "total_pax": 0}
            products[pname]["order_count"] += int(r["order_count"] or 0)
            products[pname]["total_revenue"] += float(r["total_revenue"] or 0)
            products[pname]["total_pax"] += int(r["total_pax"] or 0)

        trends = sorted(products.values(), key=lambda x: x["total_revenue"], reverse=True)
        for t in trends:
            t["total_revenue"] = round(t["total_revenue"], 2)

    return {
        "supplier_name": supplier_name,
        "group_by": group_by,
        "trends": trends,
    }


# ---------------------------------------------------------------------------
# Tool definitions for Claude API (tool_use format)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "get_revenue_summary",
        "description": (
            "Get revenue, order count, pax count and reseller breakdown for a supplier. "
            "Use this when the user asks about total sales, revenue, bookings, or channel performance. "
            "Optionally filter by date range."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "supplier_name": {
                    "type": "string",
                    "description": "Full supplier name, e.g. 'Hahei Explorer Cathedral Cove Boat Tour'",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date filter in YYYY-MM-DD format (optional)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date filter in YYYY-MM-DD format (optional)",
                },
            },
            "required": ["supplier_name"],
        },
    },
    {
        "name": "get_booking_trends",
        "description": (
            "Get booking trends grouped by month or by product for a supplier. "
            "Use this when the user asks about trends over time, sales by month, "
            "which products sell best, or why sales changed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "supplier_name": {
                    "type": "string",
                    "description": "Full supplier name, e.g. 'Hahei Explorer Cathedral Cove Boat Tour'",
                },
                "group_by": {
                    "type": "string",
                    "enum": ["month", "product"],
                    "description": "'month' for time trends, 'product' for product performance",
                },
            },
            "required": ["supplier_name", "group_by"],
        },
    },
]


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    supplier = "Hahei Explorer Cathedral Cove Boat Tour"

    print("=== get_revenue_summary ===")
    result = get_revenue_summary(supplier)
    print(json.dumps(result, indent=2))

    print("\n=== get_booking_trends (month) ===")
    result = get_booking_trends(supplier, group_by="month")
    print(json.dumps(result, indent=2))

    print("\n=== get_booking_trends (product) ===")
    result = get_booking_trends(supplier, group_by="product")
    print(json.dumps(result, indent=2))
