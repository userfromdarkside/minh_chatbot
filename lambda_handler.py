import json
import time

import boto3

REGION = "ap-southeast-2"
S3_BUCKET = "dazhboards-lakehouse"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
GOLD_DATABASE = "lakehouse_gold"
SILVER_DATABASE = "lakehouse_silver"

athena = boto3.client("athena", region_name=REGION)


# ---------------------------------------------------------------------------
# Athena helper
# ---------------------------------------------------------------------------

def _run_athena_query(sql, database):
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
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
# Tool: get_revenue_summary
# ---------------------------------------------------------------------------

def get_revenue_summary(supplier_name, start_date=None, end_date=None):
    if start_date or end_date:
        # Dynamic date filter → Athena on silver
        date_filter = ""
        if start_date:
            date_filter += f" AND travel_date >= TIMESTAMP '{start_date} 00:00:00'"
        if end_date:
            date_filter += f" AND travel_date <= TIMESTAMP '{end_date} 23:59:59'"

        sql = f"""
        SELECT
            supplier_name, reseller_name, source_channel,
            COUNT(*)            AS order_count,
            SUM(total_amount)   AS total_revenue,
            SUM(total_paid)     AS total_paid,
            SUM(refund_amount)  AS total_refunds,
            SUM(total_pax)      AS total_pax,
            AVG(total_amount)   AS avg_order_value,
            AVG(lead_time)      AS avg_lead_time_days
        FROM orders
        WHERE status = 'CONFIRMED'
          AND supplier_name = '{supplier_name}'
          {date_filter}
        GROUP BY supplier_name, reseller_name, source_channel
        ORDER BY total_revenue DESC
        """
        rows = _run_athena_query(sql, SILVER_DATABASE)
    else:
        sql = f"""
        SELECT * FROM revenue_summary
        WHERE supplier_name = '{supplier_name}'
        ORDER BY total_revenue DESC
        """
        rows = _run_athena_query(sql, GOLD_DATABASE)

    total_revenue = sum(float(r["total_revenue"] or 0) for r in rows)
    total_orders = sum(int(r["order_count"] or 0) for r in rows)
    total_pax = sum(int(r["total_pax"] or 0) for r in rows)

    result = {
        "supplier_name": supplier_name,
        "period": {"start_date": start_date, "end_date": end_date},
        "summary": {
            "total_revenue_nzd": round(total_revenue, 2),
            "total_orders": total_orders,
            "total_pax": total_pax,
            "avg_order_value_nzd": round(total_revenue / total_orders, 2) if total_orders else 0,
        },
        "by_reseller": rows,
    }
    return result


# ---------------------------------------------------------------------------
# Tool: get_booking_trends
# ---------------------------------------------------------------------------

def get_booking_trends(supplier_name, group_by="month"):
    sql = f"""
    SELECT * FROM booking_trends
    WHERE supplier_name = '{supplier_name}'
    ORDER BY {'year_month' if group_by == 'month' else 'total_revenue DESC'}
    """
    rows = _run_athena_query(sql, GOLD_DATABASE)

    if group_by == "month":
        monthly = {}
        for r in rows:
            ym = r["year_month"]
            if ym not in monthly:
                monthly[ym] = {"year_month": ym, "order_count": 0, "total_revenue": 0.0, "total_pax": 0}
            monthly[ym]["order_count"] += int(r["order_count"] or 0)
            monthly[ym]["total_revenue"] = round(monthly[ym]["total_revenue"] + float(r["total_revenue"] or 0), 2)
            monthly[ym]["total_pax"] += int(r["total_pax"] or 0)
        trends = list(monthly.values())
    else:
        products = {}
        for r in rows:
            pname = r["product_name"]
            if pname not in products:
                products[pname] = {"product_name": pname, "order_count": 0, "total_revenue": 0.0, "total_pax": 0}
            products[pname]["order_count"] += int(r["order_count"] or 0)
            products[pname]["total_revenue"] = round(products[pname]["total_revenue"] + float(r["total_revenue"] or 0), 2)
            products[pname]["total_pax"] += int(r["total_pax"] or 0)
        trends = sorted(products.values(), key=lambda x: x["total_revenue"], reverse=True)

    return {
        "supplier_name": supplier_name,
        "group_by": group_by,
        "trends": trends,
    }


# ---------------------------------------------------------------------------
# Bedrock Agent Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    function_name = event.get("function")
    parameters = {p["name"]: p["value"] for p in event.get("parameters", [])}

    try:
        if function_name == "get_revenue_summary":
            result = get_revenue_summary(
                supplier_name=parameters["supplier_name"],
                start_date=parameters.get("start_date"),
                end_date=parameters.get("end_date"),
            )
        elif function_name == "get_booking_trends":
            result = get_booking_trends(
                supplier_name=parameters["supplier_name"],
                group_by=parameters.get("group_by", "month"),
            )
        else:
            raise ValueError(f"Unknown function: {function_name}")

        body = json.dumps(result)

    except Exception as e:
        body = json.dumps({"error": str(e)})

    return {
        "response": {
            "actionGroup": event.get("actionGroup"),
            "function": function_name,
            "functionResponse": {
                "responseBody": {
                    "TEXT": {"body": body}
                }
            },
        }
    }
