# Dazhboards Chatbot — Demo Data Processing

Analytics chatbot for tourism suppliers, built on AWS Bedrock Agent + Apache Iceberg lakehouse.

## Architecture

```
MongoDB → S3 (Bronze) → S3 Iceberg (Silver) → S3 Iceberg (Gold) → Athena → Lambda → Bedrock Agent → Node.js API → Frontend
```

## Medallion Layers

### Bronze

| Table | Description |
|---|---|
| `orders` | Raw order documents from MongoDB, one row per order, no transformations applied |

### Silver

| Table | Description |
|---|---|
| `orders` | Cleaned orders with normalized dates, filtered confirmed/cancelled status, one row per order |
| `order_items` | Exploded from `items[]` array, one row per line item within an order |
| `order_quantities` | Exploded from `items[].quantities[]`, one row per quantity/ticket type |
| `payments` | Exploded from `payments[]`, one row per payment transaction |

### Gold

| Table | Description |
|---|---|
| `revenue_summary` | Total revenue, order count, pax, avg order value grouped by supplier and reseller |
| `booking_trends` | Booking and revenue aggregated by supplier, month, and product for trend analysis |

## Scripts

| File | Purpose |
|---|---|
| `clean_orders.py` | Clean raw MongoDB JSON export |
| `json_to_parquet.py` | Convert JSON to Parquet and upload to S3 bronze layer |
| `bronze_to_silver.py` | Transform bronze to silver Iceberg tables, register in Glue |
| `silver_to_gold.py` | Build gold aggregations via Athena, register in Glue |
| `chatbot_tools.py` | Tool function definitions for Claude API (tool_use format) |
| `lambda_handler.py` | AWS Lambda handler for Bedrock Agent action group |

## AWS Infrastructure

| Resource | Value |
|---|---|
| S3 Bucket | `dazhboards-lakehouse` (ap-southeast-2) |
| Glue Databases | `lakehouse_bronze`, `lakehouse_silver`, `lakehouse_gold` |
| Lambda | `dazhboards-chatbot-tools` |
| Bedrock Agent | `E8FSRBU099` |
| Athena Results | `s3://dazhboards-lakehouse/athena-results/` |

## Chatbot Tools

### `get_revenue_summary(supplier_name, start_date?, end_date?)`
Returns total revenue, order count, pax count and reseller breakdown.

### `get_booking_trends(supplier_name, group_by)`
Returns booking trends grouped by `month` or `product`.

## Pilot Supplier

- **Name:** Hahei Explorer Cathedral Cove Boat Tour
- **Orders:** 30 confirmed, 5 cancelled
- **Revenue:** NZD $9,583
- **Pax:** 88 guests
- **Date range:** Jul 2024 → Feb 2026

## Node.js Integration

```javascript
const { BedrockAgentRuntimeClient, InvokeAgentCommand } = require("@aws-sdk/client-bedrock-agent-runtime");

const client = new BedrockAgentRuntimeClient({ region: "ap-southeast-2" });

const command = new InvokeAgentCommand({
  agentId: "E8FSRBU099",
  agentAliasId: "TSTALIASID",
  sessionId: sessionId,
  inputText: userMessage,
});

const response = await client.send(command);
for await (const chunk of response.completion) {
  if (chunk.chunk?.bytes) {
    answer += new TextDecoder().decode(chunk.chunk.bytes);
  }
}
```
