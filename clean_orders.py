import json
from datetime import datetime

INPUT_FILE = "hahei_orders.json"
OUTPUT_FILE = "hahei_orders_clean.json"

EMPTY_FIELDS_TO_DROP = [
    "vouchers", "bookingFields", "questions", "travellers",
    "ad_hoc_items", "accommodations", "upgrade_accommodations",
    "pre_accommodations", "post_accommodations", "payment_cards",
]

FIELDS_TO_DROP = [
    "barcodeType", "fields",
    "agent_tour_commission", "agent_accommodation_commission",
    "agent_activity_commission", "agent_payment",
    "tour_margin", "tour_expenses",
    "cf_reminder_email_count", "waiver_reminder_email_count",
    "__v", "resellerLogo",
    "countryName", "isImported", "isDeleted",
    "supplier", "reseller", "posId",
    "productCode",  # redundant, exists in items[]
]

ITEM_FIELDS_TO_DROP = [
    "participants", "transferReturn", "productCode",
    "startTime", "endTime",  # keep local versions only
]

CUSTOMER_FIELDS_TO_DROP = ["id", "country"]


def parse_date(value):
    """Parse ISO date string to date only (YYYY-MM-DD)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return value


def parse_datetime(value):
    """Normalize ISO datetime string."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return value


def parse_local_datetime(value):
    """Parse local datetime string (YYYY-MM-DD HH:MM:SS) to ISO."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return value


def is_masked_email(email):
    """Detect Viator/platform proxy emails."""
    if not email:
        return False
    masked_patterns = ["expmessaging", "tripadvisor.com", "viator.com"]
    return any(p in email for p in masked_patterns)


def clean_customer(customer):
    cleaned = {k: v for k, v in customer.items() if k not in CUSTOMER_FIELDS_TO_DROP}
    cleaned["email_masked"] = is_masked_email(cleaned.get("email"))
    return cleaned


def clean_quantities(quantities):
    """Keep only quantity options that were actually booked (value > 0)."""
    return [q for q in quantities if q.get("value", 0) > 0]


def clean_item(item):
    cleaned = {k: v for k, v in item.items() if k not in ITEM_FIELDS_TO_DROP}

    # Parse local datetime strings to ISO format
    if "startTimeLocal" in cleaned:
        cleaned["startTimeLocal"] = parse_local_datetime(cleaned["startTimeLocal"])
    if "endTimeLocal" in cleaned:
        cleaned["endTimeLocal"] = parse_local_datetime(cleaned["endTimeLocal"])

    # Keep only booked quantities
    if "quantities" in cleaned:
        cleaned["quantities"] = clean_quantities(cleaned["quantities"])

    return cleaned


def clean_payment(payment):
    cleaned = dict(payment)
    if "date" in cleaned:
        cleaned["date"] = parse_datetime(cleaned["date"])
    return cleaned


def clean_order(order):
    # Drop unwanted top-level fields
    cleaned = {k: v for k, v in order.items()
               if k not in FIELDS_TO_DROP and k not in EMPTY_FIELDS_TO_DROP}

    # Clean customer
    if "customer" in cleaned:
        cleaned["customer"] = clean_customer(cleaned["customer"])

    # Normalize dates
    for date_field in ["dateConfirmed", "dateCreated", "datePaid", "createdAt", "updatedAt"]:
        if date_field in cleaned:
            cleaned[date_field] = parse_datetime(cleaned[date_field])

    for date_field in ["travelDate", "travelStartDate", "travelEndDate"]:
        if date_field in cleaned:
            cleaned[date_field] = parse_datetime(cleaned[date_field])

    # Use travelStartDate as canonical travel date
    cleaned["travel_date"] = parse_date(cleaned.get("travelStartDate"))

    # Clean items
    if "items" in cleaned:
        cleaned["items"] = [clean_item(i) for i in cleaned["items"]]

    # Clean payments
    if "payments" in cleaned:
        cleaned["payments"] = [clean_payment(p) for p in cleaned["payments"]]

    return cleaned


def main():
    with open(INPUT_FILE, "r") as f:
        orders = json.load(f)

    print(f"Loaded {len(orders)} orders")

    cleaned_orders = [clean_order(o) for o in orders]

    with open(OUTPUT_FILE, "w") as f:
        json.dump(cleaned_orders, f, indent=2)

    print(f"Cleaned {len(cleaned_orders)} orders -> {OUTPUT_FILE}")

    # Summary of what was dropped
    sample_before = set(orders[0].keys())
    sample_after = set(cleaned_orders[0].keys())
    dropped = sample_before - sample_after
    added = sample_after - sample_before
    print(f"\nFields dropped: {sorted(dropped)}")
    print(f"Fields added:   {sorted(added)}")


if __name__ == "__main__":
    main()
