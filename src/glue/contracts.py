"""Business keys and row-level checks shared by the Glue jobs.

Bronze uses KEYS to recognize supported entity types. Silver uses the keys to
merge entity rows and CHECKS to choose which rows may be written. Gold uses the
entity list to load Silver tables. Source field types come from Glue Catalog.
Money is stored in integer paise; timestamps describe UTC events.
"""

# Each dictionary entry maps an entity/table name to the business ID of one row.
# A new event for the same ID carries a higher version when the business state changes.
KEYS = {
    # A product ID identifies one SKU, including its size/colour variant.
    "product": "product_id",
    # A customer ID identifies one customer profile.
    "customer": "customer_id",
    # An order-item ID identifies one line within an order.
    "order_item": "order_item_id",
    # A payment ID identifies one payment transaction for an order.
    "payment": "payment_id",
    # A shipment-item ID identifies one parcel allocation to an order line.
    "shipment": "shipment_item_id",
    # A return ID identifies one request to return an order line.
    "return": "return_id",
    # A refund ID identifies one refund transaction.
    "refund": "refund_id",
}

# These are Spark SQL conditions evaluated by Silver before a row is merged.
# A False or null result sends the row to quarantine; valid rows continue.
# Cross-table checks, such as whether a refund references a payment, run later in Gold.
CHECKS = {
    # Require product/style/category identifiers and a positive catalogue price.
    "product": "product_id IS NOT NULL AND style_id IS NOT NULL AND category IS NOT NULL AND mrp_paise > 0",
    # Require a customer ID, name, state and a six-digit PIN-code format starting 1-9.
    "customer": "customer_id IS NOT NULL AND customer_name IS NOT NULL AND state IS NOT NULL AND postcode RLIKE '^[1-9][0-9]{5}$'",
    # Require order/customer/SKU links, order time, delivery state and positive quantity/price.
    # Discount must fit the line value; status and payment method must be recognized.
    # The row stores order-time price/geography for historical reporting.
    "order_item": "order_item_id IS NOT NULL AND order_id IS NOT NULL AND customer_id IS NOT NULL AND product_id IS NOT NULL AND to_timestamp(ordered_at) IS NOT NULL AND state IS NOT NULL AND quantity > 0 AND unit_price_paise > 0 AND discount_paise >= 0 AND discount_paise <= quantity * unit_price_paise AND status IN ('PLACED','CONFIRMED','PACKED','SHIPPED','DELIVERED','RETURNED','CANCELLED') AND payment_method IN ('UPI','CARD','NETBANKING','COD')",
    # Require payment/order IDs, positive amount and a recognized method/status.
    # A captured payment must also have a valid paid_at timestamp.
    "payment": "payment_id IS NOT NULL AND order_id IS NOT NULL AND amount_paise > 0 AND status IN ('CAPTURED','PENDING','FAILED') AND method IN ('UPI','CARD','NETBANKING','COD') AND (status <> 'CAPTURED' OR to_timestamp(paid_at) IS NOT NULL)",
    # Require shipment/order-line IDs, positive quantity, ship time and promised time.
    # A delivered allocation must have delivery time at or after shipment time.
    "shipment": "shipment_item_id IS NOT NULL AND shipment_id IS NOT NULL AND order_item_id IS NOT NULL AND quantity > 0 AND to_timestamp(shipped_at) IS NOT NULL AND to_timestamp(promised_at) IS NOT NULL AND status IN ('SHIPPED','DELIVERED','RTO') AND (status <> 'DELIVERED' OR to_timestamp(delivered_at) >= to_timestamp(shipped_at))",
    # Require return/order-line IDs, request time, positive quantity and known reason/status.
    "return": "return_id IS NOT NULL AND order_item_id IS NOT NULL AND to_timestamp(requested_at) IS NOT NULL AND quantity > 0 AND reason IN ('SIZE_TOO_SMALL','SIZE_TOO_LARGE','COLOUR_MISMATCH','FABRIC_EXPECTATION','DAMAGED','CHANGED_MIND') AND status IN ('REQUESTED','RECEIVED','REJECTED')",
    # Require refund/return/payment IDs, positive amount and known status.
    # A completed refund must have a valid refunded_at timestamp.
    "refund": "refund_id IS NOT NULL AND return_id IS NOT NULL AND payment_id IS NOT NULL AND amount_paise > 0 AND status IN ('PENDING','COMPLETED') AND (status <> 'COMPLETED' OR to_timestamp(refunded_at) IS NOT NULL)",
}
