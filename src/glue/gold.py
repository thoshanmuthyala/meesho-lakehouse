"""Gold job: build four reports from the current Silver entity tables.

Input: latest accepted customer, product, order, payment, shipment, return and
refund rows. Output: four versioned reporting tables and a current-publication
pointer, followed by Athena views. Relationship problems are recorded as
warnings; they can mean a report has missing rows or inconsistent totals.
Money stays in integer paise until each report projects decimal rupees.
"""

# F is a short alias for Spark column functions such as col, lit and from_json.
from pyspark.sql import functions as F
from common import Run
from contracts import KEYS

# Each SQL query returns the keys that violate one cross-table rule.
# Gold saves those keys as diagnostic quarantine output and continues publishing.
# LEFT ANTI JOIN finds a row whose referenced parent is missing.
RELATIONSHIPS = {
    # Captured payments for an order should not exceed the sum of its order-line values.
    "payment_cap": "WITH totals AS (SELECT order_id,SUM(quantity*unit_price_paise-discount_paise) amount FROM order_item GROUP BY order_id) SELECT p.order_id FROM payment p JOIN totals t ON p.order_id=t.order_id WHERE p.status='CAPTURED' GROUP BY p.order_id,t.amount HAVING SUM(p.amount_paise)>t.amount",
    # Completed refunds for one return should fit the returned share of its order-line value.
    # Multiplying both sides by ordered quantity compares the cap without rounding.
    "refund_item_cap": "SELECT f.return_id FROM refund f JOIN returns r ON f.return_id=r.return_id JOIN order_item o ON r.order_item_id=o.order_item_id WHERE f.status='COMPLETED' GROUP BY f.return_id,r.quantity,o.quantity,o.unit_price_paise,o.discount_paise HAVING SUM(f.amount_paise)*o.quantity>r.quantity*(o.quantity*o.unit_price_paise-o.discount_paise)",
    # A completed refund should refer to a received return and occur at or after its request time.
    "refund_return_state": "SELECT f.refund_id FROM refund f JOIN returns r ON f.return_id=r.return_id WHERE f.status='COMPLETED' AND (r.status<>'RECEIVED' OR to_timestamp(f.refunded_at)<to_timestamp(r.requested_at))",
    # Find order lines whose customer is missing.
    "order_customer": "SELECT o.order_item_id FROM order_item o LEFT ANTI JOIN customer c ON o.customer_id=c.customer_id",
    # Find order lines whose product/SKU is missing.
    "order_product": "SELECT o.order_item_id FROM order_item o LEFT ANTI JOIN product p ON o.product_id=p.product_id",
    # Find payments with no corresponding order line for their order ID.
    "payment_order": "SELECT p.payment_id FROM payment p LEFT ANTI JOIN order_item o ON p.order_id=o.order_id",
    # Find shipment allocations whose order line is missing.
    "shipment_order_item": "SELECT s.shipment_item_id FROM shipment s LEFT ANTI JOIN order_item o ON s.order_item_id=o.order_item_id",
    # Find returns whose order line is missing.
    "return_order_item": "SELECT r.return_id FROM returns r LEFT ANTI JOIN order_item o ON r.order_item_id=o.order_item_id",
    # Find refunds whose return request is missing.
    "refund_return": "SELECT f.refund_id FROM refund f LEFT ANTI JOIN returns r ON f.return_id=r.return_id",
    # Find refunds whose payment transaction is missing.
    "refund_payment": "SELECT f.refund_id FROM refund f LEFT ANTI JOIN payment p ON f.payment_id=p.payment_id",
    # The returned order line and refunded payment should belong to the same order.
    "refund_order": "SELECT f.refund_id FROM refund f JOIN returns r ON f.return_id=r.return_id JOIN order_item o ON r.order_item_id=o.order_item_id JOIN payment p ON f.payment_id=p.payment_id WHERE o.order_id<>p.order_id",
    # Completed refunds must reference captured payments and not exceed each payment amount.
    "refund_cap": "SELECT f.payment_id FROM refund f JOIN payment p ON f.payment_id=p.payment_id WHERE f.status='COMPLETED' GROUP BY f.payment_id,p.status,p.amount_paise HAVING p.status<>'CAPTURED' OR SUM(f.amount_paise)>p.amount_paise",
    # Total non-rejected return units for an order line should not exceed ordered units.
    "return_quantity": "SELECT r.order_item_id FROM returns r JOIN order_item o ON r.order_item_id=o.order_item_id WHERE r.status<>'REJECTED' GROUP BY r.order_item_id,o.quantity HAVING SUM(r.quantity)>o.quantity",
    # Total shipment units not marked RTO should not exceed ordered units.
    "shipment_quantity": "SELECT s.order_item_id FROM shipment s JOIN order_item o ON s.order_item_id=o.order_item_id WHERE s.status<>'RTO' GROUP BY s.order_item_id,o.quantity HAVING SUM(s.quantity)>o.quantity",
    # Lines sharing an order ID should agree on customer, order time, state and payment method.
    "order_header": "SELECT order_id FROM order_item GROUP BY order_id HAVING COUNT(DISTINCT customer_id)>1 OR COUNT(DISTINCT ordered_at)>1 OR COUNT(DISTINCT state)>1 OR COUNT(DISTINCT payment_method)>1",
}

# Each entry contains (grouping keys, SQL query) for one reporting table, or mart.
# The grouping keys define one row within a publication; publication_id is added later.
# Reporting dates use Asia/Kolkata because these are Indian business events.
# GROUP BY 1,2,... refers to the corresponding SELECT column positions.
MARTS = {
    # One row per Indian order date, product category and delivery state.
    # Count distinct orders; total non-cancelled units/value; report cancelled units separately.
    # Join product for category and divide paise by 100 for rupees. Order counts across groups overlap.
    "daily_fashion_sales": (
        ["business_date", "category", "state"],
        """
        SELECT to_date(from_utc_timestamp(to_timestamp(o.ordered_at),'Asia/Kolkata')) business_date,
               p.category, o.state, COUNT(DISTINCT o.order_id) order_count,
               SUM(CASE WHEN o.status<>'CANCELLED' THEN o.quantity ELSE 0 END) booked_units,
               CAST(SUM(CASE WHEN o.status<>'CANCELLED' THEN o.quantity*o.unit_price_paise-o.discount_paise ELSE 0 END)/100.0 AS DECIMAL(20,2)) booked_value_inr,
               SUM(CASE WHEN o.status='CANCELLED' THEN o.quantity ELSE 0 END) cancelled_units
        FROM order_item o JOIN product p ON o.product_id=p.product_id
        GROUP BY 1,2,3
    """,
    ),
    # One row per Indian cash-event date and payment method.
    # The temporary cash result combines captured payments with completed refunds using UNION ALL.
    # Zero placeholders align both row types, then totals give captured, refunded and net rupees.
    # Keep cash separate from order-line joins to avoid multiplying payment amounts.
    "daily_collections": (
        ["business_date", "method"],
        """
        WITH cash AS (
          SELECT to_date(from_utc_timestamp(to_timestamp(paid_at),'Asia/Kolkata')) business_date,
                 method, amount_paise captured_paise, CAST(0 AS BIGINT) refunded_paise
          FROM payment WHERE status='CAPTURED'
          UNION ALL
          SELECT to_date(from_utc_timestamp(to_timestamp(f.refunded_at),'Asia/Kolkata')),
                 p.method, CAST(0 AS BIGINT), f.amount_paise
          FROM refund f JOIN payment p ON f.payment_id=p.payment_id WHERE f.status='COMPLETED'
        )
        SELECT business_date, method,
          CAST(SUM(captured_paise)/100.0 AS DECIMAL(20,2)) captured_inr,
          CAST(SUM(refunded_paise)/100.0 AS DECIMAL(20,2)) refunded_inr,
          CAST(SUM(captured_paise-refunded_paise)/100.0 AS DECIMAL(20,2)) net_collected_inr
        FROM cash GROUP BY 1,2
    """,
    ),
    # One row per Indian request date, category, size and return reason.
    # Join returns to order lines and products for classification; exclude rejected returns.
    # COUNT counts requests and SUM(quantity) counts returned units.
    "return_reasons": (
        ["business_date", "category", "size", "reason"],
        """
        SELECT to_date(from_utc_timestamp(to_timestamp(r.requested_at),'Asia/Kolkata')) business_date,
               p.category,p.size,r.reason,COUNT(*) return_requests,SUM(r.quantity) return_units
        FROM returns r JOIN order_item o ON r.order_item_id=o.order_item_id
        JOIN product p ON o.product_id=p.product_id WHERE r.status<>'REJECTED'
        GROUP BY 1,2,3,4
    """,
    ),
    # One row per Indian ship date, carrier and delivery state.
    # Count shipment allocations, delivered/on-time allocations, and return-to-origin (RTO) allocations.
    # These counts describe order-line allocations, not distinct parcels.
    "fulfillment": (
        ["business_date", "carrier", "state"],
        """
        SELECT to_date(from_utc_timestamp(to_timestamp(s.shipped_at),'Asia/Kolkata')) business_date,
               s.carrier,o.state,COUNT(*) shipment_items,
               SUM(CASE WHEN s.status='DELIVERED' THEN 1 ELSE 0 END) delivered_items,
               SUM(CASE WHEN s.status='DELIVERED' AND to_timestamp(s.delivered_at)<=to_timestamp(s.promised_at) THEN 1 ELSE 0 END) on_time_items,
               SUM(CASE WHEN s.status='RTO' THEN 1 ELSE 0 END) rto_items
        FROM shipment s JOIN order_item o ON s.order_item_id=o.order_item_id GROUP BY 1,2,3
    """,
    ),
}


def process(run):
    """Check Silver relationships, build reports, and expose the completed publication."""

    # 1. Load each current Silver table as a temporary SQL view for the queries above.
    # Later deliveries can be partial because previously accepted entity tables remain available.
    for entity in KEYS:
        # Use returns as the temporary name for the return entity to keep SQL names unambiguous.
        run.spark.table(run.table("silver", entity)).createOrReplaceTempView(
            "returns" if entity == "return" else entity
        )

    # 2. Run each relationship query and save the offending keys with its check name.
    # These diagnostic copies do not remove rows from Silver or stop the reporting queries.
    # Missing joins may omit rows, so a warning requires reviewing the resulting totals.
    for name, query in RELATIONSHIPS.items():
        run.quarantine(run.spark.sql(query), "relationships", name)

    # 3. Use the workflow run ID as the reporting version. Retries get a new ID.
    # All four reports written below share this publication_id.
    publication = run.run_id

    # 4. For each report, unpack its grouping keys and SQL, then calculate the full current result.
    for name, (keys, query) in MARTS.items():
        # Stamp every result row with the same publication ID using a constant column.
        df = run.spark.sql(query).withColumn("publication_id", F.lit(publication))
        # Record empty-report/missing-date quality warnings while allowing the write to continue.
        run.quality(df, name, 'Rules = [RowCount > 0, IsComplete "business_date"]')
        # Include publication_id in the merge key so a new report version preserves older versions.
        run.merge(df, "gold", name, ["publication_id"] + keys)

    # 5. After all four report writes succeed, prepare one pointer row: current -> this publication.
    # Current Athena views use this row to select a complete reporting version.
    pointer = run.spark.createDataFrame(
        [("current", publication)], ["id", "publication_id"]
    )
    target = run.table("gold", "publication_current")
    # On the first run, create the empty Iceberg pointer table from this two-column DataFrame.
    if not run.spark.catalog.tableExists(target):
        pointer.limit(0).writeTo(target).using("iceberg").tableProperty(
            "format-version", "2"
        ).tableProperty(
            "location", f"s3://{run.bucket}/warehouse/gold/publication_current"
        ).create()
    # Give the single pointer row a temporary SQL name for the following MERGE.
    pointer.createOrReplaceTempView("new_publication")
    # Insert the first current pointer, or replace the old pointer in one Iceberg table commit.
    # A failure in an earlier report write never reaches this visibility switch.
    run.spark.sql(
        f"MERGE INTO {target} t USING new_publication s ON t.id=s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )

    # 6. Create/refresh Athena views now that their underlying tables exist.
    # CodeBuild packaged the SQL with this release; no separate manual SQL setup is needed.
    run.publish_views()

    # 7. Record successful publication on the batch. Coordinator also checks actual Glue
    # step states before releasing ownership. If this update fails, retry makes a new
    # publication from current Silver; it does not add the totals to the previous report.
    run.registry.update_item(
        Key={"pk": "BATCH#" + run.batch_id},
        UpdateExpression="SET #s=:s, publication_id=:p",
        ExpressionAttributeNames={"#s": "state"},
        ExpressionAttributeValues={":s": "SUCCEEDED", ":p": publication},
    )


# Glue runs this block when it starts gold.py. Importing the file does not start a job.
if __name__ == "__main__":
    # Load workflow settings, the delivery manifest, AWS clients and a Spark session.
    run = Run("gold")
    try:
        process(run)
        # Log successful completion after all processing steps return.
        run.finish()
    # Log execution errors and re-raise them so Glue reports FAILED.
    # Recorded data-quality warnings do not raise exceptions here.
    except Exception as error:
        run.fail(error)
        raise
