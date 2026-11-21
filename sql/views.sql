-- Gold runs this versioned SQL after writing its tables and publication pointer.
-- Pin publication_id for multi-query reports so new publications cannot mix pages.
CREATE OR REPLACE VIEW ${DB}_gold.current_daily_fashion_sales AS
SELECT g.* FROM ${DB}_gold.daily_fashion_sales g
JOIN ${DB}_gold.publication_current p ON g.publication_id=p.publication_id WHERE p.id='current';
CREATE OR REPLACE VIEW ${DB}_gold.current_daily_collections AS
SELECT g.* FROM ${DB}_gold.daily_collections g
JOIN ${DB}_gold.publication_current p ON g.publication_id=p.publication_id WHERE p.id='current';
CREATE OR REPLACE VIEW ${DB}_gold.current_return_reasons AS
SELECT g.* FROM ${DB}_gold.return_reasons g
JOIN ${DB}_gold.publication_current p ON g.publication_id=p.publication_id WHERE p.id='current';
CREATE OR REPLACE VIEW ${DB}_gold.current_fulfillment AS
SELECT g.* FROM ${DB}_gold.fulfillment g
JOIN ${DB}_gold.publication_current p ON g.publication_id=p.publication_id WHERE p.id='current';
CREATE OR REPLACE VIEW ${DB}_silver.orders AS
SELECT order_id,customer_id,ordered_at,city,state,payment_method,
       sum(quantity*unit_price_paise-discount_paise) order_value_paise
FROM ${DB}_silver.order_item GROUP BY 1,2,3,4,5,6;
