-- Net collections are cash captures minus completed refunds, not recognized revenue.
SELECT business_date,method,captured_inr,refunded_inr,net_collected_inr
FROM ${DB}_gold.current_daily_collections ORDER BY business_date,method;
-- Order counts across categories are non-additive: an order can contain several categories.
SELECT category,state,sum(booked_units) units,sum(booked_value_inr) booked_inr
FROM ${DB}_gold.current_daily_fashion_sales GROUP BY 1,2 ORDER BY booked_inr DESC;
SELECT category,size,reason,sum(return_units) return_units
FROM ${DB}_gold.current_return_reasons GROUP BY 1,2,3 ORDER BY return_units DESC;
SELECT carrier,sum(delivered_items) delivered,
       round(100.0*sum(on_time_items)/nullif(sum(delivered_items),0),2) on_time_percent
FROM ${DB}_gold.current_fulfillment GROUP BY 1;
