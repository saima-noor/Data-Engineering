

-- This model generates a final report combining sales, product, and marketing data,
-- aggregated by year, month, and product category.
-- It is designed to provide insights into sales performance, marketing effectiveness,
-- and the overall profitability of different product categories over time.

SELECT
    {{ extract_year_month('s.transaction_timestamp') }}, -- Extracts year and month from sales transaction timestamp.
    p_cat.category, -- Product category from the product catalog.
    COUNT(DISTINCT s.user_id) AS unique_customers, -- Counts unique customers per year, month, and category.
    SUM(s.revenue) AS total_revenue, -- Calculates total revenue per year, month, and category.
    SUM(s.cost) AS total_cost, -- Calculates total cost of sales per year, month, and category.
    mc.total_marketing_cost, -- Retrieves total marketing cost from the pre-aggregated marketing costs model.
    COUNT(me.event_id) AS total_marketing_events, -- Counts marketing events associated with sales per year, month, and category.
    CASE
        WHEN SUM(s.cost) > 0 THEN (SUM(s.revenue) / SUM(s.cost)) -- Calculates revenue to cost ratio.
        ELSE 0 -- Returns 0 if total cost is 0 to avoid division by zero.
    END AS revenue_to_cost_ratio -- Alias for the calculated ratio.

FROM {{ ref('stg_sales_transactions') }} s -- Joins with sales transactions staging model.
LEFT JOIN {{ ref('stg_product_catalog') }} p_cat ON s.product_id = p_cat.product_id -- Joins on product ID to get product category.
LEFT JOIN {{ ref('stg_marketing_events') }} me ON s.user_id = me.user_id -- Joins on user ID to link sales with marketing events.
    AND year = (SELECT year FROM {{ extract_year_month('me.event_timestamp') }}) -- Filters marketing events by year, using the macro.
    AND month = (SELECT month FROM {{ extract_year_month('me.event_timestamp') }}) -- Filters marketing events by month, using the macro.
LEFT JOIN {{ ref('int_marketing_costs') }} mc ON year = mc.event_year -- Joins with pre-aggregated marketing costs model.
    AND month = mc.event_month -- Joins on year and month.

GROUP BY year, month, p_cat.category, mc.total_marketing_cost -- Groups the results by year, month, category, and marketing cost.
HAVING total_revenue > 0 -- Filters out groups with no revenue.
ORDER BY year, month, p_cat.category; -- Orders the results by year, month, and category.

-- Key Assumptions:
-- - The 'year' and 'month' columns used in joins are derived from timestamps.
-- - Marketing costs are pre-aggregated in the 'int_marketing_costs' model for efficiency.
-- - User IDs are used to link sales transactions with marketing events.
-- - Revenue and costs are assumed to be in a consistent currency.
-- - The macro extract_year_month is used to extract the year and month from the timestamp.
-- - Subqueries are used within the join to extract the year and month from the marketing events timestamp. This may have performance implications.