Welcome to the new dbt project!

Read the comments in the .sql and .yml files to understand the purpose and functions of the files.

The models, tests, macro and documentations are created to optimize the given SQL query for transformations in Redshift.


Data:

Assume the following tables are available in Redshift:
1. `marketing_events`: Records data from various marketing initiatives.
 Columns: `event_id`, `user_id`, `event_type`, `event_timestamp`,
`channel`, `campaign`, `cost`
2. `sales_transactions`: Captures all sales data.
 Columns: `transaction_id`, `product_id`, `user_id`,
`transaction_timestamp`, `revenue`, `cost`
3. `product_catalog`: Contains details of products.
 Columns: `product_id`, `product_name`, `category`



Original Query (Redshift SQL):

SELECT
EXTRACT(YEAR FROM s.transaction_timestamp) AS transaction_year,
EXTRACT(MONTH FROM s.transaction_timestamp) AS
transaction_month,
p_cat.category,
COUNT(DISTINCT s.user_id) AS unique_customers,
SUM(s.revenue) AS total_revenue,
SUM(s.cost) AS total_cost,
SUM(
SELECT SUM(me.cost)
FROM marketing_events me

WHERE EXTRACT(YEAR FROM me.event_timestamp) = EXTRACT(YEAR
FROM s.transaction_timestamp)
AND EXTRACT(MONTH FROM me.event_timestamp) = EXTRACT(MONTH
FROM s.transaction_timestamp)
) AS total_marketing_cost,
COUNT(me.event_id) AS total_marketing_events,
-- Calculating the ratio of total revenue to total cost
CASE
WHEN SUM(s.cost) > 0 THEN (SUM(s.revenue) / SUM(s.cost))
ELSE 0
END AS revenue_to_cost_ratio
FROM
sales_transactions s
LEFT JOIN product_catalog p_cat ON s.product_id = p_cat.product_id
LEFT JOIN marketing_events me ON s.user_id = me.user_id
AND EXTRACT(YEAR FROM s.transaction_timestamp) = EXTRACT(YEAR FROM
me.event_timestamp)
AND EXTRACT(MONTH FROM s.transaction_timestamp) = EXTRACT(MONTH FROM
me.event_timestamp)
GROUP BY
transaction_year, transaction_month, p_cat.category
HAVING
total_revenue > 0
ORDER BY transaction_year, transaction_month, p_cat.category;


Deliverables:
o The refactored SQL code structured for dbt (including models and configurations).
o The dbt tests you implemented for data quality assurance.
o The dbt macro you created, including an explanation of its purpose.
o Documentation outlining the purpose and structure of each model and any
important notes.


The required deliverables that complete the task are in the .sql and .yml files