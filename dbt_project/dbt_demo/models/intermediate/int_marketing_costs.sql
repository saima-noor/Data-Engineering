--  Creating int_marketing_costs.sql separates the marketing cost aggregation logic into a dedicated model.

SELECT
    {{ extract_year_month('event_timestamp') }},
    SUM(cost) AS total_marketing_cost
FROM {{ ref('stg_marketing_events') }}
GROUP BY year, month

--The macro {{ extract_year_month('event_timestamp') }} is used directly in the SELECT clause, generating the year and month columns.
-- The macro reduces the use of Extract() function calls.
-- By creating int_marketing_costs.sql, you pre-aggregate the marketing costs by year and month. 
-- This means you calculate the sum of cost for each distinct combination of event_year and event_month only once.
-- This significantly reduces redundant calculations and improves the performance of the final model.