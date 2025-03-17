-- models/staging/stg_sales_transactions.sql

-- This model stages the raw sales transactions data from the source system.
-- It performs minimal transformations, primarily ensuring data types are consistent.
-- The purpose of this model is to:
--   1. Load the raw sales transactions data into the dbt project.
--   2. Provide a clean and consistent view of sales transactions for downstream models.
--   3. Ensure data integrity by applying basic data quality tests (e.g., unique transaction_id).
--   4. Serve as a foundational layer for building more complex transformations and analyses,
--      such as calculating sales by product or user, or linking sales to marketing events.

SELECT
    transaction_id, -- Unique identifier for each sales transaction. Primary key.
    product_id, -- Product identifier associated with the transaction. Links to product catalog.
    user_id, -- User identifier associated with the transaction. Links to marketing events.
    transaction_timestamp,
    revenue, 
    cost 

FROM {{ source('raw_data_sources', 'sales_transactions') }} 
-- Selects data from the raw sales_transactions table defined in sources.yml.

-- No further transformations are performed in this staging model.
-- Assumptions:
--   - The source data is relatively clean and consistent.
--   - The transaction_id is unique and can serve as the primary key.
--   - Data type consistency is assumed. Any data type issues should be handled in this model.
--   - The revenue and cost columns represent monetary values in a consistent currency.