-- This model stages the raw product catalog data from the source system.
-- It performs minimal transformations, primarily ensuring data types are consistent.
-- The purpose of this model is to:
--   1. Load the raw product catalog data into the dbt project.
--   2. Provide a clean and consistent view of the product catalog for downstream models.
--   3. Ensure data integrity by applying basic data quality tests (e.g., unique product_id).
--   4. Serve as a foundational layer for building more complex transformations and analyses.

SELECT
    product_id, -- Unique identifier for each product. Primary key.
    product_name, 
    category 

FROM {{ source('raw_data_sources', 'product_catalog') }} 
-- Selects data from the raw product_catalog table defined in sources.yml.

-- No further transformations are performed in this staging model.
-- Assumptions:
--   - The source data is relatively clean and consistent.
--   - The product_id is unique and can serve as the primary key.
--   - Data type consistency is assumed. Any data type issues should be handled in this model.