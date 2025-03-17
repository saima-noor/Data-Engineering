-- This model stages the raw marketing events data from the source system.
-- It performs minimal transformations, primarily ensuring data types are consistent.
-- The purpose of this model is to:
--   1. Load the raw marketing events data into the dbt project.
--   2. Provide a clean and consistent view of marketing events for downstream models.
--   3. Ensure data integrity by applying basic data quality tests (e.g., unique event_id).
--   4. Serve as a foundational layer for building more complex transformations and analyses,
--      such as calculating marketing costs by campaign or channel, or linking marketing events to sales.

SELECT
    event_id, -- Unique identifier for each marketing event. Primary key.
    user_id, -- User identifier associated with the event. Links to sales transactions.
    event_type, 
    event_timestamp, 
    channel, 
    campaign, 
    cost 

FROM {{ source('raw_data_sources', 'marketing_events') }} 
-- Selects data from the raw marketing_events table defined in sources.yml.

-- No further transformations are performed in this staging model.
-- Assumptions:
--   - The source data is relatively clean and consistent.
--   - The event_id is unique and can serve as the primary key.
--   - Data type consistency is assumed. Any data type issues should be handled in this model.
--   - The cost column represents a monetary value in a consistent currency.