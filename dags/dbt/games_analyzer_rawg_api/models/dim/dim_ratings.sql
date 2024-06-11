-- models/dim/dim_ratings.sql

-- Define the model using the raw ratings data source
{{ config(materialized='view') }}

WITH dim_ratings AS (
    SELECT
        id AS rating_id,
        title AS rating_category
    FROM {{ source('rawg_api_raw', 'ratings') }}
    GROUP BY id, title
)

SELECT *
FROM dim_ratings