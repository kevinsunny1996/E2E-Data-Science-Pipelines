-- models/dim/dim_ratings.sql

-- Define the model using the raw ratings data source
{{ config(materialized='view') }}

WITH dim_ratings AS (
    SELECT
        id AS rating_id,
        title AS rating_category,
        COUNT(id) AS rating_count,
        COUNT(id) / (SELECT COUNT(*) FROM {{ source('rawg_api_raw', 'ratings') }}) AS rating_percentage
    FROM {{ source('rawg_api_raw', 'ratings') }}
    GROUP BY id, title
)

SELECT *
FROM dim_ratings