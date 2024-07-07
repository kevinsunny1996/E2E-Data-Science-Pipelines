-- models/dim/dim_ratings.sql

-- Define the model using the raw ratings data source
{{ config(
    materialized='view'
) }}

SELECT
    id AS rating_id,
    title AS rating_category,
    COUNT(id) AS rating_count
FROM {{ source('rawg_api_raw', 'ratings') }}
WHERE game_id IN (SELECT game_id FROM {{ ref('stg_games') }} WHERE metacritic != 'None')
GROUP BY 1, 2
ORDER BY 1