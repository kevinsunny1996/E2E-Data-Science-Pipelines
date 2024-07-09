-- models/dim/dim_ratings.sql

-- Define the model using the raw ratings data source
{{ config(
    materialized='view'
) }}

SELECT
    id AS rating_id,
    title AS rating_category,
    COUNT(ratings_raw.game_id) AS rating_count
FROM {{ source('rawg_api_raw', 'ratings') }} ratings_raw LEFT JOIN {{ ref('stg_games') }} games_eph
ON ratings_raw.game_id=games_eph.game_id AND ratings_raw.id=games_eph.rating_top
WHERE games_eph.metacritic != 'None'
GROUP BY 1, 2
ORDER BY 1