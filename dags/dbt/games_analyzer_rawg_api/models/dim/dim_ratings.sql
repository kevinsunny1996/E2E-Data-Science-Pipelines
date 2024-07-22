-- models/dim/dim_ratings.sql

-- Define the model using the raw ratings data source
-- depends_on: {{ ref('fct_games') }}

{{ config(
    materialized='view'
) }}

SELECT
    id AS rating_id,
    title AS rating_category,
    COUNT(ratings_raw.game_id) AS rating_count
FROM {{ source('rawg_api_raw', 'ratings') }} ratings_raw LEFT JOIN {{ ref('fct_games') }} games_fact
ON ratings_raw.game_id=games_fact.game_id AND ratings_raw.id=games_fact.rating_id
WHERE games_fact.metacritic_score != 'None'
GROUP BY 1, 2
ORDER BY 1