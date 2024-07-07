-- models/staging/stg_games.sql

-- Define the model using the raw games data source
{{ config(materialized='ephemeral') }}

SELECT
    id AS game_id,
    name_original AS game_name,
    slug AS game_slug,
    description_raw,
    released,
    tba,
    updated,
    rating,
    rating_top,
    playtime,
    metacritic,
    load_date
FROM {{ source('rawg_api_raw', 'games') }}
WHERE metacritic != 'None'