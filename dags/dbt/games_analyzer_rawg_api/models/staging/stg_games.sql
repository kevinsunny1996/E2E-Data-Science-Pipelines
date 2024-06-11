-- models/staging/stg_games.sql

-- Define the model using the raw games data source
{{ config(materialized='ephemeral') }}

WITH cleaned_raw_games AS (
    SELECT
        id AS game_id,
        name_original AS game_name,
        slug AS game_slug,
        description_raw,
        PARSE_DATE('%Y-%m-%d', TRIM(released)) AS game_release_date,
        CASE
            WHEN LOWER(CAST(tba AS STRING)) = 'true' THEN TRUE
            WHEN LOWER(CAST(tba AS STRING)) = 'false' THEN FALSE
            ELSE NULL
        END AS tba,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(updated)) AS game_updated_at,
        rating,
        rating_top,
        playtime,
        CAST(IFNULL(metacritic, 0) AS INT64) AS metacritic_score,
        load_date
    FROM {{ source('rawg_api_raw', 'games') }}
)

SELECT *
FROM cleaned_raw_games