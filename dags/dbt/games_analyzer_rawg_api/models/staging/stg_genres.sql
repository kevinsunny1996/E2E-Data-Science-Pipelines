-- models/staging/stg_genres.sql

-- Define the model using the raw publishers data source
{{ config(materialized='ephemeral') }}

WITH cleaned_raw_genres AS (
    SELECT
        id AS genre_id,
        name AS genre_name,
        slug AS genre_slug,
        game_id,
        load_date
    FROM {{ source('rawg_api_raw', 'genres') }}
)

SELECT *
FROM cleaned_raw_genres