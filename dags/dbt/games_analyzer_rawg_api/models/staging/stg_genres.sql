-- models/staging/stg_genres.sql

-- Define the model using the raw genres data source
{{ config(materialized='ephemeral') }}

SELECT
    id AS genre_id,
    name AS genre_name,
    slug AS genre_slug,
    game_id,
    load_date,
    ROW_NUMBER() OVER (PARTITION BY id, game_id ORDER BY load_date DESC) AS genre_row_id
FROM {{ source('rawg_api_raw', 'genres') }}