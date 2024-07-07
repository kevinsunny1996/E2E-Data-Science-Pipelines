-- models/staging/stg_platforms.sql

-- Define the model using the raw platforms data source
{{ config(materialized='ephemeral') }}

SELECT
    platform_id,
    platform_name,
    platform_slug,
    platform_image,
    platform_year_end,
    platform_games_count,
    platform_image_background,
    game_id,
    platform_year_start,
    released_at,
    load_date
FROM {{ source('rawg_api_raw', 'platforms') }}