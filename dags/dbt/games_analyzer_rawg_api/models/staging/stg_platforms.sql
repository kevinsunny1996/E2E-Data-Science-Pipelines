-- models/staging/stg_platforms.sql

-- Define the model using the raw platforms data source
{{ config(materialized='ephemeral') }}

WITH cleaned_raw_platforms AS (
    SELECT
        platform_id,
        platform_name,
        PARSE_DATE('%Y-%m-%d', TRIM(released_at)) AS game_released_at_platform_date,
        game_id,
        load_date
    FROM {{ source('rawg_api_raw', 'platforms') }}
)

SELECT *
FROM cleaned_raw_platforms