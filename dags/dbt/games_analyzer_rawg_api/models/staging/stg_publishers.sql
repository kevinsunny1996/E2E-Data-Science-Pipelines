-- models/staging/stg_publishers.sql

-- Define the model using the raw publishers data source
{{ config(materialized='ephemeral') }}

SELECT
    id AS publisher_id,
    name AS publisher_name,
    slug AS publisher_slug,
    game_id,
    load_date
FROM {{ source('rawg_api_raw', 'publishers') }}