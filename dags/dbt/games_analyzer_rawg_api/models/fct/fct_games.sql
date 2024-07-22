--- This model is responsible for storing the games data from the RAWG API in the fct_games table
--- The fct_games table is a fact table that contains the games quantitative data
--- All 5 metacritic categories taken from - https://en.wikipedia.org/wiki/Metacritic
{{config(
    materialized = 'incremental',
    unique_key = 'game_id',
    incremental_strategy = 'merge'
)}}

WITH unique_stg_games AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY load_date DESC) AS row_num
    FROM {{ ref('stg_games') }}
)

SELECT 
    game_id,
    rating_top AS rating_id,
    {{ dbt_utils.generate_surrogate_key(['released']) }} AS time_id, --- Maps to dim_time table
    game_name,
    description_raw AS game_description,
    rating,
    playtime,
    metacritic AS metacritic_score,
    CASE
        WHEN CAST(metacritic AS INT64) BETWEEN 90 AND 100 THEN 'Universal acclaim'
        WHEN CAST(metacritic AS INT64) BETWEEN 75 AND 89 THEN 'Generally favorable'
        WHEN CAST(metacritic AS INT64) BETWEEN 50 AND 74 THEN 'Mixed or average'
        WHEN CAST(metacritic AS INT64) BETWEEN 20 AND 49 THEN 'Generally unfavorable'
        ELSE 'Overwhelming dislike'
    END AS metacritic_category
FROM unique_stg_games
{% if is_incremental() %}
    WHERE row_num = 1 AND load_date >= (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }})
    AND metacritic != 'None'
{% endif %}