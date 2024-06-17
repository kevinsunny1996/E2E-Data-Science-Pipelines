--- This model is responsible for storing the games data from the RAWG API in the fct_games table
--- The fct_games table is a fact table that contains the games quantitative data
{{config(
    materialized = 'incremental',
    unique_key = 'game_id'
)}}

SELECT 
    game_id,
    rating,
    rating_top,
    playtime,
    metacritic_score
FROM {{ ref('stg_games') }}
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})
{% endif %}