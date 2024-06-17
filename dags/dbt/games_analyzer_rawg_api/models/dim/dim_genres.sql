--- This model is responsible for creating the genres dimension table.
--- It groups the genres data by genre_id, genre_name, and genre_slug and displays the count of games present for that particular genre.

{{config(
    materialized = 'incremental',
    unique_key = 'genre_id'
)}}

SELECT 
    genre_id,
    genre_name,
    genre_slug,
    COUNT(game_id) AS genre_games_count
FROM {{ ref('stg_genres') }}
GROUP BY 1, 2, 3
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})
{% endif %}