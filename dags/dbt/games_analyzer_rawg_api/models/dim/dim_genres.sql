--- This model is responsible for creating the genres dimension table.
--- It groups the genres data by genre_id, genre_name, and genre_slug and displays the count of games present for that particular genre.

{{config(
    materialized = 'view',
    unique_key = 'genre_id'
)}}

SELECT 
    genre_id,
    genre_name,
    COUNT(game_id) AS genre_games_count
FROM {{ ref('stg_genres') }}
WHERE game_id IN (SELECT game_id FROM {{ ref('stg_games') }} WHERE metacritic != 'None')
GROUP BY 1, 2
ORDER BY 1