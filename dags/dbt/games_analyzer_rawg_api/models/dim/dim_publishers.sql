--- This model creates dimension table for publishers data.
--- It groups the publishers data by publisher_id, publisher_name, and publisher_slug and displays the count of games present for that particular publisher.

{{config(
    materialized = 'view',
    unique_key = 'publisher_id'
)}}

SELECT
    publisher_id,
    publisher_name,
    COUNT(game_id) AS publishers_game_count
FROM {{ ref('stg_publishers') }}
WHERE game_id IN (SELECT game_id FROM {{ ref('stg_games') }} WHERE metacritic != 'None')
GROUP BY 1, 2
ORDER BY 1