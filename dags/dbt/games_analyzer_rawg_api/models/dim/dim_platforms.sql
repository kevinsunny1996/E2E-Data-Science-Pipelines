--- This model is responsible for creating the dim_platforms table
--- It groups the platforms data by platform_id, platform_name, and platform_slug and displays the count of games present for that particular platform.
--- The release date of a game for the specific platform will be appended to the bridge table to provide more context.
{{config(
    materialized = 'view',
    unique_key = 'id'
)}}

SELECT
    platform_id,
    platform_name,
    COUNT(game_id) AS platform_games_count
FROM {{ ref('stg_platforms') }}
WHERE game_id IN (SELECT game_id FROM {{ ref('stg_games') }} WHERE metacritic != 'None')
GROUP BY 1, 2
ORDER BY 1