---Select only those games that have a numeric metacritic score
{% macro select_game_ids_with_metacritic_values() %}
    SELECT 
        COUNT(game_id) 
    FROM {{ ref('stg_games') }} 
    WHERE metacritic!='None'
{% endmacro %}