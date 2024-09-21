-- depends_on: {{ ref('fct_games') }}

{{config(
    materialized = 'incremental',
    unique_key = 'bridge_gg_id',
    incremental_strategy = 'insert_overwrite'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['game_id','genre_id']) }} AS bridge_gg_id,
    game_id,
    genre_id
FROM {{ ref('stg_genres') }}
{% if is_incremental() %}
    WHERE load_date >= (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }}) 
    AND game_id IN (SELECT game_id FROM {{ ref('fct_games') }} WHERE metacritic_score != 'None')     
{% endif %}