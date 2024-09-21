-- depends_on: {{ ref('fct_games') }}

{{config(
    materialized = 'incremental',
    unique_key = 'bridge_gpl_id',
    incremental_strategy = 'merge'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['game_id','platform_id','released_at']) }} AS bridge_gpl_id,
    game_id,
    platform_id,
    released_at
FROM {{ ref('stg_platforms') }}
{% if is_incremental() %}
    WHERE load_date >= (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }}) 
    AND game_id IN (SELECT game_id FROM {{ ref('fct_games') }} WHERE metacritic_score != 'None')
    AND released_at != 'NaT'
{% endif %}