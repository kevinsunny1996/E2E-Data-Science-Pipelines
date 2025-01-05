-- depends_on: {{ ref('fct_games') }}

{{config(
    materialized = 'incremental',
    unique_key = 'bridge_gpu_id',
    incremental_strategy = 'merge'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['game_id','publisher_id']) }} AS bridge_gpu_id,
    game_id,
    publisher_id
FROM {{ ref('stg_publishers') }}
{% if is_incremental() %}
    WHERE load_date >= (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }}) 
    AND publisher_row_id = 1
    AND game_id IN (SELECT game_id FROM {{ ref('fct_games') }} WHERE metacritic_score != 'None')
{% endif %}