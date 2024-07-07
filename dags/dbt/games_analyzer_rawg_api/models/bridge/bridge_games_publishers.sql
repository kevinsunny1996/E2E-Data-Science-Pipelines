-- depends_on: {{ ref('stg_games') }}

{{config(
    materialized = 'incremental'
)}}

SELECT
    game_id,
    publisher_id
FROM {{ ref('stg_publishers') }}
{% if is_incremental() %}
    WHERE load_date >= (SELECT COALESCE(MAX(load_date), '1900-01-01') FROM {{ this }}) 
    AND game_id IN (SELECT game_id FROM {{ ref('stg_games') }} WHERE metacritic != 'None')
{% endif %}