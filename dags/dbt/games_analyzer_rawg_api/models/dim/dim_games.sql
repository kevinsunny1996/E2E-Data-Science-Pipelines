{{config(
    materialized = 'incremental',
    unique_key = 'game_id'
)}}

SELECT 
    game_id,
    game_name,
    game_slug,
    description_raw,
    game_release_date,
    tba,
    game_updated_at
FROM {{ ref('stg_games') }}
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})
{% endif %}