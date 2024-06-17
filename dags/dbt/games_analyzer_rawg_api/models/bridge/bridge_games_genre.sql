{{config(
    materialized = 'incremental'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['game_id', 'genre_id']) }} AS game_genre_bridge_id,
    game_id,
    genre_id
FROM {{ ref('stg_genres') }}
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})      
{% endif %}