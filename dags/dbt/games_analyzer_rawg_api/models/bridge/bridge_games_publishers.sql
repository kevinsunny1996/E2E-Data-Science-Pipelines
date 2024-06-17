{{config(
    materialized = 'incremental'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['game_id', 'publisher_id']) }} AS game_publisher_bridge_id,
    game_id,
    publisher_id
FROM {{ ref('stg_publishers') }}
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})      
{% endif %}