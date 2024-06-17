--- This model is responsible for creating the dim_platforms table
{{config(
    materialized = 'incremental',
    unique_key = 'id'
)}}

SELECT
    platform_id,
    platform_name,
    CASE
        WHEN game_released_at_platform_date = 'NaT' THEN '9999-12-31'
        ELSE game_released_at_platform_date
    END AS game_released_at_platform_date,
    game_released_at_platform_date,
    game_id,
FROM {{ ref('stg_platforms') }}
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})
{% endif %}