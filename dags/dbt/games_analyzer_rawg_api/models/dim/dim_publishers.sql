--- This model creates dimension table for publishers data.
--- It groups the publishers data by publisher_id, publisher_name, and publisher_slug and displays the count of games present for that particular publisher.

{{config(
    materialized = 'incremental',
    unique_key = 'publisher_id'
)}}

SELECT
    publisher_id,
    publisher_name,
    publisher_slug,
    COUNT(game_id) AS publishers_game_count,
FROM {{ ref('stg_publishers') }}
GROUP BY 1, 2, 3
{% if is_incremental() %}
    WHERE load_date = (SELECT MAX(load_date) FROM {{ this }})
{% endif %}