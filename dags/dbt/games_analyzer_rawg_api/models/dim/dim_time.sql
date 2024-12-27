{{config(
    materialized = 'view',
    unique_key = 'time_id'
)}}

SELECT
    DISTINCT
    {{ dbt_utils.generate_surrogate_key(['released']) }} AS time_id, --- Map this time ID to Fact games table
    released,
    EXTRACT(YEAR FROM DATE(released)) AS year,
    EXTRACT(MONTH FROM DATE(released)) AS month,
    EXTRACT(DAY FROM DATE(released)) AS day,
    FORMAT_DATE('%B',DATE(released)) AS month_name,
    CASE
        WHEN EXTRACT(MONTH FROM DATE(released)) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM DATE(released)) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM DATE(released)) BETWEEN 7 AND 9 THEN 'Q3'
        ELSE 'Q4'
    END AS quarter
FROM {{ ref('stg_games') }}
WHERE released IS NOT NULL and released != 'NaT' and metacritic!='None'
GROUP BY released
