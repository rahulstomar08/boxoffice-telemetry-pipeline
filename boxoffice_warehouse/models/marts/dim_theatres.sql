{{ config(materialized='table') }}

SELECT
    MD5(theatre_id) AS theatre_sk,
    theatre_id,
    theatre_name,
    chain_name,
    city_code,
    CASE
        WHEN city_code = 'DEL' THEN 'Delhi NCR'
        WHEN city_code = 'BLR' THEN 'Bengaluru'
        ELSE city_code
    END AS city_name,
    sub_zone,
    is_single_screen,
    CAST(offline_multiplier_weight AS DECIMAL(4,2)) AS offline_multiplier_weight,
    CAST(total_capacity AS INT) AS total_capacity
FROM {{ ref('seed_theatres') }}