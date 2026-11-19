{{ config(materialized='table') }}

WITH t10_final_snapshots AS (
    SELECT 
        snap.show_id,
        mov.movie_sk,
        th.theatre_sk,
        th.city_name,
        snap.base_price_inr,
        snap.booked_seats,
        snap.tracked_gross_inr,
        th.offline_multiplier_weight
    FROM {{ ref('stg_seat_snapshots') }} snap
    JOIN {{ ref('dim_theatres') }} th 
        ON snap.theatre_id = th.theatre_id
    JOIN {{ ref('dim_movies') }} mov 
        ON snap.movie_id = mov.movie_id
    WHERE snap.snapshot_window = 'T-10m'
),

tax_and_extrapolation AS (
    SELECT
        show_id,
        movie_sk,
        theatre_sk,
        city_name,
        tracked_gross_inr,
        ROUND(tracked_gross_inr * offline_multiplier_weight, 2) AS estimated_total_gross,
        CASE 
            WHEN base_price_inr > 100.00 THEN ROUND((tracked_gross_inr * offline_multiplier_weight) / 1.18, 2)
            ELSE ROUND((tracked_gross_inr * offline_multiplier_weight) / 1.12, 2)
        END AS india_net_collection
    FROM t10_final_snapshots
)

SELECT 
    CURRENT_DATE AS calculation_date,
    show_id,
    movie_sk,
    theatre_sk,
    city_name,
    SUM(tracked_gross_inr) AS total_tracked_gross_inr,
    SUM(estimated_total_gross) AS total_est_gross_inr,
    SUM(india_net_collection) AS total_india_net_inr
FROM tax_and_extrapolation
GROUP BY 1, 2, 3, 4, 5