{{ config(materialized='table') }}

WITH t10_final_snapshots AS (
    SELECT 
        show_id,
        city_code,
        base_price_inr,
        booked_seats,
        tracked_gross_inr,
        -- Applying offline counter ticket multipliers based on region
        CASE 
            WHEN city_code = 'DEL' THEN 1.35 -- Delhi single screens have higher cash walk-ins
            ELSE 1.08 -- Bengaluru multiplexes are mostly online
        END AS offline_multiplier_weight
    FROM {{ ref('stg_seat_snapshots') }}
    WHERE snapshot_window = 'T-10m'
),

tax_and_extrapolation AS (
    SELECT
        show_id,
        city_code,
        tracked_gross_inr,
        
        -- Extrapolate total gross including offline walk-ins
        ROUND(tracked_gross_inr * offline_multiplier_weight, 2) AS estimated_total_gross,
        
        -- Statutory Indian Dynamic Ticket GST Logic
        -- Net = Gross / (1 + GST%)
        CASE 
            WHEN base_price_inr > 100.00 THEN ROUND((tracked_gross_inr * offline_multiplier_weight) / 1.18, 2)
            ELSE ROUND((tracked_gross_inr * offline_multiplier_weight) / 1.12, 2)
        END AS india_net_collection
    FROM t10_final_snapshots
)

SELECT 
    CURRENT_DATE AS calculation_date,
    show_id,
    city_code,
    SUM(tracked_gross_inr) AS total_tracked_gross_inr,
    SUM(estimated_total_gross) AS total_est_gross_inr,
    SUM(india_net_collection) AS total_india_net_inr
FROM tax_and_extrapolation
GROUP BY 1, 2, 3