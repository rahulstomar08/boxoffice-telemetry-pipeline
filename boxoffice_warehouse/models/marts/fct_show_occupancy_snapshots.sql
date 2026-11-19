{{ config(materialized='table') }}

WITH snapshot_base AS (
    SELECT
        snap.show_id,
        mov.movie_sk,
        th.theatre_sk,
        th.city_name,
        th.sub_zone,
        th.total_capacity,
        snap.snapshot_window,
        snap.booked_seats,
        snap.tracked_gross_inr,
        snap.ingested_at_utc
    FROM {{ ref('stg_seat_snapshots') }} snap
    JOIN {{ ref('dim_theatres') }} th
        ON snap.theatre_id = th.theatre_id
    JOIN {{ ref('dim_movies') }} mov
        ON snap.movie_id = mov.movie_id
)

SELECT
    MD5(CONCAT(show_id, '-', snapshot_window)) AS snapshot_sk,
    show_id,
    movie_sk,
    theatre_sk,
    city_name,
    sub_zone,
    snapshot_window,
    CASE snapshot_window
        WHEN 'T-24h' THEN 1
        WHEN 'T-6h'  THEN 2
        WHEN 'T-2h'  THEN 3
        WHEN 'T-30m' THEN 4
        WHEN 'T-10m' THEN 5
        ELSE 6
    END AS window_sort_order,
    total_capacity,
    SUM(booked_seats) AS total_booked_seats,
    ROUND((SUM(booked_seats)::DECIMAL / NULLIF(total_capacity, 0)) * 100, 2) AS occupancy_pct,
    SUM(tracked_gross_inr) AS tracked_gross_inr,
    MAX(ingested_at_utc) AS last_updated_utc
FROM snapshot_base
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9