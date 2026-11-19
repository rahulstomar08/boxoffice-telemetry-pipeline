WITH raw_snapshots AS (
    SELECT * FROM {{ source('boxoffice_raw', 'staging_seat_snapshots') }}
)

SELECT
    dedup_hash,
    CAST(show_id AS VARCHAR(128)) AS show_id,
    CAST(movie_id AS VARCHAR(64)) AS movie_id,
    CAST(theatre_id AS VARCHAR(64)) AS theatre_id,
    CAST(city_code AS VARCHAR(10)) AS city_code,
    CAST(snapshot_window AS VARCHAR(20)) AS snapshot_window,
    CAST(category_name AS VARCHAR(54)) AS category_name,
    CAST(base_price_inr AS DECIMAL(10,2)) AS base_price_inr,
    CAST(booked_seats AS INT) AS booked_seats,
    CAST(tracked_gross_inr AS DECIMAL(15,2)) AS tracked_gross_inr,
    CAST(updated_at AS TIMESTAMP) AS ingested_at_utc
FROM raw_snapshots