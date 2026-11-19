{{ config(materialized='table') }}

SELECT
    MD5(movie_id) AS movie_sk,
    movie_id,
    title,
    language,
    format,
    censor_rating
FROM {{ ref('seed_movies') }}