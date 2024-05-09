-- Reclamações positivas últimos 30 dias
-- Arquivo: models/positive_reviews_30_days.sql

{{ config(
    materialized='table',
    file_format='delta',
    schema='cs'
) }}

SELECT
    COUNT(DISTINCT id_review) AS reviews_positivas
FROM
    {{ ref('reviews_clients') }}
WHERE
    pontuacao >= 4;