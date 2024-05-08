-- Reclamações Negativas últimos 30 dias
-- Arquivo: models/negative_reviews_30_days.sql

{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT
    COUNT(DISTINCT id_review) AS reviews_negativas
FROM
    {{ ref('reviews_clients') }}
WHERE
    pontuacao <= 3;
