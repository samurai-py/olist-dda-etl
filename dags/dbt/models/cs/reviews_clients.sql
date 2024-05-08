-- Reclamações por cliente
-- Arquivo: models/reviews_clients.sql

{{ config(
    materialized='table',
    file_format='delta'
) }}

WITH data_limite AS (
    SELECT DATEADD(DAY, -30, MAX(data_criacao)) AS data_limite
    FROM {{ source('dev', 'reviews') }}
)

SELECT
    c.identificador_cliente AS pessoa,
    r.id_review,
    r.pontuacao,
    COUNT(DISTINCT r.id_review) AS reclamacoes
FROM
    {{ source('dev', 'customers') }} AS c
LEFT JOIN
    {{ source('dev', 'orders') }} AS o
ON
    o.id_cliente = c.id_cliente
LEFT JOIN
    {{ source('dev', 'reviews') }} AS r
ON
    r.id_pedido = o.id_pedido
WHERE
    r.data_criacao >= (SELECT data_limite FROM data_limite)
    AND r.pontuacao <= 3
GROUP BY
    1, 2, 3
ORDER BY
    reclamacoes DESC;
