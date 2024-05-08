--Avaliação média por produto
-- Arquivo: models/reviews_per_product.sql

{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT
    pr.id_produto AS produto,
    pr.categoria_produto AS categoria,
    AVG(r.pontuacao) AS avaliacao_media
FROM
    {{ source('dev', 'products') }} AS pr
LEFT JOIN
    {{ source('dev', 'order_items') }} AS oi
ON
    oi.id_produto = pr.id_produto
LEFT JOIN
    {{ source('dev', 'orders') }} AS o
ON
    oi.id_pedido = o.id_pedido
LEFT JOIN
    {{ source('dev', 'reviews') }} AS r
ON
    r.id_pedido = o.id_pedido
GROUP BY
    1, 2
ORDER BY
    avaliacao_media DESC
LIMIT 10;
