
--Vendas por categoria de produto
-- Arquivo: models/sales_per_product.sql

{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT
    pr.categoria_produto AS categoria,
    COUNT(o.id_pedido) AS pedidos
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
GROUP BY
    categoria
ORDER BY
    pedidos DESC;
