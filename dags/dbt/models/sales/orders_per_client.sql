
--Pedidos por cliente
-- Arquivo: models/orders_per_client.sql

{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT
    c.identificador_cliente AS pessoa,
    COUNT(DISTINCT o.id_pedido) AS pedidos
FROM
    {{ source('dev', 'customers') }} AS c
LEFT JOIN
    {{ source('dev', 'orders') }} AS o
ON
    o.id_cliente = c.id_cliente
GROUP BY
    pessoa
ORDER BY
    pedidos DESC;
