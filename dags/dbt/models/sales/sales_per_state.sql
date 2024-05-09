
--Vendas por Estado
-- Arquivo: models/sales_per_product.sql

{{ config(
    materialized='table',
    file_format='delta',
    schema='sales'
) }}

SELECT
    c.UF,
    SUM(p.valor_pagamento) AS valor_vendas
FROM
    {{ source('dev', 'customers') }} AS c
LEFT JOIN
    {{ source('dev', 'orders') }} AS o
ON
    o.id_cliente = c.id_cliente
LEFT JOIN
    {{ source('dev', 'payments') }} AS p
ON
    p.id_pedido = o.id_pedido
GROUP BY
    c.UF
ORDER BY
    valor_vendas DESC;
