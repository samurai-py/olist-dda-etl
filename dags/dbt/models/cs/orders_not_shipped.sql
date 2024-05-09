-- Pedidos não enviados
-- Arquivo: models/orders_not_shipped.sql

{{ config(
    materialized='table',
    file_format='delta',
    schema='cs'
) }}

SELECT
    COUNT(id_pedido) AS pedido_nao_enviado
FROM
    {{ source('dev', 'orders') }}
WHERE
    status_pedido = 'aprovado' OR status_pedido = 'fatura_emitida';
