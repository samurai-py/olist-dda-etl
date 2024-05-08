--Clientes por Estado
-- Arquivo: models/clients_per_state.sql

{{ config(
    materialized='table',
    file_format='delta'
) }}

SELECT
    UF,
    COUNT(DISTINCT identificador_cliente) AS qtd_clientes
FROM
    {{ source('dev', 'customers') }} AS c
GROUP BY
    UF;
