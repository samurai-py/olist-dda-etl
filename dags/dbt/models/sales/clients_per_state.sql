--Clientes por Estado
SELECT UF, COUNT(DISTINCT identificador_cliente) as qtd_clientes FROM customers as c
GROUP BY UF
ORDER BY qtd_clientes DESC;