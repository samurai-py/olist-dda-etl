
--Pedidos por cliente
SELECT c.identificador_cliente as pessoa, count(distinct o.id_pedido) as pedidos FROM customers as c
LEFT JOIN orders as o on o.id_cliente = c.id_cliente
GROUP BY pessoa
ORDER BY pedidos DESC;