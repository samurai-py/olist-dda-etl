-- Reclamações negativas por cliente
SELECT c.identificador_cliente as pessoa, count(r.id_review) as reclamacoes FROM customers as c
LEFT JOIN orders as o on o.id_cliente = c.id_cliente
LEFT JOIN reviews as r on r.id_pedido = c.id_cliente
WHERE r.pontuacao <= 3
GROUP BY pessoa
ORDER BY reclamacoes DESC;