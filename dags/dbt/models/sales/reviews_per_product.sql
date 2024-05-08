--Avaliação média por produto
select pr.id_produto as produto, pr.categoria_produto as categoria, avg(pontuacao) as avaliacao_media
from products as pr
LEFT JOIN order_items oi on oi.id_produto = pr.id_produto
LEFT JOIN orders o on oi.id_pedido = o.id_pedido
LEFT JOIN reviews r on r.id_pedido = o.id_pedido
GROUP BY 1,2
ORDER BY avaliacao_media DESC
LIMIT 10;