
--Vendas por categoria de produto
select pr.categoria_produto as categoria, count(o.id_pedido) as pedidos
from products as pr
LEFT JOIN order_items oi on oi.id_produto = pr.id_produto
LEFT JOIN orders o on oi.id_pedido = o.id_pedido
group by categoria
ORDER BY pedidos DESC;