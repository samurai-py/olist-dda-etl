-- Pedidos não enviados
select count(id_pedido) as pedido
from orders
where status_pedido = 'aprovado' or status_pedido = 'fatura_emitida'