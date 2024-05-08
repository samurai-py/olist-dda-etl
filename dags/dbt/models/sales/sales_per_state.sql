
--Vendas por Estado
SELECT UF, SUM(valor_pagamento) as valor_vendas FROM customers as c
LEFT JOIN orders as o on o.id_cliente = c.id_cliente
LEFT JOIN payments as p on p.id_pedido = o.id_pedido
GROUP BY UF
ORDER BY valor_vendas DESC;