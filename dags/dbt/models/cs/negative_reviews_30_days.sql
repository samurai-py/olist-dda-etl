-- Reclamações Negativas últimos 30 dias
SELECT COUNT(id_review) AS reclamacoes_negativas
FROM reviews
WHERE pontuacao <= 3
  AND data_criacao <= (
    SELECT DATEADD(DAY, -30, MAX(data_criacao)) AS data_limite
    FROM reviews
  );