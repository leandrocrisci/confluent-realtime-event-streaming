-- Consultas usadas como evidência.
SELECT COUNT(*) FROM `accounts_pk`;
SELECT * FROM `accounts_pk` WHERE id = 1;
SELECT * FROM `transactions_enriched`;
SELECT * FROM `desafio.fraud.detected`;

-- As transações do cartão 201 (alerta esperado) e do 58 (controle), com o horário de chegada.
SELECT id, card_id, amount, `$rowtime` AS chegada
FROM `transactions_enriched`
WHERE card_id IN (201, 58)
ORDER BY `$rowtime`, card_id;
