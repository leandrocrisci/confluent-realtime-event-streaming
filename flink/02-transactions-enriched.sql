-- Enriquece cada transação nova com os dados da conta.
-- O filtro id > 2000 deixa de fora o seed (2000 transações), que chegou ao Kafka
-- antes do accounts_pk existir e geraria alertas falsos.
-- Se a tabela já existir de uma execução anterior: DROP TABLE `transactions_enriched`;
CREATE TABLE `transactions_enriched` AS
SELECT t.`id`, t.`card_id`, t.`account_id`, t.`merchant_id`,
       t.`amount`, t.`status`,
       a.`customer_id`, a.`status` AS account_status, a.`currency`
FROM `desafio.public.transactions` AS t
LEFT JOIN `accounts_pk` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
  ON t.`account_id` = a.`id`
WHERE t.`id` > 2000;
