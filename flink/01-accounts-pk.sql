-- Cópia de accounts com chave primária, necessária para o join temporal.
-- Se a tabela já existir de uma execução anterior: DROP TABLE `accounts_pk`;
CREATE TABLE `accounts_pk` (
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'kafka.cleanup-policy' = 'compact'
) AS
SELECT `id`, `customer_id`, `balance`, `status`, `currency`
FROM `desafio.public.accounts`;
