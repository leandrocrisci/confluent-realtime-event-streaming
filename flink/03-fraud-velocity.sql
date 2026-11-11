-- RULE_VELOCITY: 3 ou mais transações do mesmo cartão em até 60 segundos.
-- Se a tabela já existir de uma execução anterior: DROP TABLE `desafio.fraud.detected`;
CREATE TABLE `desafio.fraud.detected` AS
SELECT *
FROM `transactions_enriched`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY `$rowtime`
  MEASURES
    FIRST(T.id) AS primeira_tx,
    LAST(T.id) AS ultima_tx,
    SUM(T.amount) AS total,
    LAST(T.account_status) AS account_status,
    LAST(T.currency) AS currency,
    'RULE_VELOCITY' AS regra
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE T AS TRUE
);
