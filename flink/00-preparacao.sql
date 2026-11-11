-- Executar no shell do Flink:
--   confluent flink shell --compute-pool <ID_DO_POOL> --environment <ID_DO_AMBIENTE>
-- Executar um statement de cada vez, na ordem dos arquivos 00 a 03.

-- A tabela de transações só recebe inserções; o modo append libera $rowtime e MATCH_RECOGNIZE.
ALTER TABLE `desafio.public.transactions` SET ('changelog.mode' = 'append');

-- Mantém a marca de tempo avançando quando uma partição fica sem dados.
SET 'sql.tables.scan.idle-timeout' = '5s';
