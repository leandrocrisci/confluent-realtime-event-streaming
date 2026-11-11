-- Etapa 3: preparar o banco para o CDC.
-- Pré-requisito: replicação lógica habilitada no projeto Neon (ver o guia).
-- Rode como o dono do banco (ex.: neondb_owner).

-- before preenchido em UPDATE e DELETE
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE cards        REPLICA IDENTITY FULL;
ALTER TABLE merchants    REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- usuário dedicado, com o mínimo de privilégios.
-- Defina a senha ao rodar este script no banco; nunca grave a senha real no repositório. Se o Neon recusar REPLICATION via SQL, crie o role pelo
-- console (Roles) e depois rode só os GRANTs abaixo.
CREATE ROLE cdc_user WITH LOGIN REPLICATION PASSWORD '<defina-uma-senha-forte-aqui-e-nao-versione>';
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;

-- publication criada pelo dono; o conector só consome (sem criar nada)
CREATE PUBLICATION desafio_pub
  FOR TABLE customers, accounts, cards, merchants, transactions;
