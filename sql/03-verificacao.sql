-- Verificações da Etapa 3 (cole a saída no README como evidência)

SHOW wal_level;                                   -- esperado: logical

SELECT c.relname,
       CASE c.relreplident WHEN 'f' THEN 'FULL' WHEN 'd' THEN 'DEFAULT' ELSE c.relreplident::text END AS replica_identity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY 1;                                       -- esperado: FULL nas 5 tabelas

SELECT pubname, schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'desafio_pub'
ORDER BY tablename;                               -- esperado: 5 linhas

SELECT rolname, rolreplication, rolcanlogin
FROM pg_roles WHERE rolname = 'cdc_user';         -- esperado: rolreplication = t

-- depois que o conector estiver rodando:
SELECT slot_name, plugin, active, restart_lsn
FROM pg_replication_slots;                        -- esperado: slot do conector, active = t
