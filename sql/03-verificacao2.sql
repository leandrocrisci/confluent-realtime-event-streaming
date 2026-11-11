SHOW wal_level;                                   -- esperado: logical
[{
  "wal_level": "logical"
}]


SELECT c.relname,
       CASE c.relreplident WHEN 'f' THEN 'FULL' WHEN 'd' THEN 'DEFAULT' ELSE c.relreplident::text END AS replica_identity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY 1;                                       -- esperado: FULL nas 5 tabelas
[{
  "relname": "accounts",
  "replica_identity": "FULL"
}, {
  "relname": "cards",
  "replica_identity": "FULL"
}, {
  "relname": "customers",
  "replica_identity": "FULL"
}, {
  "relname": "merchants",
  "replica_identity": "FULL"
}, {
  "relname": "transactions",
  "replica_identity": "FULL"
}]




SELECT pubname, schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'desafio_pub'
ORDER BY tablename;                               -- esperado: 5 linhas
[{
  "pubname": "desafio_pub",
  "schemaname": "public",
  "tablename": "accounts"
}, {
  "pubname": "desafio_pub",
  "schemaname": "public",
  "tablename": "cards"
}, {
  "pubname": "desafio_pub",
  "schemaname": "public",
  "tablename": "customers"
}, {
  "pubname": "desafio_pub",
  "schemaname": "public",
  "tablename": "merchants"
}, {
  "pubname": "desafio_pub",
  "schemaname": "public",
  "tablename": "transactions"
}]



SELECT rolname, rolreplication, rolcanlogin
FROM pg_roles WHERE rolname = 'cdc_user';         -- esperado: rolreplication = t
[{
  "rolname": "cdc_user",
  "rolreplication": true,
  "rolcanlogin": true
}]




-- depois que o conector estiver rodando:
SELECT slot_name, plugin, active, restart_lsn
FROM pg_replication_slots;                        -- esperado: slot do conector, active = t

[{
  "slot_name": "wal_proposer_slot",
  "plugin": null,
  "active": true,
  "restart_lsn": "0/1E6C250"
}, {
  "slot_name": "wal_retention_slot",
  "plugin": null,
  "active": false,
  "restart_lsn": "0/1E5A5D8"
}, {
  "slot_name": "desafio_slot",
  "plugin": "pgoutput",
  "active": true,
  "restart_lsn": "0/1E61420"
}]
