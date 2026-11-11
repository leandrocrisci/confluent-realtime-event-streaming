# Etapa 3: Ingestão (CDC) com Neon + Confluent Cloud

Objetivo: o Postgres do Neon alimenta o Kafka por CDC e você enxerga os três tipos de evento (`c`, `u`, `d`).

Arquivos desta pasta, na ordem de uso:

| Arquivo | Para quê | Onde rodar |
|---|---|---|
| `01-schema-seed.sql` | Cria as 5 tabelas e o seed (200/240/320/60/2000) | SQL Editor do Neon |
| `02-cdc-prep.sql` | `REPLICA IDENTITY FULL`, `cdc_user`, publication | SQL Editor do Neon |
| `03-verificacao.sql` | Verifica tudo e gera evidências | SQL Editor do Neon |
| `cdc.json` | Configuração do conector, sem segredos | Repositório (`connectors/cdc.json`) |
| `04-eventos.sql` | Gera insert, update e delete | SQL Editor do Neon |
| `env.exemplo-etapa3` | Variáveis novas do `.env` | Seu `.env` |

Sugestão de lugar no repositório: `seed/01-schema-seed.sql`, `seed/02-cdc-prep.sql`, `seed/03-verificacao.sql`, `seed/04-eventos.sql`, `connectors/cdc.json`.

---

## Passo 1: Projeto no Neon

1. No console do Neon, crie um projeto. Prefira a região **AWS us-east-1**, a mesma do cluster Confluent, para reduzir a latência.
2. Habilite a replicação lógica: **Settings → Logical Replication → Enable**. O Neon reinicia o compute. Depois de ativada, não dá para desativar pelo console.
3. Em **Connection Details**, desligue a opção *Connection pooling* e copie o **host direto** (não tem `-pooler` no nome). O pooler não suporta replicação.
4. Guarde: host, nome do banco (`neondb`) e o usuário dono (`neondb_owner`).

## Passo 2: Esquema e seed

No **SQL Editor** do Neon, cole e rode `01-schema-seed.sql`. A última consulta deve devolver:

| customers | accounts | cards | merchants | transactions |
|---|---|---|---|---|
| 200 | 240 | 320 | 60 | 2000 |

O script usa `setseed(0.42)` dentro de uma transação, para a mesma sessão valer do início ao fim. Rodar de novo recria as tabelas com os mesmos valores.

## Passo 3: Preparar o banco para o CDC

1. Abra `02-cdc-prep.sql` e **troque a senha** do `cdc_user`.
2. Rode no SQL Editor.
3. Se o Neon recusar o `REPLICATION` no `CREATE ROLE`, crie o role `cdc_user` pelo console (**Roles → New role**) e rode só os `GRANT`s e a `CREATE PUBLICATION`.

## Passo 4: Verificar (evidência)

Rode `03-verificacao.sql` e guarde as saídas em `evidencias/03-verificacao-neon.txt`. Esperado:

- `wal_level` = `logical`
- `FULL` nas 5 tabelas
- 5 linhas em `pg_publication_tables`
- `rolreplication = t` para o `cdc_user`

## Passo 5: Variáveis e conector

No `.env` (e só os nomes, sem valores, no `.env.example`):

```
NEON_HOST=<host direto, sem -pooler>
NEON_DB=neondb
CDC_USER=cdc_user
CDC_PASSWORD=<a senha escolhida>
SA_PRODUCER_ID=sa-z3ndxk3
```

Confira se a identidade do conector (producer) tem as permissões necessárias, como na Etapa 1: `WRITE`, `DESCRIBE` e `CREATE` nos tópicos com prefixo `desafio` e `__debezium-heartbeat`.

```bash
confluent kafka acl list --service-account sa-z3ndxk3
```

Valide os nomes das propriedades na sua versão antes de criar:

```bash
confluent connect plugin describe PostgresCdcSourceV2
```

Crie o conector:

```bash
set -a; source .env; set +a
envsubst < connectors/cdc.json > /tmp/cdc.json
confluent connect cluster create --config-file /tmp/cdc.json
rm /tmp/cdc.json                       # tem a senha, não deixe no disco
confluent connect cluster list         # aguarde RUNNING
```

Se ficar em `FAILED`, veja o motivo:

```bash
confluent connect cluster describe <connector-id>
```

Causas comuns: host com `-pooler`, senha errada, replicação lógica não habilitada, publication inexistente, ACL faltando.

## Passo 6: Conferir os tópicos e o snapshot

```bash
confluent kafka topic list
```

Devem aparecer `desafio.public.customers`, `.accounts`, `.cards`, `.merchants`, `.transactions` e o de heartbeat. O tópico `desafio` que você usou nos testes não é do CDC.

Rode a última consulta do `03-verificacao.sql` para ver o slot (`active = t`).

## Passo 7: Os três eventos

1. No SQL Editor, rode `04-eventos.sql`.
2. Leia o tópico com a chave do **consumer**:

```bash
confluent kafka topic consume desafio.public.merchants --from-beginning \
  --value-format avro \
  --api-key <chave-consumer> --api-secret <secret-consumer> \
  | tee evidencias/01-envelopes-cdc.json
```

Se o consumo reclamar do Schema Registry, informe também `--schema-registry-endpoint`, `--schema-registry-api-key` e `--schema-registry-api-secret`. O visualizador de mensagens do console web também serve para capturar a evidência.

O que procurar:

| `op` | Evento | `before` | `after` |
|---|---|---|---|
| `c` | INSERT | nulo | linha nova |
| `u` | UPDATE | linha antiga | linha nova |
| `d` | DELETE | linha apagada | nulo, seguido do tombstone |

---

## Cuidados com custo e com o Neon

- **Conector ligado = compute do Neon acordado.** A conexão de replicação e o heartbeat impedem o Neon de suspender, e isso consome as horas do plano gratuito. Pare o conector quando não estiver estudando.
- **Slot parado acumula WAL.** Se apagar o conector, apague também o slot:

```sql
SELECT pg_drop_replication_slot('desafio_slot');
```

- O conector gerenciado cobra por hora. Derrube no fim de cada sessão: `confluent connect cluster delete <connector-id>`.

## Para o README

Para cada passo, registre o que faz, por que escolheu e a evidência: a contagem do seed, a saída do `03-verificacao.sql`, o status `RUNNING` do conector e os três envelopes. Explique por que `REPLICA IDENTITY FULL` (para o `before` vir preenchido) e por que um usuário dedicado (menor privilégio).

## Conferência antes da Etapa 4

- [ ] Seed com 200/240/320/60/2000
- [ ] `wal_level = logical`, `FULL` nas 5 tabelas, publication com 5 tabelas
- [ ] Conector `RUNNING`, sem senha no repositório
- [ ] Tópicos `desafio.public.*` criados
- [ ] Envelopes `c`, `u` e `d` salvos em `evidencias/`
