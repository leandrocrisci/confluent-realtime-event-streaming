-- Etapa 3: esquema + seed determinístico no Neon (200 / 240 / 320 / 60 / 2000)
-- Rode no SQL Editor do Neon ou com psql. Pode ser repetido: recria as tabelas.
-- ATENÇÃO: se o conector já estiver rodando, pare-o antes de recriar as tabelas.
BEGIN;  -- setseed vale por sessão; a transação garante que tudo roda na mesma

DROP TABLE IF EXISTS transactions, cards, accounts, merchants, customers CASCADE;

CREATE TABLE customers (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL,
  email       TEXT NOT NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
  id           SERIAL PRIMARY KEY,
  customer_id  INT NOT NULL REFERENCES customers(id),
  balance      NUMERIC(15,2) NOT NULL,
  status       TEXT NOT NULL,
  currency     TEXT NOT NULL
);

CREATE TABLE cards (
  id          SERIAL PRIMARY KEY,
  account_id  INT NOT NULL REFERENCES accounts(id),
  last4       TEXT NOT NULL,
  brand       TEXT NOT NULL
);

CREATE TABLE merchants (
  id        SERIAL PRIMARY KEY,
  name      TEXT NOT NULL,
  category  TEXT NOT NULL
);

CREATE TABLE transactions (
  id           SERIAL PRIMARY KEY,
  card_id      INT NOT NULL REFERENCES cards(id),
  account_id   INT NOT NULL REFERENCES accounts(id),
  merchant_id  INT NOT NULL REFERENCES merchants(id),
  amount       NUMERIC(15,2) NOT NULL,
  status       TEXT NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT now()
);

SELECT setseed(0.42);  -- mesma semente = mesmos dados em toda execução

INSERT INTO customers (name, email)
SELECT 'Cliente ' || g, 'cliente' || g || '@exemplo.com'
FROM generate_series(1, 200) g;

INSERT INTO accounts (customer_id, balance, status, currency)
SELECT 1 + ((g - 1) % 200),
       round((random() * 10000)::numeric, 2),
       CASE WHEN random() < 0.95 THEN 'ACTIVE' ELSE 'BLOCKED' END,
       'BRL'
FROM generate_series(1, 240) g;

INSERT INTO cards (account_id, last4, brand)
SELECT 1 + ((g - 1) % 240),
       lpad((floor(random() * 10000))::int::text, 4, '0'),
       (ARRAY['VISA', 'MASTERCARD', 'ELO'])[1 + floor(random() * 3)::int]
FROM generate_series(1, 320) g;

INSERT INTO merchants (name, category)
SELECT 'Loja ' || g,
       (ARRAY['MERCADO', 'RESTAURANTE', 'COMBUSTIVEL', 'FARMACIA', 'ONLINE'])[1 + floor(random() * 5)::int]
FROM generate_series(1, 60) g;

-- a conta do cartão c é 1 + ((c-1) % 240), mesma regra usada em cards
INSERT INTO transactions (card_id, account_id, merchant_id, amount, status)
SELECT c,
       1 + ((c - 1) % 240),
       1 + floor(random() * 60)::int,
       round((5 + random() * 495)::numeric, 2),
       'APPROVED'
FROM (SELECT 1 + floor(random() * 320)::int AS c FROM generate_series(1, 2000)) s;

COMMIT;

SELECT (SELECT count(*) FROM customers)    AS customers,
       (SELECT count(*) FROM accounts)     AS accounts,
       (SELECT count(*) FROM cards)        AS cards,
       (SELECT count(*) FROM merchants)    AS merchants,
       (SELECT count(*) FROM transactions) AS transactions;
