-- Gera os três tipos de evento do CDC (rode com o conector em RUNNING)

-- op=c (INSERT)
INSERT INTO merchants (name, category) VALUES ('Loja Teste CDC', 'ONLINE') RETURNING id;

-- op=u (UPDATE): before e after preenchidos (REPLICA IDENTITY FULL)
UPDATE accounts SET balance = balance - 10 WHERE id = 1 RETURNING id, balance;

-- op=d (DELETE): apaga só a linha criada acima, que não tem dependências
DELETE FROM merchants WHERE name = 'Loja Teste CDC' RETURNING id;
