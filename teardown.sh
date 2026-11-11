#!/bin/bash
# Derruba o que gera custo: statements do Flink e o conector CDC.
# Pré-requisito: o mesmo .env usado no setup.sh.
set -e

echo "🛑 Iniciando desmonte controlado do Pipeline de Pagamentos..."

# 1. Carregar as variáveis do .env
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

if [ -z "$CONFLUENT_ENVIRONMENT_ID" ] || [ -z "$CONFLUENT_CLUSTER_ID" ]; then
    echo "❌ Erro: defina CONFLUENT_ENVIRONMENT_ID e CONFLUENT_CLUSTER_ID no .env."
    exit 1
fi

# 2. Contexto da Confluent Cloud
confluent environment use "$CONFLUENT_ENVIRONMENT_ID" &> /dev/null
confluent kafka cluster use "$CONFLUENT_CLUSTER_ID" &> /dev/null

# 3. Excluir TODOS os statements do Flink (qualquer status), para a lista terminar vazia
echo "🧹 Excluindo os statements do Flink..."
STATEMENTS=$(confluent flink statement list --environment "$CONFLUENT_ENVIRONMENT_ID" -o json | jq -r '.[].name') || STATEMENTS=""
for stmt in $STATEMENTS; do
    echo "   - $stmt"
    confluent flink statement delete "$stmt" --environment "$CONFLUENT_ENVIRONMENT_ID" --force
done

# 4. Excluir o conector CDC pelo ID, para interromper a cobrança
echo "🗑 Removendo o conector gerenciado 'desafio-cdc'..."
CONNECTOR_ID=$(confluent connect cluster list -o json | jq -r '.[] | select(.name=="desafio-cdc") | .id') || CONNECTOR_ID=""
if [ -n "$CONNECTOR_ID" ] && [ "$CONNECTOR_ID" != "null" ]; then
    echo "   - $CONNECTOR_ID"
    confluent connect cluster delete "$CONNECTOR_ID" --force
else
    echo "⚠️ Conector 'desafio-cdc' não encontrado (já removido?)."
fi

# 5. Validar que as listas ficaram vazias
echo "📋 Conectores ativos:"
confluent connect cluster list
echo "📋 Statements do Flink:"
confluent flink statement list --environment "$CONFLUENT_ENVIRONMENT_ID"

echo "--------------------------------------------------------"
echo "⚠️  ATENÇÃO — PRÓXIMO PASSO OBRIGATÓRIO NO NEON:"
echo "O conector foi removido, mas o slot de replicação ainda retém dados no banco."
echo "Para evitar que o Neon acumule logs WAL, rode no SQL Editor:"
echo "   SELECT pg_drop_replication_slot('desafio_slot');"
echo "--------------------------------------------------------"
echo "💀 Teardown concluído."
