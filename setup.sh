#!/bin/bash
# Sobe o conector CDC (Camada 3) a partir de connectors/cdc.json.
# Pré-requisito: .env preenchido a partir do .env.example.
set -e

echo "🚀 Iniciando automação do Pipeline de Pagamentos CDC..."

# 1. Verificar se o .env existe
if [ ! -f .env ]; then
    echo "❌ Erro: arquivo .env não encontrado na raiz do projeto."
    echo "Crie o arquivo a partir do .env.example e preencha as credenciais."
    exit 1
fi

# 2. Verificar as ferramentas usadas pelo script
for cmd in confluent envsubst jq; do
    if ! command -v "$cmd" > /dev/null; then
        echo "❌ Erro: '$cmd' não está instalado."
        exit 1
    fi
done

# 3. Ambiente virtual Python e dependências
echo "📦 Ativando o ambiente virtual Python..."
if [ ! -d venv ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -q --upgrade pip
if [ -f requirements.txt ]; then
    pip install -q -r requirements.txt
else
    pip install -q python-dotenv certifi "confluent-kafka[avro,json,protobuf]"
fi

# 4. Carregar as variáveis do .env
set -a
source .env
set +a

if [ -z "$CONFLUENT_ENVIRONMENT_ID" ] || [ -z "$CONFLUENT_CLUSTER_ID" ]; then
    echo "❌ Erro: defina CONFLUENT_ENVIRONMENT_ID e CONFLUENT_CLUSTER_ID no .env."
    exit 1
fi

# 5. Contexto da Confluent Cloud
echo "☁️ Configurando o contexto na Confluent Cloud..."
confluent environment use "$CONFLUENT_ENVIRONMENT_ID"
confluent kafka cluster use "$CONFLUENT_CLUSTER_ID"

# 6. Criar o conector gerenciado PostgresCdcSourceV2
if [ ! -f connectors/cdc.json ]; then
    echo "❌ Erro: arquivo connectors/cdc.json não encontrado."
    exit 1
fi

echo "🔄 Criando o conector Debezium CDC..."
# O arquivo temporário contém credenciais: permissão restrita (mktemp) e remoção garantida (trap)
TMP_CONFIG=$(mktemp)
trap 'rm -f "$TMP_CONFIG"' EXIT
envsubst < connectors/cdc.json > "$TMP_CONFIG"
confluent connect cluster create --config-file "$TMP_CONFIG"
rm -f "$TMP_CONFIG"
echo "🔒 Arquivo temporário com credenciais removido."

# 7. Aguardar o status RUNNING (até 2 minutos)
echo "⏳ Aguardando o conector ficar RUNNING..."
STATUS=""
for i in $(seq 1 24); do
    STATUS=$(confluent connect cluster list -o json | jq -r '.[] | select(.name=="desafio-cdc") | .status')
    if [ "$STATUS" = "RUNNING" ]; then
        break
    fi
    sleep 5
done

confluent connect cluster list

if [ "$STATUS" != "RUNNING" ]; then
    echo "⚠️ O conector ainda não está RUNNING. Confira o status acima e os logs no console."
    exit 1
fi

echo "✅ setup.sh executado com sucesso!"
echo "Lembrete: as tabelas e a publicação precisam existir no Neon antes deste passo."
