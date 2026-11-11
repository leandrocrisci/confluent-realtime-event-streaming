import os
from decimal import Decimal
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# 1. Carrega as variáveis do arquivo .env
load_dotenv()

# 2. Configuração do Schema Registry Client
schema_registry_conf = {
    'url': os.getenv('CONFLUENT_SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('CONFLUENT_SCHEMA_REGISTRY_API_KEY')}:{os.getenv('CONFLUENT_SCHEMA_REGISTRY_API_SECRET')}"
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 3. Buscar e ler o arquivo local do Schema Avro
path_schema = "schemas/transaction.avsc"
with open(path_schema, "r") as f:
    schema_str = f.read()

# 4. Configurar o Serializador Avro baseado no schema lido
avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=schema_str
)

# 5. Configuração do Producer do Kafka
producer_conf = {
    'bootstrap.servers': os.getenv('CONFLUENT_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_API_KEY'),
    'sasl.password': os.getenv('CONFLUENT_API_SECRET')
}
producer = Producer(producer_conf)

# Função de callback para monitorar se a mensagem foi entregue com sucesso
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Falha no envio da mensagem: {err}")
    else:
        print(f"✅ Mensagem enviada para o tópico {msg.topic()} [Partição: {msg.partition()}]")

# 6. Dados de transação alinhados com o esquema transaction.avsc
topico = "desafio"  # Prefixo correto do pipeline de acordo com sua ACL e CDC
chave_mensagem = "tx_1001"

# Os campos numéricos devem ser inteiros e bater com as chaves do .avsc
dados_transacao = {
    "id": 1001,
    "account_id": 238,
    "card_id": 15,
    "merchant_id": 61,
    "amount": Decimal("250.50"),  # Tipagem precisa para campos lógicos decimais
    "transaction_date": "2026-11-06T21:38:00Z"
}

print("🚀 Enviando transação via Avro para o Confluent Cloud...")

# 7. Disparar a mensagem serializada incluindo o SerializationContext obrigatório
producer.produce(
    topic=topico,
    key=StringSerializer('utf_8')(chave_mensagem),
    value=avro_serializer(
        dados_transacao, 
        ctx=SerializationContext(topico, MessageField.VALUE)
    ),
    on_delivery=delivery_report
)

# Garante que todas as mensagens na fila sejam enviadas antes de fechar o script
producer.flush()
