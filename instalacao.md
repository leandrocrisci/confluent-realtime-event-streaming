# Guia de Instalação e Configuração - Confluent Platform no Linux (WSL)

Este documento reúne todas as instruções de instalação, configuração de ambiente, reset de credenciais e resolução de problemas para a execução local da Confluent Platform 8.4.0 no ambiente Linux/WSL.

---

## 1. Configuração do Ambiente Linux (Ubuntu/Debian)

### Atualização do Sistema
Sempre atualize a lista de pacotes do repositório antes de iniciar qualquer instalação:
```bash
sudo apt update
```

### Instalação do Java (OpenJDK 21 LTS)
A Confluent Platform 8.0+ não suporta mais o Java 8. Recomenda-se a instalação do Java 21:
```bash
sudo apt install openjdk-21-jdk -y
```

Para verificar se o Java foi instalado corretamente e conferir a versão ativa:
```bash
java -version
```

---

## 2. Gerenciamento de Credenciais (Reset de Senha do Usuário)

Caso esqueça a senha definida no ambiente Linux (WSL), execute o procedimento de recuperação através do **PowerShell do Windows (como Administrador)**:

1. Feche o terminal do Ubuntu.
2. Identifique o nome correto da sua distribuição rodando:
   ```powershell
   wsl -l -v
   ```
3. Faça login diretamente como superusuário (`root`), que não exige senha:
   ```powershell
   wsl -d Ubuntu -u root
   ```
4. Dentro do ambiente root do Linux, altere a senha do seu usuário principal (`leandrocrisci`):
   ```bash
   passwd leandrocrisci
   ```
5. Insira e confirme a nova senha (os caracteres não serão exibidos na tela por segurança).
6. Saia do modo root:
   ```bash
   exit
   ```

---

## 3. Variáveis de Ambiente (`.bashrc`)

Para que o Confluent CLI funcione perfeitamente, os caminhos da plataforma e do Java precisam estar mapeados.

1. Localize o caminho de instalação do seu Java rodando:
   ```bash
   update-java-alternatives -l
   ```
2. Abra o arquivo de perfil do terminal:
   ```bash
   nano ~/.bashrc
   ```
3. Adicione as seguintes linhas ao final do arquivo, ajustando os caminhos conforme sua estrutura de diretórios (garantindo que a pasta da Confluent aponte para a raiz onde os diretórios `/bin` e `/etc` estão localizados):
   ```bash
   # Configuração do Java para o Confluent
   export JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-amd64

   # Configuração do Confluent Home
   export CONFLUENT_HOME=$HOME/confluent
   export PATH=$PATH:$CONFLUENT_HOME/bin
   ```
4. Salve o arquivo (`Ctrl + O`, depois `Enter`) e saia (`Ctrl + X`).
5. Recarregue as configurações do terminal:
   ```bash
   source ~/.bashrc
   ```

---

## 4. Gerenciamento dos Serviços Locais (Confluent CLI)

### Comandos Principais
Os serviços locais destinam-se estritamente a ambientes de desenvolvimento de nó único (single-node).

* **Iniciar todos os serviços:**
  ```bash
  confluent local services start
  ```
* **Verificar o status dos serviços:**
  ```bash
  confluent local services status
  ```
* **Parar todos os serviços:**
  ```bash
  confluent local services stop
  ```
* **Limpar dados e metadados do cluster local:**
  ```bash
  confluent local destroy
  ```

### Atalhos Úteis (Aliases)
Para facilitar a rotina, adicione atalhos ao seu `~/.bashrc`:
```bash
alias ligarkafka="confluent local services start"
alias desligarkafka="confluent local services stop"
```

---

## 5. Resolução de Problemas (Troubleshooting)

### Erro: `Error: set environment variable CONFLUENT_HOME` ou `No such file or directory`
* **Causa:** O CLI não encontra a pasta com os arquivos estruturais da plataforma ou o caminho inserido na variável aponta para uma subpasta incorreta (ex: `confluent-8.4.0` redundante).
* **Solução:** Certifique-se de mover os arquivos da subpasta gerada na extração para a raiz do diretório configurado, mantendo a pasta `/etc` e `/bin` diretamente abaixo de `$HOME/confluent`.

### Erro: `Connect failed to start` (Falso Alarme / Timeout)
* **Causa:** O Kafka Connect é um serviço pesado e pode estourar o tempo limite de resposta que o Confluent CLI aguarda nativamente no WSL, embora continue inicializando em background.
* **Solução:** Aguarde alguns instantes e verifique o log detalhado do Connect usando:
  ```bash
  confluent local services connect log | tail -n 50
  ```
  Se o log terminar com `Kafka Connect started`, o serviço está operando normalmente. Para forçar a inicialização do restante dos componentes parados pelo falso alarme (como o painel visual), execute:
  ```bash
  confluent local services start control-center
  ```

### Ajuste de Memória RAM para o WSL (Obrigatório para Clusters Completos)
A inicialização simultânea do Kafka, KRaft, Schema Registry e Connect exige recursos de hardware que o Windows pode limitar por padrão.

Se encontrar erros de falta de memória (`java.lang.OutOfMemoryError`), crie um arquivo chamado `.wslconfig` na sua pasta de usuário do Windows (`C:\Users\SeuUsuario\.wslconfig`) com o seguinte conteúdo:
```text
[wsl2]
memory=6GB
processors=4
```
Após salvar, reinicie o subsistema WSL pelo PowerShell do Windows executando `wsl --shutdown`.
