# Sistema de Recomendação de Compras - Infraestrutura

Este projeto configura a infraestrutura Docker para um sistema de recomendação de compras que integra quatro bases de dados diferentes.

## Bancos de Dados

- **PostgreSQL**: Dados relacionais (clientes, produtos, compras)
- **MongoDB**: Interesses dos clientes (orientado a documentos)
- **Neo4j**: Rede de amigos (orientado a grafos)
- **Redis**: Cache e consolidação (chave-valor)

## Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.8+ instalado


## Como Usar

### 1. Subir os serviços Docker

```bash
docker-compose up -d
```

Isso irá iniciar os 4 bancos de dados em containers separados.

### 2. Criar e ativar ambiente virtual (recomendado)

Para evitar conflitos com o ambiente Python do sistema, use um ambiente virtual:

Versão linux:
```bash
# Criar ambiente virtual
python3 -m venv venv

# Ativar ambiente virtual
source venv/bin/activate
```

Versão Windows (Powershell):
```powershell
python3 -m venv venv

.\venv\Scripts\Activate.ps1

# Se der erro de política de execução no PowerShell, execute primeiro:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 3. Instalar dependências Python

```bash
pip install -r requirements.txt
```

**Alternativas se não puder usar ambiente virtual:**

Se encontrar erro "externally-managed-environment" e não puder instalar python3-venv, você pode usar:

```bash
pip install --break-system-packages -r requirements.txt
```

**Atenção**: Esta opção pode causar conflitos com pacotes do sistema. Use apenas se necessário.

### 4. Popular os bancos de dados

```bash
python seed_databases.py
```

O script irá:
- Aguardar todos os serviços estarem prontos
- Criar o esquema no PostgreSQL
- Popular todos os bancos com dados fictícios consistentes
- Manter a integridade referencial entre os bancos (mesmo ID/CPF)

### 5. Adicionar mais dados (opcional)

Para adicionar mais 20 registros em cada banco:

```bash
python add_more_data.py
```

Este script:
- Adiciona 20 novos clientes no PostgreSQL
- Adiciona 20 novos produtos no PostgreSQL
- Adiciona compras para os novos clientes
- Adiciona 20 novos documentos no MongoDB
- Adiciona 20 novas pessoas no Neo4j com relacionamentos
- NÃO remove dados existentes

**Importante**: Após executar este script, execute a sincronização na API (`POST /api/sync_data`) para atualizar o Redis com os novos dados.

### 6. Demonstração de Atualização (Para Apresentação)

Dois scripts disponíveis para demonstrar o fluxo de atualização:

#### Opção 1: Demonstração Interativa

Script passo-a-passo com pausas para explicação:

```bash
# Certifique-se de que a API está rodando primeiro
python app.py

# Em outro terminal, execute a demonstração interativa
python demo_atualizacao.py
```

Este script demonstra:
- Alteração de dados nos bancos originais (PostgreSQL, MongoDB, Neo4j)
- Verificação de que o Redis está desatualizado após mudanças
- **A sincronização é feita pelo front-end** usando o botão "Sincronizar/Atualizar Bases"
- Verificação de que o Redis foi atualizado (via front-end)
- Consulta dos dados atualizados no front-end

**Fluxo da demonstração:**
1. Altera nome, cidade e email de um cliente no PostgreSQL
2. Adiciona uma nova compra para o cliente
3. Atualiza interesses no MongoDB
4. Adiciona novos relacionamentos de amizade no Neo4j
5. Mostra que o Redis ainda tem dados antigos
6. No front-end, clique em **"Sincronizar/Atualizar Bases"** para limpar/recriar dados no Redis
7. Verifique no front-end que os dados foram atualizados

### 8. Executar a API REST

A API FastAPI consolida dados dos bancos e serve o front-end:

```bash
# Com ambiente virtual ativado
python app.py

# Ou usando uvicorn diretamente
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

A API estará disponível em:
- **Front-end**: http://localhost:8000
- **API Docs (Swagger)**: http://localhost:8000/docs
- **API Docs (ReDoc)**: http://localhost:8000/redoc

**Rotas da API:**
- `POST /api/sync_data` - Sincroniza e consolida dados no Redis
- `GET /api/clientes` - Lista todos os clientes do Redis
- `GET /api/clientes/amigos` - Clientes e seus amigos do Redis
- `GET /api/clientes/compras` - Clientes e compras do Redis
- `GET /api/recomendacoes` - Recomendações personalizadas do Redis

**Fluxo de uso:**
1. Acesse http://localhost:8000 no navegador
2. Clique em "Sincronizar/Atualizar Bases" para consolidar dados no Redis
3. Navegue pelas abas para visualizar os dados consolidados

**Para demonstração na apresentação:**
1. Execute `python demo_atualizacao.py` em um terminal
2. Acompanhe o script alterando dados e sincronizando via API
3. Mostre no front-end que os dados foram atualizados

## Estrutura dos Dados

### PostgreSQL

- **clientes**: id, cpf, nome, endereco, cidade, uf, email
- **produtos**: id, produto, valor, quantidade, tipo
- **compras**: id, id_produto (FK), data, id_cliente (FK)

### MongoDB

- **coleção**: `clientes_interesses`
- **documentos**: id_cliente, cpf, nome, interesses (lista), data_atualizacao

### Neo4j

- **nós**: Pessoa {id, cpf, nome}
- **relacionamentos**: AMIGO_DE (bidirecional)

### Redis

- **Chave**: `cliente:{id}` (ex: `cliente:1`)
- **Valor**: JSON consolidado contendo:
  - `dados_pessoais`: Dados do PostgreSQL
  - `compras`: Lista de compras do PostgreSQL
  - `interesses`: Lista de interesses do MongoDB
  - `amigos`: Lista de amigos do Neo4j
  - `recomendacoes`: Recomendações baseadas em compras dos amigos
  - `ultima_atualizacao`: Timestamp da última sincronização

## Arquitetura do Sistema

### Fluxo de Dados

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ PostgreSQL  │     │  MongoDB    │     │    Neo4j    │
│ (Relacional)│     │ (Documentos)│     │   (Grafos)  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │  API FastAPI│
                    │  (ETL/Sync) │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    Redis    │
                    │(Consolidado)│
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Front-end  │
                    │  (Leitura)  │
                    └─────────────┘
```

### Regras de Negócio

1. **API como Centralizador**: A API FastAPI é responsável por:
   - Ler dados de PostgreSQL, MongoDB e Neo4j
   - Consolidar (merge) os dados por cliente
   - Gerar recomendações baseadas em compras dos amigos
   - Armazenar dados consolidados no Redis

2. **Leitura do Front-end**: 
   - O front-end e todas as rotas de consulta (`GET /api/*`) leem **exclusivamente do Redis**
   - Não acessam diretamente os bancos originais

3. **Atualização**:
   - A rota `POST /api/sync_data` limpa o Redis e recria todos os dados consolidados
   - Deve ser executada sempre que houver mudanças nos bancos originais

4. **Recomendações**:
   - Baseadas nas compras dos amigos do cliente
   - Lógica: "Seu amigo comprou X, talvez você goste"
   - Apenas produtos que o cliente ainda não comprou

## Parar os serviços

```bash
docker-compose down
```

Para remover também os volumes (dados):

```bash
docker-compose down -v
```

## Portas dos Serviços

- PostgreSQL: `5432`
- MongoDB: `27017`
- Neo4j HTTP: `7474`, Bolt: `7687`
- Redis: `6379`

## Credenciais

- **PostgreSQL**: user=`postgres`, password=`postgres`, database=`recomendacao_db`
- **MongoDB**: user=`admin`, password=`admin123`
- **Neo4j**: user=`neo4j`, password=`neo4j123`
- **Redis**: sem autenticação (apenas localhost)
