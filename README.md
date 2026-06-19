# precodag — Pipeline de Pricing com Apache Airflow

Pipeline diário de pricing da empresa fictícia ShopBrasil, desenvolvido em
Apache Airflow para substituir uma rotina Python agendada por cron. A solução
busca produtos, valida o payload, calcula métricas por categoria e persiste os
resultados em PostgreSQL com controle de concorrência, observabilidade e
idempotência.

## Identificação acadêmica

| Campo | Identificação |
|---|---|
| Disciplina | Orquestração de Workflows |
| Curso | Engenharia de Inteligência Artificial e MLOps Online |
| Professor | Reinaldo Carlos Mendes |
| Atividade | Atividade 01 — Airflow |
| Projeto | `precodag` |

## Contexto do problema

A ShopBrasil é um marketplace de e-commerce em crescimento. No cenário inicial,
um script Python executado por cron alimentava diariamente um painel utilizado
pelas equipes de pricing e pelos gerentes de categoria.

Essa arquitetura apresentava limitações operacionais:

- falhas da API podiam ocorrer sem acompanhamento adequado;
- reprocessamentos manuais podiam duplicar dados;
- novas categorias exigiam alteração manual do código;
- o processamento não possuía controle explícito de concorrência;
- logs, tentativas e histórico de execução tinham baixa visibilidade;
- a manutenção das dependências do fluxo era difícil.

## Objetivo da solução

O projeto substitui o cron por uma DAG real no Apache Airflow. A DAG
`precodag_pricing_diario` executa diariamente às 06:00 no fuso
`America/Sao_Paulo` e realiza as seguintes etapas:

1. Busca produtos na FakeStore API.
2. Valida o schema mínimo de cada produto.
3. Descobre as categorias presentes no payload.
4. Calcula métricas de pricing em paralelo por categoria.
5. Consolida os resultados mapeados.
6. Atualiza um snapshot idempotente no PostgreSQL analítico.
7. Acrescenta registros a uma tabela histórica.
8. Registra um resumo da execução nos logs do Airflow.

## Fonte de dados

A única fonte externa do pipeline é a
[FakeStore API](https://fakestoreapi.com/docs). O endpoint consumido é:

```text
GET https://fakestoreapi.com/products
```

Exemplo equivalente de consumo:

```python
import requests

response = requests.get("https://fakestoreapi.com/products")
print(response.json())
```

No projeto, a requisição usa timeout explícito de 20 segundos,
`raise_for_status()`, tratamento de exceções e novo `raise`. Esse comportamento
permite que falhas HTTP, de conexão ou de payload acionem a política de retry do
Airflow.

## Arquitetura da solução

| Componente | Responsabilidade |
|---|---|
| FakeStore API | Fornecer o catálogo de produtos usado no cálculo das métricas. |
| Airflow Scheduler | Interpretar a agenda, criar DAG runs, expandir tasks e controlar dependências, retries e pools. |
| Airflow Webserver | Expor a interface para operação, logs e inspeção das DAGs. |
| LocalExecutor | Executar tasks em processos locais coordenados pelo Scheduler. |
| PostgreSQL | Hospedar, em bancos separados, os metadados do Airflow e as tabelas analíticas. |
| DAG `precodag_pricing_diario` | Orquestrar ingestão, análise e persistência. |

O ambiente é definido em `docker-compose.yml` com a imagem
`apache/airflow:2.9.3-python3.11` e PostgreSQL 15. O `airflow-init` aplica as
migrações, cria o usuário acadêmico, configura o pool e registra a Connection.

Detalhes adicionais estão em [`docs/arquitetura.md`](docs/arquitetura.md).

## Diferença entre banco de metadados e banco analítico

Os dois bancos são hospedados pelo mesmo serviço PostgreSQL local, mas possuem
finalidades e nomes distintos:

| Banco | Responsabilidade | Dados armazenados |
|---|---|---|
| `airflow` | Metadata Database do Airflow | DAG runs, task instances, XComs, usuários, pools e connections. |
| `analytics` | Banco analítico da ShopBrasil | Snapshot e histórico das métricas de pricing. |

As tasks de negócio não gravam métricas no banco `airflow`. A persistência usa
a Connection `postgres_analytics`, direcionada ao banco `analytics`.

## Fluxo da DAG

```text
grupo_ingestao
  buscar_produtos
  -> validar_produtos

grupo_analise
  -> listar_categorias
  -> calcular_metricas_por_categoria.expand(...)
  -> consolidar_metricas

grupo_persistencia
  -> salvar_snapshot_postgres
  -> salvar_historico_postgres
  -> registrar_resumo_execucao
```

O agendamento é `0 6 * * *`, com `start_date` explícito, timezone criado por
`pendulum.timezone("America/Sao_Paulo")` e `catchup=False`.

## Topologias implementadas

- **Linear:** busca, validação e descoberta de categorias ocorrem em sequência.
- **Fan-out:** o Dynamic Task Mapping cria uma instância de cálculo para cada
  categoria descoberta.
- **Fan-in:** `consolidar_metricas` reúne a lista de resultados produzida pelas
  instâncias mapeadas antes da persistência.

## TaskFlow API

A DAG utiliza `@dag`, e as tasks Python usam `@task`. As saídas retornadas pelas
funções formam objetos XComArg, estabelecem dependências e são disponibilizadas
automaticamente via XCom.

O operador `ValidarProdutosOperator` recebe a saída da busca e devolve a lista
validada. O uso de XCom é adequado ao volume didático da FakeStore API. Em um
cenário de alto volume, os dados deveriam ser armazenados externamente, e o XCom
transportaria somente referências ou identificadores.

## Dynamic Task Mapping

`listar_categorias` extrai e ordena as categorias encontradas no payload, sem
lista hardcoded. Em seguida, a DAG executa:

```python
calcular_metricas_por_categoria.partial(
    produtos=produtos_validados.output,
).expand(categoria=categorias)
```

O `.expand(...)` cria em tempo de execução uma task para cada categoria. Assim,
novas categorias fornecidas pela API não exigem modificação na DAG.

## TaskGroups

O fluxo possui três grupos:

- `grupo_ingestao`: busca e validação dos produtos;
- `grupo_analise`: descoberta de categorias, fan-out e fan-in;
- `grupo_persistencia`: gravação do snapshot, gravação histórica e resumo.

TaskGroup organiza visual e logicamente a DAG na interface do Airflow. Ele não
altera a semântica das dependências entre as tasks.

## Pool de concorrência

O pool `ecommerce_pool` possui 2 slots. A task
`calcular_metricas_por_categoria` declara `pool="ecommerce_pool"`, limitando a
duas as instâncias mapeadas que podem ocupar o pool simultaneamente. O pool
protege os recursos do ambiente quando a quantidade de categorias cresce.

O script `scripts/criar_pool.sh` usa `airflow pools set`, portanto pode criar ou
atualizar o pool em execuções repetidas.

## Resiliência e callbacks

A task crítica `buscar_produtos` implementa:

- `retries=3`;
- intervalo inicial de 5 minutos;
- `retry_exponential_backoff=True`;
- timeout HTTP de 20 segundos;
- tratamento com `try/except`;
- `raise` após o registro da exceção;
- `on_failure_callback=ao_falhar`;
- `on_retry_callback=ao_tentar_novamente`;
- `on_success_callback=ao_sucesso`.

Os callbacks simulam alertas operacionais por log e incluem DAG, task e data
lógica. O projeto não possui integração real com Slack, e-mail ou outra
plataforma externa de alertas.

## Persistência no PostgreSQL

As tasks de persistência instanciam `PostgresHook` com a Connection
`postgres_analytics`. Cada task obtém uma conexão, executa a operação em lote
com `executemany` e confirma a transação com `commit`.

O snapshot é gravado antes do histórico. O banco analítico e as tabelas são
inicializados por `sql/init.sql` quando o volume PostgreSQL é criado pela
primeira vez.

## Idempotência

A tabela `metricas_categoria_snapshot` possui restrição única composta por
`data_referencia + categoria`. A gravação executa `ON CONFLICT DO UPDATE`, de
modo que reprocessar a mesma data atualiza os valores existentes e não cria uma
segunda linha para a mesma categoria.

A tabela `metricas_categoria_historico` tem finalidade distinta: ela opera em
modo append e registra novas linhas em cada execução bem-sucedida. Portanto, o
snapshot representa o estado consolidado por data, enquanto o histórico
preserva a evolução das execuções.

## Tabelas do projeto

| Tabela | Modo de gravação | Chave ou identificador | Finalidade |
|---|---|---|---|
| `metricas_categoria_snapshot` | Upsert | Única: `data_referencia`, `categoria` | Manter uma linha consolidada por categoria e data. |
| `metricas_categoria_historico` | Append | `id` serial | Registrar cada execução para análise histórica. |

As duas tabelas armazenam preço médio, mínimo, máximo e quantidade de produtos.

## Estrutura de pastas

```text
precodag/
├── dags/
│   ├── __init__.py
│   ├── metricas.py
│   └── precodag_pricing_dag.py
├── docs/
│   ├── arquitetura.md
│   └── matriz_requisitos.md
├── plugins/
│   ├── __init__.py
│   └── operators/
│       ├── __init__.py
│       └── validar_produtos_operator.py
├── scripts/
│   ├── criar_conexao_postgres.sh
│   └── criar_pool.sh
├── sql/
│   └── init.sql
├── tests/
│   ├── __init__.py
│   └── test_metricas.py
├── .env.example
├── .gitignore
├── docker-compose.yml
├── README.md
└── requirements.txt
```

Arquivos locais como `.env`, `.venv`, caches, logs e resultados temporários não
fazem parte da entrega versionável.

## Pré-requisitos

- Docker Engine ou Docker Desktop com integração WSL;
- Docker Compose v2, disponível pelo comando `docker compose`;
- WSL/Ubuntu para executar os comandos indicados;
- portas locais `5432` e `8080` disponíveis.

O Airflow e suas dependências são executados nos containers. Não é necessário
instalar Apache Airflow diretamente no Windows ou no Ubuntu.

## Como executar o projeto

No terminal Ubuntu/WSL:

```bash
cd /mnt/c/Users/Luciano/Desktop/Projetos/precodag
cp .env.example .env
docker compose up airflow-init
docker compose up -d
```

O serviço `airflow-init` deve terminar com código zero após preparar os
metadados, o usuário, o pool e a Connection. Em seguida, valide os serviços de
longa duração:

```bash
docker compose ps
```

Valide também o registro da DAG e eventuais erros de importação:

```bash
docker compose exec -T airflow-webserver airflow dags list
docker compose exec -T airflow-webserver airflow dags list-import-errors
```

O resultado esperado para o segundo comando é `No data found`.

## Como acessar o Airflow

A interface web fica disponível em:

```text
http://localhost:8080
```

Credenciais acadêmicas locais:

- usuário: `airflow`;
- senha: `airflow`.

Esses valores são exclusivamente didáticos e não devem ser usados em produção.

## Como validar pool e connection

Consulte o pool:

```bash
docker compose exec -T airflow-webserver airflow pools list
```

O resultado deve incluir `ecommerce_pool` com 2 slots.

Consulte a Connection:

```bash
docker compose exec -T airflow-webserver airflow connections get postgres_analytics
```

Campos esperados: tipo `postgres`, host `postgres`, schema `analytics` e porta
`5432`. Os scripts de criação são idempotentes e também podem ser executados
novamente:

```bash
docker compose exec -T airflow-webserver bash scripts/criar_pool.sh
docker compose exec -T airflow-webserver bash scripts/criar_conexao_postgres.sh
```

## Como executar a DAG

Na interface do Airflow, localize `precodag_pricing_diario`, remova a pausa e
use o botão de execução manual. Também é possível operar pela CLI:

```bash
docker compose exec -T airflow-webserver airflow dags unpause precodag_pricing_diario
docker compose exec -T airflow-webserver airflow dags trigger precodag_pricing_diario
```

A execução agendada ocorre diariamente às 06:00 em `America/Sao_Paulo`.

## Como consultar os dados

Liste as tabelas do banco analítico:

```bash
docker compose exec -T postgres psql -U airflow -d analytics -c "\dt"
```

Consulte o snapshot:

```bash
docker compose exec -T postgres psql -U airflow -d analytics -c "
SELECT
    data_referencia,
    categoria,
    preco_medio,
    preco_minimo,
    preco_maximo,
    quantidade_produtos
FROM metricas_categoria_snapshot
ORDER BY categoria;
"
```

Conte as linhas do snapshot:

```bash
docker compose exec -T postgres psql -U airflow -d analytics -c "
SELECT COUNT(*) AS total_linhas_snapshot
FROM metricas_categoria_snapshot;
"
```

Conte as linhas do histórico:

```bash
docker compose exec -T postgres psql -U airflow -d analytics -c "
SELECT COUNT(*) AS total_linhas_historico
FROM metricas_categoria_historico;
"
```

## Como validar idempotência

1. Execute a DAG e anote as contagens do snapshot e do histórico.
2. Reexecute a DAG no mesmo dia de referência.
3. Consulte novamente as duas contagens.
4. Verifique duplicidades no snapshot:

```bash
docker compose exec -T postgres psql -U airflow -d analytics -c "
SELECT
    data_referencia,
    categoria,
    COUNT(*) AS ocorrencias
FROM metricas_categoria_snapshot
GROUP BY data_referencia, categoria
HAVING COUNT(*) > 1
ORDER BY data_referencia, categoria;
"
```

O snapshot deve manter a mesma quantidade de linhas para a data reprocessada, o
histórico deve crescer, e a consulta de duplicidade não deve retornar linhas.

## Como parar o ambiente

```bash
docker compose down
```

Esse comando remove os containers e a rede do Compose, preservando o volume com
os bancos locais.

## Evidências de validação

As evidências abaixo registram os resultados observados na validação de ponta a
ponta do projeto.

### Infraestrutura

- `precodag-airflow-scheduler`: `healthy`;
- `precodag-airflow-webserver`: `healthy`;
- `precodag-postgres`: `healthy`.

### DAG e configuração do Airflow

- DAG carregada sem erro de importação;
- `airflow dags list-import-errors` retornou `No data found`;
- DAG `precodag_pricing_diario` listada no Airflow;
- pool `ecommerce_pool` validado com 2 slots;
- Connection `postgres_analytics` validada como tipo `postgres`, host
  `postgres`, schema `analytics` e porta `5432`.

### Banco analítico

- tabelas `metricas_categoria_snapshot` e
  `metricas_categoria_historico` criadas;
- DAG executada com estado `success`;
- todas as tasks concluídas com estado `success`;
- Dynamic Task Mapping processou 4 categorias:
  `electronics`, `jewelery`, `men's clothing` e `women's clothing`;
- após a primeira execução, snapshot e histórico continham 4 linhas cada.

### Evidência de idempotência

Antes da segunda execução:

```text
snapshot  = 4 linhas
historico = 4 linhas
```

Depois da segunda execução:

```text
snapshot  = 4 linhas
historico = 8 linhas
```

A consulta de duplicidade no snapshot retornou `0 rows`. O resultado comprova o
upsert idempotente do snapshot e o comportamento append do histórico.

## Checklist de aderência à Atividade 01

| Requisito solicitado | Entrega realizada | Evidência no projeto | Status |
|---|---|---|---|
| Projeto dockerizado | Ambiente com Airflow e PostgreSQL via Docker Compose | `docker-compose.yml` | Atendido |
| TaskFlow API | DAG criada com `@dag` e tasks com `@task` | `dags/precodag_pricing_dag.py` | Atendido |
| Dependências por chamada de funções | Fluxo definido pela composição das tasks e XComArgs | DAG principal | Atendido |
| XCom automático via return | Tasks retornam dados pequenos entre etapas | DAG principal | Atendido |
| Topologia linear | Busca, validação e listagem de categorias em sequência | `grupo_ingestao` e `grupo_analise` | Atendido |
| Fan-out | Cálculo por categoria com Dynamic Task Mapping | `.expand(...)` | Atendido |
| Fan-in | Consolidação das métricas antes da persistência | `consolidar_metricas` | Atendido |
| Agendamento diário às 06:00 | DAG agendada com cron `0 6 * * *` | configuração da DAG | Atendido |
| Timezone America/Sao_Paulo | Uso de `pendulum.timezone("America/Sao_Paulo")` | configuração da DAG | Atendido |
| `catchup=False` | Backfill automático desativado | configuração da DAG | Atendido |
| Retry na task Buscar Produtos | Task crítica configurada com retries | `buscar_produtos` | Atendido |
| Exponential backoff | Retry com backoff exponencial | `retry_exponential_backoff=True` | Atendido |
| Try/except + raise | Erros da API tratados e relançados | `buscar_produtos` | Atendido |
| Callbacks de ciclo de vida | Falha, retry e sucesso registrados por callbacks | `ao_falhar`, `ao_tentar_novamente`, `ao_sucesso` | Atendido |
| Dynamic Task Mapping | Categorias processadas dinamicamente | `calcular_metricas_por_categoria.expand(...)` | Atendido |
| Pool com 2 slots | Pool `ecommerce_pool` criado com 2 slots | `scripts/criar_pool.sh` | Atendido |
| TaskGroups | Tasks agrupadas por ingestão, análise e persistência | `grupo_ingestao`, `grupo_analise`, `grupo_persistencia` | Atendido |
| PostgresHook | Persistência usando hook do Airflow | tasks de persistência | Atendido |
| Connection do Airflow | Connection `postgres_analytics` | `scripts/criar_conexao_postgres.sh` | Atendido |
| Idempotência | Snapshot com chave única e `ON CONFLICT DO UPDATE` | `sql/init.sql` e task de snapshot | Atendido |
| Tabela histórica append | Histórico registra cada execução | `metricas_categoria_historico` | Atendido |
| Operador customizado | Validação com `ValidarProdutosOperator` | `plugins/operators/validar_produtos_operator.py` | Atendido |
| Documentação | README, arquitetura e matriz de requisitos | `README.md`, `docs/` | Atendido |
| Entrega via repositório | Projeto preparado para GitHub/GitLab | estrutura versionável | Atendido |

A matriz detalhada está em
[`docs/matriz_requisitos.md`](docs/matriz_requisitos.md).

## Considerações finais

O `precodag` transforma a rotina de pricing da ShopBrasil em um workflow
declarativo, rastreável e reprocessável. A solução combina TaskFlow API,
topologias linear, fan-out e fan-in, Dynamic Task Mapping, pool de concorrência,
callbacks, retries com backoff e persistência transacional. O snapshot
idempotente elimina duplicidades por data e categoria, enquanto a tabela
histórica preserva a evolução das execuções.
