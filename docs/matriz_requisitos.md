# Matriz de requisitos da Atividade 01

## Identificação acadêmica

| Campo | Identificação |
|---|---|
| Disciplina | Orquestração de Workflows |
| Curso | Engenharia de Inteligência Artificial e MLOps Online |
| Professor | Reinaldo Carlos Mendes |
| Atividade | Atividade 01 — Airflow |
| Projeto | `precodag` |

## Requisitos obrigatórios

| Requisito solicitado | Implementação | Localização | Evidência verificável | Status |
|---|---|---|---|---|
| Projeto dockerizado | Airflow 2.9.3, PostgreSQL 15 e serviços de inicialização definidos no Compose | `docker-compose.yml` | Serviços `postgres`, `airflow-init`, `airflow-webserver` e `airflow-scheduler` | Atendido |
| Fonte FakeStore API | Catálogo obtido do endpoint obrigatório | `dags/precodag_pricing_dag.py` | `URL_PRODUTOS_FAKESTORE = "https://fakestoreapi.com/products"` | Atendido |
| Timeout HTTP explícito | Requisição limitada a 20 segundos | DAG principal, `buscar_produtos` | `requests.get(..., timeout=20)` | Atendido |
| TaskFlow API | DAG e tasks Python declaradas por decorators | DAG principal | `@dag` e `@task` | Atendido |
| Dependências por chamada de funções | Saídas são passadas às etapas consumidoras | DAG principal | Chamadas com XComArgs e composição dos TaskGroups | Atendido |
| XCom automático via return | Funções retornam listas, métricas e contagens | DAG principal | `return produtos`, `return categorias`, `return metricas_categoria` | Atendido |
| XCom para dados pequenos | Payload didático trafega via retorno automático | `README.md`, `docs/arquitetura.md` | Seções `TaskFlow API` e `XComs` | Atendido |
| Topologia linear | Ingestão, validação e descoberta executadas em sequência | DAG principal | `buscar_produtos -> validar_produtos -> listar_categorias` | Atendido |
| Topologia fan-out | Uma task de cálculo por categoria | DAG principal | `.expand(categoria=categorias)` | Atendido |
| Topologia fan-in | Resultados mapeados reunidos antes da persistência | DAG principal | `consolidar_metricas(metricas_por_categoria)` | Atendido |
| Agenda diária às 06:00 | Expressão cron diária | Configuração de `@dag` | `schedule="0 6 * * *"` | Atendido |
| Timezone America/Sao_Paulo | Fuso criado com Pendulum | Configuração da DAG | `pendulum.timezone("America/Sao_Paulo")` | Atendido |
| `start_date` definido | Data inicial com timezone explícito | Configuração da DAG | `pendulum.datetime(2026, 1, 1, tz=fuso_horario)` | Atendido |
| `catchup=False` | Backfill automático desativado | Configuração da DAG | `catchup=False` | Atendido |
| Task crítica Buscar Produtos | Etapa dedicada de ingestão | DAG principal | `task_id="buscar_produtos"` | Atendido |
| Retry | Três novas tentativas configuradas | `buscar_produtos` | `retries=3` | Atendido |
| Exponential backoff | Intervalos de retry crescentes | `buscar_produtos` | `retry_exponential_backoff=True` | Atendido |
| Tratamento de erro | Exceções HTTP e de payload capturadas | `buscar_produtos` | Bloco `try/except` | Atendido |
| `raise` para acionar retry | Erro relançado após registro no log | `buscar_produtos` | `raise` no bloco `except` | Atendido |
| Callback de falha | Falha da task crítica registrada | DAG principal | `on_failure_callback=ao_falhar` | Atendido |
| Callback de retry | Nova tentativa registrada | DAG principal | `on_retry_callback=ao_tentar_novamente` | Atendido |
| Callback de sucesso | Conclusão da busca registrada | DAG principal | `on_success_callback=ao_sucesso` | Atendido |
| Descoberta dinâmica de categorias | Categorias extraídas do payload | `listar_categorias` | Compreensão de conjunto sem lista hardcoded | Atendido |
| Dynamic Task Mapping | Expansão por categorias conhecidas em runtime | DAG principal | `.partial(...).expand(...)` | Atendido |
| Pool com 2 slots | Concorrência das tasks mapeadas limitada | DAG principal, `scripts/criar_pool.sh` | `pool="ecommerce_pool"` e `SLOTS="2"` | Atendido |
| Pelo menos dois TaskGroups | Três grupos funcionais | DAG principal | `grupo_ingestao`, `grupo_analise`, `grupo_persistencia` | Atendido |
| Persistência com PostgresHook | Acesso ao banco por provider do Airflow | Tasks de persistência | `PostgresHook(postgres_conn_id=...)` | Atendido |
| Connection analítica | Credencial lógica separada dos metadados | `scripts/criar_conexao_postgres.sh` | Conn ID `postgres_analytics`, schema `analytics` | Atendido |
| Consistência de escrita | Inserção em lote e confirmação transacional | Tasks de persistência | `executemany(...)` e `conexao.commit()` | Atendido |
| Snapshot idempotente | Upsert por data e categoria | DAG principal, `sql/init.sql` | `UNIQUE (data_referencia, categoria)` e `ON CONFLICT DO UPDATE` | Atendido |
| Tabela histórica | Dados preservados por execução | DAG principal, `sql/init.sql` | `metricas_categoria_historico` com `INSERT` append | Atendido |
| Banco de metadados separado logicamente | Metadados e métricas usam bancos distintos | `docker-compose.yml`, `sql/init.sql` | Bancos `airflow` e `analytics` | Atendido |
| Documentação de execução | Procedimento completo via Docker Compose | `README.md` | Seções de execução, validação, consulta e parada | Atendido |
| Estrutura preparada para versionamento | Segredos locais, logs e caches ignorados | `.gitignore`, `.env.example` | `.env`, `logs/`, caches e `.venv` ignorados | Atendido |

## Requisitos opcionais implementados

| Requisito opcional | Implementação | Localização | Evidência verificável | Status |
|---|---|---|---|---|
| Operador customizado | Validação do schema mínimo antes da análise | `plugins/operators/validar_produtos_operator.py` | `class ValidarProdutosOperator(BaseOperator)` | Atendido |
| Validação de produto | Confere `id`, `title`, `price` e `category` | Operador customizado | `_validar_produto(...)` | Atendido |
| Tabela histórica | Registro append para análise de evolução | `sql/init.sql`, DAG principal | `metricas_categoria_historico` | Atendido |
| Alerta por callback | Eventos operacionais simulados por logs | DAG principal | `logger.error`, `logger.warning` e `logger.info` nos callbacks | Atendido |

## Evidências de execução validadas

| Verificação | Resultado observado | Status |
|---|---|---|
| Saúde da infraestrutura | Scheduler, Webserver e PostgreSQL em estado `healthy` | Validado |
| Importação da DAG | `airflow dags list-import-errors` retornou `No data found` | Validado |
| Registro da DAG | `precodag_pricing_diario` apareceu no Airflow | Validado |
| Pool | `ecommerce_pool` com 2 slots | Validado |
| Connection | `postgres_analytics`, tipo `postgres`, host `postgres`, schema `analytics`, porta `5432` | Validado |
| Tabelas analíticas | Snapshot e histórico existentes | Validado |
| Execução ponta a ponta | DAG e todas as tasks em estado `success` | Validado |
| Mapeamento dinâmico | 4 categorias processadas | Validado |
| Primeira execução | Snapshot com 4 linhas e histórico com 4 linhas | Validado |
| Segunda execução | Snapshot com 4 linhas e histórico com 8 linhas | Validado |
| Duplicidade no snapshot | Consulta retornou `0 rows` | Validado |

## Conclusão da matriz

Todos os requisitos obrigatórios da Atividade 01 estão associados a uma
implementação e a uma evidência verificável no projeto. Os opcionais declarados
correspondem somente às funcionalidades existentes: operador customizado,
tabela histórica e callbacks simulados por log.
