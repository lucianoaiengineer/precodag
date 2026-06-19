"""Define o pipeline diário de pricing da ShopBrasil no Apache Airflow.

A DAG consulta a FakeStore API, valida o catálogo, calcula métricas por
categoria com Dynamic Task Mapping e persiste snapshot e histórico no banco
analítico. O módulo também concentra callbacks e funções auxiliares que
dependem do contexto de execução do Airflow.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from metricas import calcular_metricas_categoria
from operators.validar_produtos_operator import ValidarProdutosOperator

logger = logging.getLogger(__name__)

URL_PRODUTOS_FAKESTORE = "https://fakestoreapi.com/products"
CONEXAO_POSTGRES_ANALYTICS = "postgres_analytics"

fuso_horario = pendulum.timezone("America/Sao_Paulo")


def _detalhes_contexto(contexto: dict[str, Any]) -> tuple[str, str, str]:
    """Extrai identificadores úteis do contexto de execução do Airflow.

    Args:
        contexto: Contexto entregue pelo Airflow ao callback da task.

    Returns:
        Tupla com identificador da DAG, identificador da task e data lógica.
        Valores substitutos são usados quando uma informação não está
        disponível no contexto.
    """
    instancia = contexto.get("task_instance")
    dag_id = getattr(instancia, "dag_id", "dag_desconhecida")
    task_id = getattr(instancia, "task_id", "task_desconhecida")
    logical_date = contexto.get("logical_date") or contexto.get("execution_date")
    return dag_id, task_id, str(logical_date)


def ao_falhar(contexto: dict[str, Any]) -> None:
    """Registra um alerta operacional quando a task crítica falha.

    Args:
        contexto: Contexto da task instance fornecido pelo Airflow.

    Returns:
        Nenhum valor. O callback produz somente um registro de erro no log.
    """
    dag_id, task_id, logical_date = _detalhes_contexto(contexto)
    logger.error(
        "ALERTA: falha na task critica. dag=%s task=%s logical_date=%s",
        dag_id,
        task_id,
        logical_date,
    )


def ao_tentar_novamente(contexto: dict[str, Any]) -> None:
    """Registra a entrada da task crítica em uma nova tentativa.

    Args:
        contexto: Contexto da task instance fornecido pelo Airflow.

    Returns:
        Nenhum valor. O callback produz somente um aviso no log.
    """
    dag_id, task_id, logical_date = _detalhes_contexto(contexto)
    logger.warning(
        "Retry acionado para task critica. dag=%s task=%s logical_date=%s",
        dag_id,
        task_id,
        logical_date,
    )


def ao_sucesso(contexto: dict[str, Any]) -> None:
    """Registra a conclusão bem-sucedida da task crítica.

    Args:
        contexto: Contexto da task instance fornecido pelo Airflow.

    Returns:
        Nenhum valor. O callback produz somente um registro informativo.
    """
    dag_id, task_id, logical_date = _detalhes_contexto(contexto)
    logger.info(
        "Task critica concluida com sucesso. dag=%s task=%s logical_date=%s",
        dag_id,
        task_id,
        logical_date,
    )


def _data_referencia_da_execucao() -> str:
    """Obtém a data de referência da janela processada pela DAG.

    A função prioriza o fim do intervalo de dados e usa a data lógica como
    fallback. A conversão para o fuso do projeto mantém a chave idempotente
    alinhada ao calendário de São Paulo.

    Returns:
        Data de referência no formato ISO ``AAAA-MM-DD``.
    """
    contexto = get_current_context()
    referencia_temporal = (
        contexto.get("data_interval_end") or contexto["logical_date"]
    )

    if hasattr(referencia_temporal, "in_timezone"):
        return referencia_temporal.in_timezone(fuso_horario).date().isoformat()

    return (
        pendulum.instance(referencia_temporal, tz=fuso_horario)
        .in_timezone(fuso_horario)
        .date()
        .isoformat()
    )


@task(
    task_id="buscar_produtos",
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    on_failure_callback=ao_falhar,
    on_retry_callback=ao_tentar_novamente,
    on_success_callback=ao_sucesso,
)
def buscar_produtos() -> list[dict[str, Any]]:
    """Busca o catálogo da FakeStore API com proteção contra falhas temporárias.

    Returns:
        Lista de produtos recebida da API e validada como coleção JSON.

    Raises:
        requests.RequestException: Quando ocorre falha de rede ou resposta HTTP
            inválida.
        TypeError: Quando o payload retornado não é uma lista.
        ValueError: Quando a resposta não contém JSON válido.
    """
    # O timeout explícito evita que uma indisponibilidade da fonte externa
    # mantenha a task bloqueada indefinidamente.
    try:
        resposta_api = requests.get(URL_PRODUTOS_FAKESTORE, timeout=20)
        resposta_api.raise_for_status()
        produtos = resposta_api.json()

        # A validação estrutural antecede o operador, que verifica o schema de
        # cada produto individualmente.
        if not isinstance(produtos, list):
            raise TypeError("A FakeStore API retornou payload que nao e lista.")

        logger.info("Busca concluida com %s produtos.", len(produtos))
        return produtos
    except (requests.RequestException, TypeError, ValueError) as erro:
        logger.exception("Falha ao buscar produtos da FakeStore API: %s", erro)
        raise


@task(task_id="listar_categorias")
def listar_categorias(produtos: list[dict[str, Any]]) -> list[str]:
    """Descobre as categorias existentes nos produtos validados.

    Args:
        produtos: Lista de produtos aprovada pelo operador customizado.

    Returns:
        Categorias não vazias, sem duplicidade e ordenadas alfabeticamente.
    """
    categorias = sorted(
        {
            str(produto["category"]).strip()
            for produto in produtos
            if str(produto.get("category", "")).strip()
        }
    )
    logger.info("Categorias identificadas dinamicamente: %s", categorias)
    return categorias


@task(task_id="calcular_metricas_por_categoria", pool="ecommerce_pool")
def calcular_metricas_por_categoria(
    produtos: list[dict[str, Any]],
    categoria: str,
) -> dict[str, float | int | str]:
    """Calcula os indicadores de pricing de uma categoria mapeada.

    Args:
        produtos: Lista completa de produtos validados.
        categoria: Categoria associada à instância criada pelo mapeamento.

    Returns:
        Métricas agregadas de preço e quantidade para a categoria.
    """
    metricas_categoria = calcular_metricas_categoria(produtos, categoria)
    logger.info("Metricas calculadas para categoria %s.", categoria)
    return metricas_categoria


@task(task_id="consolidar_metricas")
def consolidar_metricas(
    lista_metricas: list[dict[str, float | int | str]],
) -> list[dict[str, float | int | str]]:
    """Consolida os resultados produzidos pelo fan-out de categorias.

    Args:
        lista_metricas: Resultados das instâncias mapeadas de cálculo.

    Returns:
        Métricas ordenadas por categoria para persistência determinística.
    """
    metricas_consolidadas = sorted(
        lista_metricas,
        key=lambda metricas_categoria: str(metricas_categoria["categoria"]),
    )
    logger.info(
        "Foram consolidadas metricas de %s categorias.",
        len(metricas_consolidadas),
    )
    return metricas_consolidadas


@task(task_id="salvar_snapshot_postgres")
def salvar_snapshot_postgres(
    metricas_consolidadas: list[dict[str, float | int | str]],
) -> int:
    """Persiste o snapshot idempotente no PostgreSQL analítico.

    Args:
        metricas_consolidadas: Indicadores finais de todas as categorias.

    Returns:
        Quantidade de registros enviados ao banco. Retorna zero quando não há
        métricas disponíveis.

    Raises:
        AirflowNotFoundException: Quando a Connection configurada não existe.
        psycopg2.Error: Quando a conexão ou a operação SQL falha.
    """
    data_referencia = _data_referencia_da_execucao()

    # Constrói uma linha por categoria com a mesma data de referência da DAG
    # para formar a chave idempotente do snapshot.
    parametros = [
        (
            data_referencia,
            metricas_categoria["categoria"],
            metricas_categoria["preco_medio"],
            metricas_categoria["preco_minimo"],
            metricas_categoria["preco_maximo"],
            metricas_categoria["quantidade_produtos"],
        )
        for metricas_categoria in metricas_consolidadas
    ]

    if not parametros:
        logger.warning("Nenhuma metrica disponivel para salvar no snapshot.")
        return 0

    # ON CONFLICT atualiza a chave data_referencia + categoria e impede que um
    # reprocessamento do mesmo período duplique linhas no snapshot.
    comando_sql = """
        INSERT INTO metricas_categoria_snapshot (
            data_referencia,
            categoria,
            preco_medio,
            preco_minimo,
            preco_maximo,
            quantidade_produtos
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (data_referencia, categoria)
        DO UPDATE SET
            preco_medio = EXCLUDED.preco_medio,
            preco_minimo = EXCLUDED.preco_minimo,
            preco_maximo = EXCLUDED.preco_maximo,
            quantidade_produtos = EXCLUDED.quantidade_produtos,
            atualizado_em = CURRENT_TIMESTAMP;
    """

    postgres_hook = PostgresHook(postgres_conn_id=CONEXAO_POSTGRES_ANALYTICS)
    with postgres_hook.get_conn() as conexao:
        with conexao.cursor() as cursor:
            cursor.executemany(comando_sql, parametros)
        conexao.commit()

    logger.info("Snapshot idempotente salvo com %s linhas.", len(parametros))
    return len(parametros)


@task(task_id="salvar_historico_postgres")
def salvar_historico_postgres(
    metricas_consolidadas: list[dict[str, float | int | str]],
) -> int:
    """Acrescenta as métricas ao histórico do PostgreSQL analítico.

    Args:
        metricas_consolidadas: Indicadores finais de todas as categorias.

    Returns:
        Quantidade de registros enviados ao histórico. Retorna zero quando não
        há métricas disponíveis.

    Raises:
        AirflowNotFoundException: Quando a Connection configurada não existe.
        psycopg2.Error: Quando a conexão ou a operação SQL falha.
    """
    data_referencia = _data_referencia_da_execucao()

    # O histórico recebe uma nova linha por categoria em toda execução
    # bem-sucedida, preservando a evolução dos indicadores ao longo do tempo.
    parametros = [
        (
            data_referencia,
            metricas_categoria["categoria"],
            metricas_categoria["preco_medio"],
            metricas_categoria["preco_minimo"],
            metricas_categoria["preco_maximo"],
            metricas_categoria["quantidade_produtos"],
        )
        for metricas_categoria in metricas_consolidadas
    ]

    if not parametros:
        logger.warning("Nenhuma metrica disponivel para salvar no historico.")
        return 0

    comando_sql = """
        INSERT INTO metricas_categoria_historico (
            data_referencia,
            categoria,
            preco_medio,
            preco_minimo,
            preco_maximo,
            quantidade_produtos
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    postgres_hook = PostgresHook(postgres_conn_id=CONEXAO_POSTGRES_ANALYTICS)
    with postgres_hook.get_conn() as conexao:
        with conexao.cursor() as cursor:
            cursor.executemany(comando_sql, parametros)
        conexao.commit()

    logger.info("Historico append salvo com %s linhas.", len(parametros))
    return len(parametros)


@task(task_id="registrar_resumo_execucao")
def registrar_resumo_execucao(
    metricas_consolidadas: list[dict[str, float | int | str]],
    linhas_snapshot: int,
    linhas_historico: int,
) -> dict[str, int]:
    """Registra um resumo final para facilitar a observabilidade da DAG.

    Args:
        metricas_consolidadas: Métricas processadas no fan-in.
        linhas_snapshot: Quantidade de linhas gravadas no snapshot.
        linhas_historico: Quantidade de linhas acrescentadas ao histórico.

    Returns:
        Contagens consolidadas da execução, também disponibilizadas por XCom.
    """
    resumo_execucao = {
        "categorias_processadas": len(metricas_consolidadas),
        "linhas_snapshot": linhas_snapshot,
        "linhas_historico": linhas_historico,
    }
    logger.info("Resumo da execucao: %s", resumo_execucao)
    return resumo_execucao


@dag(
    dag_id="precodag_pricing_diario",
    description=(
        "Pipeline diario de pricing da ShopBrasil com ingestao da FakeStore "
        "API, calculo de metricas por categoria e persistencia idempotente "
        "no PostgreSQL."
    ),
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=fuso_horario),
    catchup=False,
    default_args={"owner": "shopbrasil-analytics"},
    tags=["shopbrasil", "pricing", "taskflow", "precodag"],
)
def precodag_pricing_diario() -> None:
    """Compõe o workflow diário de pricing da ShopBrasil.

    A composição organiza as tasks em ingestão, análise e persistência. As
    chamadas de funções e XComArgs estabelecem as dependências do fluxo.

    Returns:
        Nenhum valor. A função declara a estrutura da DAG no carregamento do
        módulo pelo Airflow.
    """
    # A ingestão combina a TaskFlow API com o operador customizado, garantindo
    # que apenas produtos com schema válido avancem para a análise.
    with TaskGroup(group_id="grupo_ingestao"):
        produtos = buscar_produtos()
        produtos_validados = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos,
        )

    # O Dynamic Task Mapping cria uma execução por categoria descoberta, sem
    # exigir uma lista hardcoded quando a fonte incluir novas categorias.
    with TaskGroup(group_id="grupo_analise"):
        categorias = listar_categorias(produtos_validados.output)
        metricas_por_categoria = calcular_metricas_por_categoria.partial(
            produtos=produtos_validados.output,
        ).expand(categoria=categorias)
        metricas_consolidadas = consolidar_metricas(metricas_por_categoria)

    # A persistência mantém objetivos distintos: snapshot idempotente para o
    # estado diário e histórico append para rastreabilidade das execuções.
    with TaskGroup(group_id="grupo_persistencia"):
        linhas_snapshot = salvar_snapshot_postgres(metricas_consolidadas)
        linhas_historico = salvar_historico_postgres(metricas_consolidadas)

        # A gravação histórica só ocorre depois da conclusão do snapshot.
        linhas_snapshot >> linhas_historico
        registrar_resumo_execucao(
            metricas_consolidadas,
            linhas_snapshot,
            linhas_historico,
        )


precodag_pricing_diario()
