"""Fornece o operador de validação dos produtos da FakeStore API.

O operador estabelece uma fronteira explícita entre a ingestão externa e as
tasks de análise, impedindo que produtos com schema inválido avancem na DAG.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema mínimo dos produtos antes do processamento analítico.

    O operador recebe por XCom a coleção produzida pela task de busca, valida
    cada item e devolve cópias dos produtos aprovados para as próximas tasks.

    Args:
        produtos: Sequência de produtos recebida da etapa de ingestão.
        **kwargs: Argumentos padrão encaminhados ao ``BaseOperator``.
    """

    template_fields: Sequence[str] = ("produtos",)

    def __init__(
        self,
        *,
        produtos: Sequence[Mapping[str, Any]],
        **kwargs: Any,
    ) -> None:
        """Inicializa o operador com os produtos que serão validados.

        Args:
            produtos: Sequência de mapeamentos recebida da task anterior.
            **kwargs: Configurações do Airflow, como ``task_id`` e dependências.
        """
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        """Valida os produtos e retorna a coleção aprovada via XCom.

        Args:
            context: Contexto de execução injetado pelo Airflow. O nome do
                parâmetro faz parte do contrato de ``BaseOperator.execute``.

        Returns:
            Lista de cópias dos produtos que atenderam ao schema obrigatório.

        Raises:
            TypeError: Quando a coleção não é uma lista ou um item não é um
                mapeamento.
            ValueError: Quando um produto viola uma regra de campo obrigatório.
        """
        if not isinstance(self.produtos, list):
            raise TypeError("Produtos devem ser recebidos como lista.")

        # Valida todos os itens antes de disponibilizar a saída para evitar que
        # um lote parcialmente inválido siga para o cálculo de métricas.
        produtos_validados = []
        for indice, produto in enumerate(self.produtos, start=1):
            self._validar_produto(produto, indice)
            produtos_validados.append(dict(produto))

        self.log.info("%s produtos validados com sucesso.", len(produtos_validados))
        return produtos_validados

    @staticmethod
    def _validar_produto(produto: Mapping[str, Any], indice: int) -> None:
        """Valida os campos obrigatórios de um produto.

        Args:
            produto: Mapeamento que representa um item da FakeStore API.
            indice: Posição humana do produto na lista, usada nas mensagens.

        Raises:
            TypeError: Quando o item não implementa a interface de mapeamento.
            ValueError: Quando ``id``, ``title``, ``price`` ou ``category`` não
                atende ao contrato do pipeline.
        """
        if not isinstance(produto, Mapping):
            raise TypeError(f"Produto na posicao {indice} nao e um objeto valido.")

        # Centraliza a leitura dos campos para produzir mensagens consistentes
        # e preservar o identificador do produto nas falhas de validação.
        identificador = produto.get("id")
        titulo = produto.get("title")
        preco = produto.get("price")
        categoria = produto.get("category")

        if identificador is None:
            raise ValueError(f"Produto na posicao {indice} nao possui id.")

        if not isinstance(titulo, str) or not titulo.strip():
            raise ValueError(
                f"Produto {identificador} deve possuir title textual nao vazio."
            )

        if isinstance(preco, bool) or not isinstance(preco, (int, float)):
            raise ValueError(f"Produto {identificador} deve possuir price numerico.")

        if preco < 0:
            raise ValueError(f"Produto {identificador} possui price negativo.")

        if not isinstance(categoria, str) or not categoria.strip():
            raise ValueError(
                f"Produto {identificador} deve possuir category textual nao vazia."
            )
