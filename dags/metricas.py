"""Implementa cálculos de pricing independentes do runtime do Airflow.

As funções deste módulo permanecem separadas da definição da DAG para que a
regra de negócio possa ser validada por testes unitários sem inicializar o
Airflow.
"""

from __future__ import annotations

from typing import Any


def calcular_metricas_categoria(
    produtos: list[dict[str, Any]],
    categoria: str,
) -> dict[str, float | int | str]:
    """Calcula as métricas de preço de uma categoria.

    Os produtos recebidos já passaram pelo operador de validação. Quando a
    categoria não possui itens, a função retorna indicadores zerados para
    manter um contrato de saída estável para as tasks consumidoras.

    Args:
        produtos: Lista completa de produtos validados pela DAG.
        categoria: Nome exato da categoria que deve ser processada.

    Returns:
        Dicionário com categoria, preço médio, preço mínimo, preço máximo e
        quantidade de produtos. Os valores monetários usam duas casas
        decimais.
    """
    # O filtro permanece isolado da agregação para deixar explícito qual
    # subconjunto alimenta os indicadores de uma execução mapeada.
    produtos_da_categoria = [
        produto
        for produto in produtos
        if str(produto.get("category", "")).strip() == categoria
    ]

    # Uma saída vazia previsível evita falha no fan-in caso a categoria deixe
    # de ter produtos entre a descoberta e o cálculo.
    if not produtos_da_categoria:
        return {
            "categoria": categoria,
            "preco_medio": 0.0,
            "preco_minimo": 0.0,
            "preco_maximo": 0.0,
            "quantidade_produtos": 0,
        }

    # A conversão para float normaliza preços numéricos antes das agregações.
    precos = [float(produto["price"]) for produto in produtos_da_categoria]
    quantidade_produtos = len(precos)

    return {
        "categoria": categoria,
        "preco_medio": round(sum(precos) / quantidade_produtos, 2),
        "preco_minimo": round(min(precos), 2),
        "preco_maximo": round(max(precos), 2),
        "quantidade_produtos": quantidade_produtos,
    }
