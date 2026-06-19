"""Valida a regra de cálculo das métricas de pricing por categoria.

Os cenários exercitam agregação, arredondamento monetário e o contrato de saída
para categorias sem produtos, sem depender do runtime do Airflow.
"""

from __future__ import annotations

from dags.metricas import calcular_metricas_categoria


def test_calcula_metricas_de_categoria_com_multiplos_produtos() -> None:
    """Confere média, extremos e quantidade usando apenas a categoria alvo."""
    produtos = [
        {"id": 1, "title": "Camisa", "price": 10.0, "category": "moda"},
        {"id": 2, "title": "Calca", "price": 30.0, "category": "moda"},
        {"id": 3, "title": "Livro", "price": 50.0, "category": "livros"},
    ]

    metricas_categoria = calcular_metricas_categoria(produtos, "moda")

    assert metricas_categoria["categoria"] == "moda"
    assert metricas_categoria["preco_medio"] == 20.0
    assert metricas_categoria["preco_minimo"] == 10.0
    assert metricas_categoria["preco_maximo"] == 30.0
    assert metricas_categoria["quantidade_produtos"] == 2


def test_calcula_metricas_com_arredondamento_de_duas_casas() -> None:
    """Garante que os indicadores monetários usem duas casas decimais."""
    produtos = [
        {"id": 1, "title": "Produto A", "price": 10.0, "category": "tech"},
        {"id": 2, "title": "Produto B", "price": 10.01, "category": "tech"},
        {"id": 3, "title": "Produto C", "price": 10.02, "category": "tech"},
    ]

    metricas_categoria = calcular_metricas_categoria(produtos, "tech")

    assert metricas_categoria["preco_medio"] == 10.01
    assert metricas_categoria["preco_minimo"] == 10.0
    assert metricas_categoria["preco_maximo"] == 10.02
    assert metricas_categoria["quantidade_produtos"] == 3


def test_categoria_sem_produtos_retorna_metricas_zeradas() -> None:
    """Confere o contrato zerado quando não existem itens na categoria."""
    produtos = [
        {"id": 1, "title": "Camisa", "price": 10.0, "category": "moda"},
    ]

    metricas_categoria = calcular_metricas_categoria(produtos, "inexistente")

    assert metricas_categoria["categoria"] == "inexistente"
    assert metricas_categoria["preco_medio"] == 0.0
    assert metricas_categoria["preco_minimo"] == 0.0
    assert metricas_categoria["preco_maximo"] == 0.0
    assert metricas_categoria["quantidade_produtos"] == 0
