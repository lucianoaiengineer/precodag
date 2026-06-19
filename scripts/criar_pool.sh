#!/usr/bin/env bash
set -euo pipefail

POOL_ID="ecommerce_pool"
SLOTS="2"
DESCRICAO="Limita concorrencia das tasks mapeadas de pricing por categoria."

# "airflow pools set" cria ou atualiza o mesmo pool, permitindo reexecucao.
airflow pools set \
  "${POOL_ID}" \
  "${SLOTS}" \
  "${DESCRICAO}"

airflow pools get "${POOL_ID}"
