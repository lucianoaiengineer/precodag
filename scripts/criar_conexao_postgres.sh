#!/usr/bin/env bash
set -euo pipefail

CONN_ID="postgres_analytics"

# Remove todas as entradas com o mesmo conn_id antes de recriar a Connection.
# Os valores abaixo sao didaticos para ambiente local. Nao usar em producao.
python -c "
from airflow.models.connection import Connection
from airflow.utils.session import create_session

conn_id = '${CONN_ID}'

with create_session() as session:
    removidas = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .delete(synchronize_session=False)
    )
    session.commit()

print(f'Connections removidas para {conn_id}: {removidas}')
"

airflow connections add "${CONN_ID}" \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema analytics \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432

airflow connections get "${CONN_ID}"
