-- Inicializacao do banco analitico da ShopBrasil.
-- O mesmo servico PostgreSQL tambem hospeda o banco de metadados do Airflow.

SELECT 'CREATE DATABASE analytics OWNER airflow'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'analytics'
)\gexec

\connect analytics

CREATE TABLE IF NOT EXISTS metricas_categoria_snapshot (
    data_referencia DATE NOT NULL,
    categoria TEXT NOT NULL,
    preco_medio NUMERIC(10, 2) NOT NULL,
    preco_minimo NUMERIC(10, 2) NOT NULL,
    preco_maximo NUMERIC(10, 2) NOT NULL,
    quantidade_produtos INTEGER NOT NULL,
    atualizado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    categoria TEXT NOT NULL,
    preco_medio NUMERIC(10, 2) NOT NULL,
    preco_minimo NUMERIC(10, 2) NOT NULL,
    preco_maximo NUMERIC(10, 2) NOT NULL,
    quantidade_produtos INTEGER NOT NULL,
    criado_em TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metricas_snapshot_data
    ON metricas_categoria_snapshot (data_referencia);

CREATE INDEX IF NOT EXISTS idx_metricas_historico_data
    ON metricas_categoria_historico (data_referencia);
