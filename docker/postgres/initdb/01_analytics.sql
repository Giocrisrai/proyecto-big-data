-- Se ejecuta SOLO en un volumen de datos limpio (primer arranque de Postgres).
-- Crea la base 'analytics' y la tabla de agregados que alimenta Grafana.
CREATE DATABASE analytics;
\connect analytics
CREATE TABLE IF NOT EXISTS ventas_agg (
    region          TEXT,
    n_tx            BIGINT,
    monto_total     BIGINT,
    actualizado_en  TIMESTAMP DEFAULT now()
);
