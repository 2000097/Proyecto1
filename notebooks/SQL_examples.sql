-- Auditoría y time travel en Delta Lake

-- Conteo actual
SELECT count(*) AS rows_now FROM delta.`dbfs:/tmp/lottery/silver_bets`;

-- Viajar al primer snapshot
SELECT count(*) AS rows_v0 FROM delta.`dbfs:/tmp/lottery/silver_bets` VERSION AS OF 0;

-- Historial de operaciones (si usas Unity Catalog, reemplaza por catálogo.esquema.tabla)
DESCRIBE HISTORY delta.`dbfs:/tmp/lottery/silver_bets`;

-- Bitácora de calidad (últimos 50 eventos)
SELECT * FROM delta.`dbfs:/tmp/lottery/dq_logs` ORDER BY event_ts DESC LIMIT 50;
