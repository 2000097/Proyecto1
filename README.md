# Lottery Fraud MLOps (Databricks + GitHub)

Este proyecto implementa un flujo MLOps para detección de fraude en apuestas de lotería con:
- **Controles de calidad** (duplicados, nulos, rangos) y **bitácora** de limpieza en Delta.
- **Versionado de datos** con **Delta Lake** (time travel).
- **Generación de features** reutilizables (monto promedio por usuario, # apuestas 7 días, riesgo por IP/geo).
- **Entrenamiento de modelo** con **MLflow** y registro del modelo.
- **CI/CD** con GitHub Actions (lint + tests).

## Estructura
```
mlops-lottery-fraud/
  config/
    mapping.yaml         # mapeo de columnas de tu CSV -> nombres canónicos
    params.yaml          # parámetros de calidad, rutas y modelado
  src/
    ingest_and_clean.py  # ingesta a Bronze y limpieza a Silver + bitácora
    feature_engineering.py
    train.py
    udfs.py
  tests/
    test_udfs.py         # pruebas unitarias
  notebooks/
    SQL_examples.sql     # ejemplos de time travel y auditoría
  .github/workflows/ci.yml
  README.md
```

## Uso en Databricks
1. Sube tu CSV a DBFS (o monta un bucket). Actualiza `config/mapping.yaml` y `config/params.yaml` si es necesario.
2. En un **Cluster** con Runtime 13+ (o superior) y MLflow habilitado, crea un **Repo** apuntando a tu GitHub y sincroniza este proyecto.
3. Ejecuta secuencialmente:
   - `src/ingest_and_clean.py`
   - `src/feature_engineering.py`
   - `src/train.py`
4. Explora los datos y la bitácora:
   ```sql
   SELECT * FROM delta.`dbfs:/tmp/lottery/dq_logs` ORDER BY event_ts DESC;
   ```
5. Prueba **time travel**:
   ```sql
   SELECT count(*) FROM delta.`dbfs:/tmp/lottery/silver_bets` VERSION AS OF 0;
   ```

## Rama y commits (Git)
- Ramas: `main` (producción), `dev` (experimentos). Pull Requests con revisión.
- Cada cambio debe incluir: descripción, afectación de tablas/funciones, y evidencias (capturas o enlaces a dashboards/MLflow).

## Seguridad y Gobernanza
- Usa permisos en catálogo/DBFS y Unity Catalog (si disponible).
- Mantén evidencia en la bitácora Delta y en los PRs de GitHub.

