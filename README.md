# F1 Data Platform

Pipeline de datos de Fórmula 1 construido como proyecto portfolio para demostrar
habilidades de Data Engineering moderno: ingesta, transformación con dbt y
arquitectura medallion sobre DuckDB.

## Arquitectura
### Capas

| Capa | Modelos | Descripción |
|------|---------|-------------|
| Raw | `raw_races`, `raw_results`, `raw_fastest_laps` | Datos crudos de la API FastF1 |
| Staging | `stg_races`, `stg_results`, `stg_fastest_laps` | Limpieza de tipos y estandarización |
| Marts | `driver_standings`, `team_performance`, `fastest_laps_enriched` | Modelos de negocio listos para análisis |

## Stack técnico

- **Python** — ingesta de datos vía FastF1
- **DuckDB** — base de datos analítica local
- **dbt-duckdb 1.8** — transformación y documentación
- **GitHub Actions** — CI con `dbt test` en cada push

## Datos

Temporadas 2022, 2023 y 2024 de Fórmula 1:
- 68 carreras
- 400 resultados de pilotos
- 20 vueltas rápidas

## Cómo ejecutar

```bash
# 1. Crear entorno virtual con Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 2. Instalar dependencias
pip install dbt-duckdb==1.8.2 fastf1 pandas

# 3. Cargar datos raw
mkdir cache_f1
python ingest_f1.py

# 4. Ejecutar transformaciones
cd f1_dbt
dbt run
dbt test

# 5. Ver documentación y lineage
dbt docs generate
dbt docs serve
```

## Tests de calidad

10 tests automatizados cubriendo:
- Integridad referencial entre modelos
- Unicidad de claves de negocio
- Ausencia de nulos en campos críticos

## Próximos pasos

- [ ] Capa RAG: chatbot que responde preguntas sobre los datos en lenguaje natural
- [ ] Streaming: ingesta en tiempo real durante clasificaciones y carreras
- [ ] Azure: migración de DuckDB a Microsoft Fabric