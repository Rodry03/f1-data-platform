# F1 Data Platform

Pipeline de datos de Fórmula 1 construido como proyecto portfolio para demostrar
habilidades de Data Engineering moderno: ingesta, transformación con dbt,
arquitectura medallion sobre DuckDB y chatbot RAG en lenguaje natural.

## Arquitectura
FastF1 API → Python (ingest) → DuckDB (raw) → dbt staging → dbt marts → RAG Chatbot

### Capas

| Capa | Modelos | Descripción |
|------|---------|-------------|
| Raw | `raw_races`, `raw_results`, `raw_fastest_laps` | Datos crudos de la API FastF1 |
| Staging | `stg_races`, `stg_results`, `stg_fastest_laps` | Limpieza de tipos y estandarización |
| Marts | `driver_standings`, `team_performance`, `fastest_laps_enriched` | Modelos de negocio listos para análisis |
| RAG | `rag_f1.py` | Chatbot que responde preguntas en lenguaje natural sobre los datos |

## Demo — RAG Chatbot

El chatbot traduce preguntas en lenguaje natural a SQL sobre los modelos dbt
y responde usando exclusivamente los datos de la base de datos:
Tu pregunta: ¿Quién ganó más carreras en 2022?
SQL generado:
SELECT driver_name, team_name, wins FROM driver_standings
WHERE season_year = 2022 ORDER BY wins DESC LIMIT 1
Respuesta: Max Verstappen ganó más carreras en 2022, con un total de 14 victorias.

## Stack técnico

- **Python** — ingesta de datos vía FastF1
- **DuckDB** — base de datos analítica local
- **dbt-duckdb 1.8** — transformación, tests y documentación
- **LangChain + Groq (llama-3.3-70b)** — Text-to-SQL RAG
- **GitHub Actions** — CI con `dbt test` en cada push

## Datos

Temporada 2022 de Fórmula 1:
- 22 carreras
- 400 resultados de pilotos
- 20 vueltas rápidas

## Cómo ejecutar

```bash
# 1. Crear entorno virtual con Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 2. Instalar dependencias
pip install dbt-duckdb==1.8.2 fastf1 pandas langchain langchain-groq python-dotenv

# 3. Configurar API key
cp .env.example .env
# Edita .env y añade tu GROQ_API_KEY (gratuita en console.groq.com)

# 4. Cargar datos raw
mkdir cache_f1
python ingest_f1.py

# 5. Ejecutar transformaciones
cd f1_dbt
dbt run
dbt test

# 6. Lanzar el chatbot
cd ..
python rag_f1.py
```

## Tests de calidad

10 tests automatizados cubriendo:
- Integridad referencial entre modelos
- Unicidad de claves de negocio
- Ausencia de nulos en campos críticos

## Próximos pasos

- [ ] Migración a Microsoft Fabric + dbt Cloud
- [ ] Ingesta de temporadas 2023 y 2024
- [ ] Streaming en tiempo real durante clasificaciones
- [ ] Interfaz web con Streamlit