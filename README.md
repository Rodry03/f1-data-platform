# F1 Data Platform

Pipeline de datos de Fórmula 1 construido como proyecto portfolio para demostrar
habilidades de Data Engineering moderno: ingesta, transformación con dbt,
arquitectura medallion sobre DuckDB y chatbot RAG en lenguaje natural.

## Arquitectura
FastF1 API → Python (ingest) → DuckDB (raw) → CSV seeds → dbt staging → dbt marts → RAG Chatbot / Streamlit

### Capas

| Capa | Modelos | Descripción |
|------|---------|-------------|
| Raw | `raw_races`, `raw_results`, `raw_fastest_laps`, `raw_track_status`, `raw_leader_laps` | Datos crudos de la API FastF1, versionados como seeds |
| Staging | `stg_races`, `stg_results`, `stg_fastest_laps`, `stg_track_status`, `stg_leader_laps` | Limpieza de tipos y estandarización (incrementales; `stg_races` es vista) |
| Marts | `driver_standings`, `team_performance`, `fastest_laps_enriched`, `track_status_periods` | Modelos de negocio listos para análisis |
| App | `streamlit_app.py`, `rag_f1.py` | Dashboard web y chatbot que responde preguntas en lenguaje natural |

### Estado de pista

`track_status_periods` tiene un periodo por fila (green, yellow, safety_car, red_flag,
vsc_deployed, vsc_ending), con su inicio y fin en segundos de sesión, la duración y las
vueltas del líder en las que empieza y termina (`start_lap = 0` = pre-carrera).
`is_restart_procedure` marca el Safety Car del procedimiento de reanudación tras una
bandera roja, para no contarlo como SC por incidente.

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
- **dbt-core 1.11 + dbt-duckdb 1.8** — transformación, tests y documentación
- **LangChain + Groq (llama-3.3-70b)** — Text-to-SQL RAG
- **Streamlit + Plotly** — dashboard web
- **GitHub Actions** — CI con `dbt test` en cada push

## Datos

Temporadas 2022 a 2026 de Fórmula 1 (2026 hasta la ronda 16):
- 108 carreras disputadas (22 + 22 + 24 + 24 + 16) sobre 115 del calendario
- 2.190 resultados de pilotos
- 108 vueltas rápidas
- 1.408 periodos de estado de pista y 6.469 vueltas del líder

## Cómo ejecutar

```bash
# 1. Crear entorno virtual con Python 3.11
py -3.11 -m venv venv
venv\Scripts\activate

# 2. Instalar dependencias
pip install -r requirements.txt -r requirements-dev.txt

# 3. Configurar API key
cp .env.example .env
# Edita .env y añade tu GROQ_API_KEY (gratuita en console.groq.com)

# 4. Cargar datos raw (una temporada por ejecución; por defecto 2022).
#    Es idempotente: solo descarga las rondas que faltan en cada tabla
python ingest_f1.py 2024

# 5. Exportar raw → seeds CSV (dbt lee los seeds, no las tablas de la ingesta)
python export_seeds.py

# 6. Ejecutar transformaciones y tests
cd f1_dbt
dbt deps
dbt seed
dbt build

# 7. Lanzar la aplicación (desde la raíz del proyecto)
cd ..

# Opción A — app web con Streamlit (dashboard + chatbot), en http://localhost:8501
streamlit run streamlit_app.py

# Opción B — chatbot por terminal (escribe 'salir' para terminar)
python rag_f1.py
```

Ambas leen `f1_data.duckdb` y necesitan `GROQ_API_KEY` en `.env` para el chatbot.

## Tests de calidad

25 tests dbt automatizados:
- Ausencia de nulos en campos críticos (`not_null`)
- Unicidad de claves de negocio (`unique_combination`, test propio)
- Periodos de estado de pista y vueltas del líder sin huecos ni solapes (`dbt_utils.mutually_exclusive_ranges`)
- Códigos de estado de pista válidos (`accepted_values`, con severidad warn)
- Coherencia de `is_restart_procedure` (`dbt_utils.expression_is_true`)

## Próximos pasos

- [ ] Migración a Microsoft Fabric + dbt Cloud
- [x] Ingesta de temporadas 2023 a 2026
- [x] Estado de pista (track status) por periodos
- [ ] Streaming en tiempo real durante clasificaciones
- [x] Interfaz web con Streamlit