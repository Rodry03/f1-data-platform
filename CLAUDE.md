# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All dbt commands must be run from inside the `f1_dbt/` directory:

```bash
cd f1_dbt

# Full pipeline
dbt seed          # Load CSV seeds into DuckDB
dbt run           # Build all models
dbt test          # Run all tests

# Targeted runs
dbt run --select stg_results          # Single model
dbt run --select staging.*            # Entire layer
dbt run --select +driver_standings    # Model + all its upstream dependencies
dbt test --select driver_standings    # Test a specific model

# Docs
dbt docs generate && dbt docs serve
```

Ingestion and RAG scripts run from the **project root** (not `f1_dbt/`):

```bash
# Ingest a season from the FastF1 API
python ingest_f1.py 2023        # defaults to 2022 if no arg

# Export DuckDB raw tables → dbt seed CSVs (required after ingest)
python export_seeds.py

# Launch the RAG chatbot
python rag_f1.py
```

## Architecture

The pipeline has three distinct stages, each with its own entry point:

```
FastF1 API
    ↓  ingest_f1.py
f1_data.duckdb  (raw_races, raw_results, raw_fastest_laps tables)
    ↓  export_seeds.py
f1_dbt/seeds/*.csv
    ↓  dbt seed
DuckDB (main schema) ← source: f1_raw
    ↓  dbt run (staging layer — incremental views)
stg_races / stg_results / stg_fastest_laps
    ↓  dbt run (marts layer — tables)
driver_standings / team_performance / fastest_laps_enriched
    ↓  rag_f1.py
RAG Chatbot (LangChain + Groq llama-3.3-70b)
```

**Critical detail:** dbt does not read the raw DuckDB tables written by `ingest_f1.py` directly. `export_seeds.py` must be run after ingestion to write the raw tables back to CSV, which dbt then loads via `dbt seed`. This is why CI runs `dbt seed` first.

## dbt Project details

- **Profile:** `f1_dbt` → DuckDB file at `../f1_data.duckdb` (relative to `f1_dbt/`)
- **Staging** materialised as incremental views; incremental predicate is `year > max(season_year)` on the existing table — adding a new season requires re-seeding and running.
- **Marts** materialised as tables.

### Macros (`f1_dbt/macros/`)

| Macro | Usage |
|---|---|
| `classify_position(col)` | Returns a Spanish label: Victoria / Podio / Puntos / Sin puntos / No clasificado |
| `format_lap_time(seconds)` | Formats seconds → `MM:SS.fff` using DuckDB's `printf` — not portable to other adapters |
| `unique_combination` | Custom dbt test asserting uniqueness across a combination of columns |

### Surrogate keys

`stg_results` uses `dbt_utils.generate_surrogate_key(['year', 'round', 'driver_code'])`. Any new staging model that needs a surrogate key should follow the same pattern.

## Environment

Copy `.env.example` to `.env` and fill in:

```
GROQ_API_KEY=...   # required for rag_f1.py (free at console.groq.com)
```

The `rag_f1.py` chatbot queries the three mart tables directly via DuckDB. It must be run from the project root so it can resolve `f1_data.duckdb`.
