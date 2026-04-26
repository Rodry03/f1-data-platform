import fastf1
import duckdb
import pandas as pd
from pathlib import Path
import sys

Path("cache_f1").mkdir(exist_ok=True)
fastf1.Cache.enable_cache("cache_f1")

con = duckdb.connect("f1_data.duckdb")

# Acepta el año como argumento, por defecto 2022
year = int(sys.argv[1]) if len(sys.argv) > 1 else 2022
print(f"\nCargando temporada {year}...")

carreras = []
resultados = []
vueltas_rapidas = []

schedule = fastf1.get_event_schedule(year, include_testing=False)
for _, evento in schedule.iterrows():
    ronda = int(evento["RoundNumber"])
    nombre = evento["EventName"]
    pais = evento["Country"]
    fecha = str(evento["EventDate"].date())
    carreras.append({
        "year": year,
        "round": ronda,
        "event_name": nombre,
        "country": pais,
        "date": fecha
    })
    try:
        session = fastf1.get_session(year, ronda, "R")
        session.load()
        res = session.results
        if res is not None and len(res) > 0:
            for _, row in res.iterrows():
                resultados.append({
                    "year": year,
                    "round": ronda,
                    "event_name": nombre,
                    "driver_number": str(row.get("DriverNumber", "")),
                    "driver_code": str(row.get("Abbreviation", "")),
                    "full_name": str(row.get("FullName", "")),
                    "team": str(row.get("TeamName", "")),
                    "position": row.get("Position", None),
                    "points": row.get("Points", None),
                    "status": str(row.get("Status", ""))
                })
        laps = session.laps
        if laps is not None and len(laps) > 0:
            fastest = laps.pick_fastest()
            if fastest is not None and not fastest.empty:
                vueltas_rapidas.append({
                    "year": year,
                    "round": ronda,
                    "event_name": nombre,
                    "driver_code": str(fastest.get("Driver", "")),
                    "team": str(fastest.get("Team", "")),
                    "lap_time_seconds": fastest["LapTime"].total_seconds() if pd.notna(fastest["LapTime"]) else None
                })
    except Exception as e:
        print(f"  Saltando {year} ronda {ronda}: {e}")

df_carreras = pd.DataFrame(carreras)
df_resultados = pd.DataFrame(resultados)
df_vueltas = pd.DataFrame(vueltas_rapidas)

print(f"\nFilas recopiladas:")
print(f"  Carreras:        {len(df_carreras)}")
print(f"  Resultados:      {len(df_resultados)}")
print(f"  Vueltas rapidas: {len(df_vueltas)}")

# Insertar en DuckDB usando INSERT OR REPLACE para no duplicar
if len(df_carreras) > 0:
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_races AS SELECT * FROM df_carreras WHERE 1=0
    """)
    con.execute("INSERT OR REPLACE INTO raw_races SELECT * FROM df_carreras")

if len(df_resultados) > 0:
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_results AS SELECT * FROM df_resultados WHERE 1=0
    """)
    con.execute("INSERT OR REPLACE INTO raw_results SELECT * FROM df_resultados")

if len(df_vueltas) > 0:
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_fastest_laps AS SELECT * FROM df_vueltas WHERE 1=0
    """)
    con.execute("INSERT OR REPLACE INTO raw_fastest_laps SELECT * FROM df_vueltas")

print(f"\nTemporada {year} cargada en f1_data.duckdb")
con.close()