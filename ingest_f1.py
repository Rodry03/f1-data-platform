import sys
from pathlib import Path

import duckdb
import fastf1
import pandas as pd

Path("cache_f1").mkdir(exist_ok=True)
fastf1.Cache.enable_cache("cache_f1")

con = duckdb.connect("f1_data.duckdb")

# Acepta el año como argumento, por defecto 2022
year = int(sys.argv[1]) if len(sys.argv) > 1 else 2022
print(f"\nCargando temporada {year}...")

def rondas_existentes(tabla):
    # Cada tabla raw se comprueba por separado: así una tabla nueva puede
    # rellenar rondas antiguas sin duplicar las tablas que ya las tienen
    try:
        rows = con.execute(f"SELECT DISTINCT round FROM {tabla} WHERE year = ?", [year]).fetchall()
        return {r[0] for r in rows}
    except duckdb.CatalogException:
        return set()

existing_rounds = rondas_existentes("raw_results")
existing_track_status = rondas_existentes("raw_track_status")
existing_leader_laps = rondas_existentes("raw_leader_laps")
if existing_rounds:
    print(f"Rondas ya en BD: {sorted(existing_rounds)}")

carreras = []
resultados = []
vueltas_rapidas = []
estados_pista = []
vueltas_lider = []

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
    falta_resultados = ronda not in existing_rounds
    falta_track_status = ronda not in existing_track_status
    falta_leader_laps = ronda not in existing_leader_laps
    if not (falta_resultados or falta_track_status or falta_leader_laps):
        print(f"  Ronda {ronda} ({nombre}) ya descargada, saltando...")
        continue
    try:
        session = fastf1.get_session(year, ronda, "R")
        session.load()
        res = session.results
        laps = session.laps
        if falta_resultados and res is not None and len(res) > 0:
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
        if falta_resultados and laps is not None and len(laps) > 0:
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
        # Track status: Time es relativo al inicio de la sesión (misma base que laps.Time)
        track_status = session.track_status
        if falta_track_status and track_status is not None and len(track_status) > 0:
            # Fin de sesión = última marca de session_status ('Ends'); si no hay, última vuelta
            session_status = session.session_status
            if session_status is not None and len(session_status) > 0:
                fin_sesion = session_status["Time"].max()
            else:
                fin_sesion = laps["Time"].max()
            # seq conserva el orden de origen para desempatar timestamps duplicados
            for seq, (_, row) in enumerate(track_status.iterrows()):
                estados_pista.append({
                    "year": year,
                    "round": ronda,
                    "event_name": nombre,
                    "seq": seq,
                    "time": row["Time"],
                    "status": str(row["Status"]),
                    "message": str(row["Message"]),
                    "session_end_time": fin_sesion
                })
        # Vueltas del líder: el primer piloto en completar cada vuelta
        if falta_leader_laps and laps is not None and len(laps) > 0:
            completadas = laps.dropna(subset=["Time"])
            lideres = completadas.loc[completadas.groupby("LapNumber")["Time"].idxmin()]
            for _, vuelta in lideres.iterrows():
                vueltas_lider.append({
                    "year": year,
                    "round": ronda,
                    "event_name": nombre,
                    "lap_number": int(vuelta["LapNumber"]),
                    "driver_code": str(vuelta["Driver"]),
                    "lap_start_time": vuelta["LapStartTime"],
                    "lap_end_time": vuelta["Time"]
                })
    except Exception as e:
        print(f"  Saltando {year} ronda {ronda}: {e}")

df_carreras = pd.DataFrame(carreras)
df_resultados = pd.DataFrame(resultados)
df_vueltas = pd.DataFrame(vueltas_rapidas)
df_estados_pista = pd.DataFrame(estados_pista)
df_vueltas_lider = pd.DataFrame(vueltas_lider)

print("\nFilas recopiladas:")
print(f"  Carreras:        {len(df_carreras)}")
print(f"  Resultados:      {len(df_resultados)}")
print(f"  Vueltas rapidas: {len(df_vueltas)}")
print(f"  Estados pista:   {len(df_estados_pista)}")
print(f"  Vueltas lider:   {len(df_vueltas_lider)}")

# raw_races: siempre tenemos todas las rondas, reemplazamos el año completo
con.execute("CREATE TABLE IF NOT EXISTS raw_races AS SELECT * FROM df_carreras WHERE 1=0")
con.execute("DELETE FROM raw_races WHERE year = ?", [year])
con.execute("INSERT INTO raw_races SELECT * FROM df_carreras")

# raw_results, raw_fastest_laps, raw_track_status y raw_leader_laps:
# solo contienen las rondas que faltaban en cada tabla, INSERT directo
if len(df_resultados) > 0:
    con.execute("CREATE TABLE IF NOT EXISTS raw_results AS SELECT * FROM df_resultados WHERE 1=0")
    con.execute("INSERT INTO raw_results SELECT * FROM df_resultados")

if len(df_vueltas) > 0:
    con.execute("CREATE TABLE IF NOT EXISTS raw_fastest_laps AS SELECT * FROM df_vueltas WHERE 1=0")
    con.execute("INSERT INTO raw_fastest_laps SELECT * FROM df_vueltas")

if len(df_estados_pista) > 0:
    con.execute("CREATE TABLE IF NOT EXISTS raw_track_status AS SELECT * FROM df_estados_pista WHERE 1=0")
    con.execute("INSERT INTO raw_track_status SELECT * FROM df_estados_pista")

if len(df_vueltas_lider) > 0:
    con.execute("CREATE TABLE IF NOT EXISTS raw_leader_laps AS SELECT * FROM df_vueltas_lider WHERE 1=0")
    con.execute("INSERT INTO raw_leader_laps SELECT * FROM df_vueltas_lider")

print(f"\nTemporada {year} cargada en f1_data.duckdb")
con.close()