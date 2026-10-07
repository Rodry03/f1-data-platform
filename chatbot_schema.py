# Contexto de esquema compartido por rag_f1.py y streamlit_app.py

CHATBOT_TABLES = [
    "driver_standings",
    "team_performance",
    "fastest_laps_enriched",
    "track_status_periods",
]

# DESCRIBE solo da nombres y tipos; aquí va la semántica que el LLM no puede deducir
TABLE_NOTES = {
    "track_status_periods": """Notas: un periodo de estado de pista por fila (desde un cambio de estado hasta el siguiente).
status_label: green, yellow, safety_car, red_flag, vsc_deployed, vsc_ending (status_code 1, 2, 4, 5, 6, 7).
start_time_s / end_time_s: segundos desde el inicio de la sesión; duration_s: duración en segundos.
start_lap / end_lap: vuelta del líder al empezar / terminar el periodo; 0 = pre-carrera.
is_restart_procedure: true si el Safety Car es el procedimiento de reanudación tras una bandera roja.
Para contar Safety Cars reales, filtrar status_label = 'safety_car' AND is_restart_procedure = false.
event_name está en inglés (ej. 'Monaco Grand Prix').
Ejemplos:
- ¿En qué vueltas hubo Safety Car o bandera roja en Mónaco 2024?
  SELECT status_label, start_lap, end_lap, duration_s, is_restart_procedure FROM track_status_periods WHERE season_year = 2024 AND event_name = 'Monaco Grand Prix' AND status_label IN ('safety_car', 'red_flag') ORDER BY period_number
- ¿Qué carrera de 2024 pasó más tiempo bajo VSC?
  SELECT event_name, round(sum(duration_s) / 60, 1) AS minutos_vsc FROM track_status_periods WHERE season_year = 2024 AND status_label IN ('vsc_deployed', 'vsc_ending') GROUP BY event_name ORDER BY minutos_vsc DESC LIMIT 1
- ¿Cuántas banderas rojas hubo por temporada?
  SELECT season_year, count(*) AS banderas_rojas FROM track_status_periods WHERE status_label = 'red_flag' GROUP BY season_year ORDER BY season_year""",
}
