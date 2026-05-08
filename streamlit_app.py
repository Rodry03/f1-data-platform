import os
import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

load_dotenv()

st.set_page_config(
    page_title="F1 Data Platform",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background-color: #15151e; }
    [data-testid="stSidebar"] { background-color: #1f1f2e; }
    h1, h2, h3 { color: #ffffff; }
    .stDataFrame { border: 1px solid #2a2a3e; }
    div[data-testid="metric-container"] {
        background-color: #1f1f2e;
        border: 1px solid #2a2a3e;
        border-radius: 8px;
        padding: 12px;
    }
</style>
""", unsafe_allow_html=True)

TEAM_COLORS = {
    "Red Bull Racing": "#3671C6",
    "Ferrari": "#E8002D",
    "Mercedes": "#27F4D2",
    "Alpine": "#FF87BC",
    "McLaren": "#FF8000",
    "Alfa Romeo": "#C92D4B",
    "Aston Martin": "#229971",
    "Haas F1 Team": "#B6BABD",
    "AlphaTauri": "#6692FF",
    "Williams": "#64C4FF",
    "Aston Martin Aramco": "#229971",
    "Kick Sauber": "#C92D4B",
    "RB": "#6692FF",
    "Visa Cash App RB": "#6692FF",
    "Alpine F1 Team": "#FF87BC",
    "Haas": "#B6BABD",
}

DARK_LAYOUT = dict(
    template="plotly_dark",
    plot_bgcolor="#15151e",
    paper_bgcolor="#15151e",
    font_color="#ffffff",
    margin=dict(t=50, b=40),
)


def team_color(team):
    return TEAM_COLORS.get(team, "#e10600")


@st.cache_resource
def get_con():
    return duckdb.connect("f1_data.duckdb", read_only=True)


con = get_con()


def query(sql, params=None):
    if params:
        return con.execute(sql, params).df()
    return con.execute(sql).df()


@st.cache_data
def get_schema_text():
    tables = ["driver_standings", "team_performance", "fastest_laps_enriched"]
    parts = []
    for t in tables:
        cols = con.execute(f"DESCRIBE {t}").fetchall()
        col_str = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
        parts.append(f"Tabla: {t}\nColumnas: {col_str}")
    return "\n\n".join(parts)


@st.cache_resource
def get_llm():
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0,
        api_key=os.getenv("GROQ_API_KEY"),
    )


sql_prompt = ChatPromptTemplate.from_messages([
    ("system", """Eres un experto en Fórmula 1 y analista de datos.
Tienes acceso a una base de datos DuckDB con los siguientes modelos dbt:

{schema}

Cuando el usuario haga una pregunta:
1. Genera una consulta SQL válida para DuckDB que responda la pregunta
2. Incluye SIEMPRE las columnas necesarias para que la respuesta sea completa
3. Devuelve SOLO el SQL, sin explicaciones, sin markdown, sin comillas
La consulta debe usar exactamente los nombres de tabla y columna del schema."""),
    ("human", "{question}"),
])

answer_prompt = ChatPromptTemplate.from_messages([
    ("system", """Eres un experto en Fórmula 1.
Responde en español de forma clara y concisa basándote SOLO en los datos proporcionados.
No inventes información que no esté en los datos."""),
    ("human", "Pregunta: {question}\n\nDatos:\n{data}\n\nResponde la pregunta usando estos datos."),
])

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏎️ F1 Data Platform")
    st.markdown("---")
    page = st.radio(
        "Navegación",
        ["🏆 Pilotos", "📈 Evolución de Puntos", "⚡ Vueltas Rápidas", "🏎️ Equipos", "🤖 Chatbot"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    if page != "🤖 Chatbot" and page != "🏎️ Equipos":
        season = st.selectbox("Temporada", [2026, 2025, 2024, 2023, 2022])
    st.markdown("---")
    if st.button("🔄 Refrescar datos"):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.rerun()


# ── 1. Pilotos ────────────────────────────────────────────────────────────────
if page == "🏆 Pilotos":
    st.title(f"🏆 Clasificación de Pilotos — {season}")

    df = query("""
        SELECT
            championship_position  AS "#",
            driver_name            AS Piloto,
            team_name              AS Equipo,
            total_points           AS Puntos,
            wins                   AS Victorias,
            podiums                AS Podios,
            races_entered          AS Carreras,
            round(avg_finish_position, 1) AS Pos_Media
        FROM driver_standings
        WHERE season_year = ?
        ORDER BY championship_position
    """, [season])

    st.dataframe(df, use_container_width=True, hide_index=True)

    top10 = df.head(10)
    colors = [team_color(t) for t in top10["Equipo"]]

    fig = go.Figure(go.Bar(
        x=top10["Piloto"],
        y=top10["Puntos"],
        marker_color=colors,
        text=top10["Puntos"],
        textposition="outside",
    ))
    fig.update_layout(title="Top 10 — Puntos totales", xaxis_tickangle=-30, **DARK_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        fig2 = go.Figure(go.Bar(
            x=top10["Piloto"], y=top10["Victorias"],
            marker_color=colors, text=top10["Victorias"], textposition="outside",
        ))
        fig2.update_layout(title="Victorias", xaxis_tickangle=-30, **DARK_LAYOUT)
        st.plotly_chart(fig2, use_container_width=True)
    with col2:
        fig3 = go.Figure(go.Bar(
            x=top10["Piloto"], y=top10["Podios"],
            marker_color=colors, text=top10["Podios"], textposition="outside",
        ))
        fig3.update_layout(title="Podios", xaxis_tickangle=-30, **DARK_LAYOUT)
        st.plotly_chart(fig3, use_container_width=True)


# ── 2. Evolución de Puntos ────────────────────────────────────────────────────
elif page == "📈 Evolución de Puntos":
    st.title(f"📈 Evolución de Puntos — {season}")

    df_all = query("""
        SELECT
            driver_name,
            driver_code,
            team_name,
            round_number,
            SUM(points_scored) OVER (
                PARTITION BY driver_code
                ORDER BY round_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_points
        FROM stg_results
        WHERE season_year = ?
        ORDER BY driver_name, round_number
    """, [season])

    races = query("""
        SELECT round_number, event_name
        FROM stg_races
        WHERE season_year = ?
        ORDER BY round_number
    """, [season])

    df_all = df_all.merge(races, on="round_number", how="left")

    driver_names = sorted(df_all["driver_name"].unique().tolist())
    default = driver_names[:2] if len(driver_names) >= 2 else driver_names

    selected = st.multiselect(
        "Selecciona pilotos para comparar",
        driver_names,
        default=default,
        max_selections=10,
    )

    if selected:
        df_filt = df_all[df_all["driver_name"].isin(selected)]
        color_map = {
            d: team_color(df_filt[df_filt["driver_name"] == d]["team_name"].iloc[0])
            for d in selected
        }

        fig = px.line(
            df_filt,
            x="round_number",
            y="cumulative_points",
            color="driver_name",
            color_discrete_map=color_map,
            markers=True,
            hover_data=["event_name"],
            labels={
                "round_number": "Ronda",
                "cumulative_points": "Puntos acumulados",
                "driver_name": "Piloto",
                "event_name": "Carrera",
            },
        )
        fig.update_layout(
            title="Puntos acumulados por ronda",
            hovermode="x unified",
            legend_title="Piloto",
            **DARK_LAYOUT,
        )

        # Eje X con nombres de carrera
        race_labels = races.set_index("round_number")["event_name"].to_dict()
        fig.update_xaxes(
            tickvals=list(race_labels.keys()),
            ticktext=[v.replace(" Grand Prix", " GP") for v in race_labels.values()],
            tickangle=-45,
        )

        st.plotly_chart(fig, use_container_width=True)

        # Métricas finales
        st.markdown("### Puntos finales")
        cols = st.columns(len(selected))
        for i, driver in enumerate(selected):
            final = df_filt[df_filt["driver_name"] == driver]["cumulative_points"].max()
            cols[i].metric(driver, f"{int(final)} pts")
    else:
        st.info("Selecciona al menos un piloto.")


# ── 3. Vueltas Rápidas ────────────────────────────────────────────────────────
elif page == "⚡ Vueltas Rápidas":
    st.title(f"⚡ Vueltas Rápidas — {season}")

    df_fl = query("""
        SELECT
            round_number    AS Ronda,
            event_name      AS Carrera,
            driver_code     AS Piloto,
            team_name       AS Equipo,
            lap_time_formatted AS Tiempo,
            lap_time_seconds   AS Segundos,
            fastest_lap_rank_in_season AS Rank_Temporada
        FROM fastest_laps_enriched
        WHERE season_year = ?
        ORDER BY round_number
    """, [season])

    st.dataframe(df_fl.drop(columns=["Segundos"]), use_container_width=True, hide_index=True)

    colors_fl = [team_color(t) for t in df_fl["Equipo"]]

    fig = go.Figure(go.Bar(
        x=df_fl["Carrera"].str.replace(" Grand Prix", " GP"),
        y=df_fl["Segundos"],
        marker_color=colors_fl,
        text=df_fl["Tiempo"],
        textposition="outside",
        customdata=df_fl[["Piloto", "Equipo"]],
        hovertemplate="<b>%{x}</b><br>%{customdata[0]} — %{customdata[1]}<br>%{text}<extra></extra>",
    ))
    fig.update_layout(
        title="Tiempo de vuelta rápida por carrera (segundos)",
        xaxis_tickangle=-45,
        yaxis_title="Segundos",
        yaxis=dict(range=[
            df_fl["Segundos"].min() * 0.995,
            df_fl["Segundos"].max() * 1.005,
        ]),
        **DARK_LAYOUT,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Ranking de vueltas rápidas en la temporada")
    df_rank = df_fl.sort_values("Rank_Temporada")[["Rank_Temporada", "Piloto", "Equipo", "Carrera", "Tiempo"]].head(5)
    st.dataframe(df_rank, use_container_width=True, hide_index=True)


# ── 4. Equipos ────────────────────────────────────────────────────────────────
elif page == "🏎️ Equipos":
    st.title("🏎️ Histórico de Equipos")

    df_teams = query("""
        SELECT
            season_year     AS Año,
            team_name       AS Equipo,
            total_points    AS Puntos,
            wins            AS Victorias,
            podiums         AS Podios,
            total_entries   AS Entradas,
            round(avg_finish_position, 1) AS Pos_Media
        FROM team_performance
        ORDER BY season_year, Puntos DESC
    """)

    with st.sidebar:
        sel_season = st.selectbox("Temporada (tabla)", sorted(df_teams["Año"].unique(), reverse=True))

    st.subheader(f"Temporada {sel_season}")
    st.dataframe(
        df_teams[df_teams["Año"] == sel_season].drop(columns=["Año"]),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### Evolución de puntos por equipo")
    teams_avail = sorted(df_teams["Equipo"].unique().tolist())
    sel_teams = st.multiselect(
        "Equipos a comparar",
        teams_avail,
        default=teams_avail[:6],
    )

    if sel_teams:
        df_filt = df_teams[df_teams["Equipo"].isin(sel_teams)]
        color_map_t = {t: team_color(t) for t in sel_teams}
        fig = px.line(
            df_filt,
            x="Año",
            y="Puntos",
            color="Equipo",
            color_discrete_map=color_map_t,
            markers=True,
            text="Puntos",
            labels={"Año": "Temporada", "Puntos": "Puntos totales"},
        )
        fig.update_traces(textposition="top center")
        fig.update_xaxes(tickvals=sorted(df_teams["Año"].unique()))
        fig.update_layout(
            title="Puntos por temporada",
            hovermode="x unified",
            legend_title="Equipo",
            **DARK_LAYOUT,
        )
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            fig2 = px.bar(
                df_filt, x="Año", y="Victorias", color="Equipo",
                color_discrete_map=color_map_t, barmode="group",
                labels={"Año": "Temporada"},
            )
            fig2.update_layout(title="Victorias por temporada", **DARK_LAYOUT)
            st.plotly_chart(fig2, use_container_width=True)
        with col2:
            fig3 = px.bar(
                df_filt, x="Año", y="Podios", color="Equipo",
                color_discrete_map=color_map_t, barmode="group",
                labels={"Año": "Temporada"},
            )
            fig3.update_layout(title="Podios por temporada", **DARK_LAYOUT)
            st.plotly_chart(fig3, use_container_width=True)


# ── 5. Chatbot ────────────────────────────────────────────────────────────────
elif page == "🤖 Chatbot":
    st.title("🤖 F1 Chatbot")
    st.caption("Pregunta cualquier cosa sobre los datos de F1 — temporadas 2022, 2023 y 2024")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg:
                with st.expander("Ver SQL generado"):
                    st.code(msg["sql"], language="sql")

    if prompt := st.chat_input("¿Quién ganó más carreras en 2023?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consultando datos..."):
                llm = get_llm()
                schema_text = get_schema_text()

                sql_response = (sql_prompt | llm).invoke({
                    "schema": schema_text,
                    "question": prompt,
                })
                sql = sql_response.content.strip()

                try:
                    data = con.execute(sql).df().to_string(index=False)
                except Exception as e:
                    data = f"Error ejecutando SQL: {e}"

                answer = (answer_prompt | llm).invoke({
                    "question": prompt,
                    "data": data,
                })
                response_text = answer.content

            st.markdown(response_text)
            with st.expander("Ver SQL generado"):
                st.code(sql, language="sql")

        st.session_state.messages.append({
            "role": "assistant",
            "content": response_text,
            "sql": sql,
        })
