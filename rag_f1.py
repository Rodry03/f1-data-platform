import os
import duckdb
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

load_dotenv()

con = duckdb.connect("f1_data.duckdb")

def get_schema():
    schema = {}
    tables = ["driver_standings", "team_performance", "fastest_laps_enriched"]
    for table in tables:
        cols = con.execute(f"DESCRIBE {table}").fetchall()
        schema[table] = [f"{col[0]} ({col[1]})" for col in cols]
    return schema

def run_query(sql: str):
    try:
        result = con.execute(sql).fetchdf()
        return result.to_string(index=False)
    except Exception as e:
        return f"Error ejecutando SQL: {e}"

schema = get_schema()
schema_text = "\n".join([
    f"Tabla: {table}\nColumnas: {', '.join(cols)}"
    for table, cols in schema.items()
])

# llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
llm = ChatGroq(model="llama-3.3-70b-versatile", temperature=0, api_key=os.getenv("GROQ_API_KEY"))


prompt = ChatPromptTemplate.from_messages([
("system", """Eres un experto en Fórmula 1 y analista de datos.
Tienes acceso a una base de datos DuckDB con los siguientes modelos dbt:

{schema}

Cuando el usuario haga una pregunta:
1. Genera una consulta SQL válida para DuckDB que responda la pregunta
2. Incluye SIEMPRE las columnas necesarias para que la respuesta sea completa, no solo el nombre
3. Devuelve SOLO el SQL, sin explicaciones, sin markdown, sin comillas
La consulta debe usar exactamente los nombres de tabla y columna del schema."""),
    ("human", "{question}")
])

sql_chain = prompt | llm

answer_prompt = ChatPromptTemplate.from_messages([
    ("system", """Eres un experto en Fórmula 1. 
Responde en español de forma clara y concisa basándote SOLO en los datos proporcionados.
No inventes información que no esté en los datos."""),
    ("human", """Pregunta: {question}

Datos de la base de datos:
{data}

Responde la pregunta usando estos datos.""")
])

answer_chain = answer_prompt | llm

def ask(question: str):
    print(f"\nPregunta: {question}")
    print("-" * 50)
    
    sql_response = sql_chain.invoke({
        "schema": schema_text,
        "question": question
    })
    sql = sql_response.content.strip()
    print(f"SQL generado:\n{sql}\n")
    
    data = run_query(sql)
    print(f"Datos obtenidos:\n{data}\n")
    
    answer = answer_chain.invoke({
        "question": question,
        "data": data
    })
    print(f"Respuesta: {answer.content}")
    print("=" * 50)

if __name__ == "__main__":
    print("=" * 50)
    print("  F1 Data Platform — RAG Chatbot")
    print("  Datos disponibles: temporada 2022")
    print("  Escribe 'salir' para terminar")
    print("=" * 50)

    preguntas_ejemplo = [
        "¿Quién ganó más carreras en 2022?",
        "¿Qué equipo acumuló más puntos en 2022?",
        "¿Cuál fue la vuelta rápida más rápida?",
        "¿Cuántos podios tuvo Leclerc en 2022?",
        "¿Qué piloto terminó más carreras en 2022?",
    ]

    print("\nPreguntas de ejemplo:")
    for i, p in enumerate(preguntas_ejemplo, 1):
        print(f"  {i}. {p}")
    print()

    while True:
        try:
            question = input("\nTu pregunta: ").strip()
            if question.lower() in ["salir", "exit", "quit", ""]:
                print("Hasta luego.")
                break
            ask(question)
        except KeyboardInterrupt:
            print("\nHasta luego.")
            break