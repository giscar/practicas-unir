import psycopg2
import requests
import re
import sys
import time

print(">>> INICIO DEL SISTEMA INTELIGENTE")

# ---------------------------
# CONFIG MODELOS
# ---------------------------
MODELO_SQL = "mistral"
MODELO_RESPUESTA = "llama3"

# ---------------------------
# HISTORIAL (memoria simple)
# ---------------------------
historial = []

# ---------------------------
# CONEXIÓN BD
# ---------------------------
def get_conn():
    try:
        return psycopg2.connect(
            host="localhost",
            database="empresa",
            user="admin",
            password="admin"
        )
    except Exception as e:
        print("❌ Error BD:", e)
        return None

# ---------------------------
# EJECUTAR SQL
# ---------------------------
def ejecutar_sql(query):
    conn = get_conn()
    if conn is None:
        return "error conexion"

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    except Exception as e:
        return f"error sql: {e}"

# ---------------------------
# LLAMADA SEGURA A OLLAMA
# ---------------------------
def llamar_ollama(modelo, prompt, temperature=0.2, max_tokens=150):
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": modelo,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens
                }
            }
        )

        data = response.json()

        if "response" not in data:
            return f"error llm: {data}"

        return data["response"]

    except Exception as e:
        return f"error llm: {e}"

# ---------------------------
# LIMPIAR SQL
# ---------------------------
def limpiar_sql(sql):
    sql = re.sub(r"```sql|```", "", sql).strip()
    sql = sql.strip("'\"")
    return sql.strip()

# ---------------------------
# VALIDACIÓN AVANZADA SQL
# ---------------------------
def validar_sql(sql):
    sql_lower = sql.lower()

    if not sql_lower.startswith("select"):
        return False, "Solo se permiten consultas SELECT"

    peligrosos = ["drop", "delete", "update", "insert", "alter"]
    if any(p in sql_lower for p in peligrosos):
        return False, "Consulta contiene operaciones no permitidas"

    tablas_validas = ["clientes", "productos", "pedidos", "detalle_pedido"]
    if not any(t in sql_lower for t in tablas_validas):
        return False, "Consulta no usa tablas válidas"

    return True, "OK"

# ---------------------------
# CLASIFICADOR DE INTENCIÓN
# ---------------------------
def clasificar_intencion(pregunta):
    prompt = f"""
Clasifica la intención:

Tipos:
- conteo
- listado
- agregacion
- detalle

Pregunta: {pregunta}
Respuesta:
"""
    return llamar_ollama("mistral", prompt, 0.1, 20)

# ---------------------------
# GENERAR SQL
# ---------------------------
def generar_sql(pregunta, intencion):
    prompt = f"""
Genera SQL PostgreSQL.

Intención: {intencion}

Tablas:
clientes(id, nombre, ciudad, fecha_registro)
productos(id, nombre, precio, categoria)
pedidos(id, cliente_id, fecha)
detalle_pedido(id, pedido_id, producto_id, cantidad)

Reglas:
- SOLO SQL
- SOLO SELECT
- SIN texto extra

Pregunta: {pregunta}
SQL:
"""
    return limpiar_sql(llamar_ollama(MODELO_SQL, prompt))

# ---------------------------
# REINTENTO
# ---------------------------
def reintentar_sql(sql, error, pregunta):
    prompt = f"""
Corrige este SQL:

{sql}

Error:
{error}

Devuelve SOLO SQL válido.
"""
    nuevo_sql = limpiar_sql(llamar_ollama(MODELO_SQL, prompt))
    return ejecutar_sql(nuevo_sql)

# ---------------------------
# RESPUESTA NATURAL
# ---------------------------
def generar_respuesta(pregunta, resultado):
    prompt = f"""
Explica el resultado de forma clara:

Pregunta: {pregunta}
Resultado: {resultado}

Respuesta:
"""
    return llamar_ollama(MODELO_RESPUESTA, prompt, 0.4, 80)

# ---------------------------
# FORMATEO INTELIGENTE
# ---------------------------
def formatear_respuesta(pregunta, resultado):
    if isinstance(resultado, str):
        return "No pude procesar la consulta. Intenta reformularla."

    if not resultado:
        return "No se encontraron resultados."

    if len(resultado) == 1 and len(resultado[0]) == 1:
        valor = resultado[0][0]
        if "cuántos" in pregunta.lower():
            return f"Hay {valor} registros."
        return f"El resultado es {valor}."

    return generar_respuesta(pregunta, resultado)

# ---------------------------
# MAIN LOOP
# ---------------------------
if __name__ == "__main__":
    print(">>> SISTEMA LISTO\n")

    while True:
        try:
            pregunta = input("Pregunta (o 'salir'): ")

            if pregunta.lower() == "salir":
                break

            inicio = time.time()

            # 1. intención
            intencion = clasificar_intencion(pregunta)
            print("🧭 Intención:", intencion)

            # 2. SQL
            sql = generar_sql(pregunta, intencion)
            print("\n🧠 SQL:\n", sql)

            valido, msg = validar_sql(sql)
            if not valido:
                print("🚫", msg)
                continue

            # 3. ejecutar
            resultado = ejecutar_sql(sql)

            # 4. reintento si falla
            if isinstance(resultado, str):
                resultado = reintentar_sql(sql, resultado, pregunta)

            # 5. respuesta
            respuesta = formatear_respuesta(pregunta, resultado)

            # 6. guardar historial
            historial.append({
                "pregunta": pregunta,
                "sql": sql
            })

            fin = time.time()

            print("\n💬 Respuesta:\n", respuesta)
            print(f"\n⏱ Tiempo: {fin - inicio:.2f}s")

        except Exception as e:
            print("❌ Error:", e)