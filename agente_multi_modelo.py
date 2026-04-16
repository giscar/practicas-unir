import psycopg2
import requests
import re
import sys

print(">>> INICIO DEL SCRIPT")

# ---------------------------
# MODELO
# ---------------------------
MODELO = sys.argv[1] if len(sys.argv) > 1 else "mistral"
print(f">>> Modelo: {MODELO}")

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
# LIMPIAR SQL
# ---------------------------
def limpiar_sql(sql):
    sql = re.sub(r"```sql|```", "", sql).strip()

    if sql.startswith("'") and sql.endswith("'"):
        sql = sql[1:-1]

    if sql.startswith('"') and sql.endswith('"'):
        sql = sql[1:-1]

    return sql.strip()

# ---------------------------
# VALIDAR SQL
# ---------------------------
def es_sql_valido(sql):
    sql = sql.strip().lower()

    if not sql.startswith("select"):
        return False

    peligrosos = ["drop", "delete", "update", "insert", "alter"]
    if any(p in sql for p in peligrosos):
        return False

    return True

# ---------------------------
# GENERAR SQL (LLM)
# ---------------------------
def generar_sql(pregunta):
    prompt = f"""
Genera una consulta SQL SELECT válida para PostgreSQL.

Tablas:
clientes(id, nombre, ciudad, fecha_registro)
productos(id, nombre, precio, categoria)
pedidos(id, cliente_id, fecha)
detalle_pedido(id, pedido_id, producto_id, cantidad)

Relaciones:
- pedidos.cliente_id = clientes.id
- detalle_pedido.pedido_id = pedidos.id
- detalle_pedido.producto_id = productos.id

Reglas:
- SOLO SQL
- SOLO SELECT
- SIN explicaciones
- SIN comillas
- SIN ``` 

Pregunta: {pregunta}
SQL:
"""

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": MODELO,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.2,
                    "num_ctx": 2048,
                    "num_predict": 120
                }
            }
        )

        return response.json()["response"]

    except Exception as e:
        return f"error llm: {e}"

# ---------------------------
# REINTENTO AUTOMÁTICO
# ---------------------------
def ejecutar_con_reintento(sql, pregunta):
    resultado = ejecutar_sql(sql)

    if isinstance(resultado, str) and "error" in resultado.lower():
        print("\n⚠️ Error detectado. Reintentando...")

        prompt = f"""
La siguiente consulta SQL tiene un error:

{sql}

Error:
{resultado}

Corrige la consulta usando SOLO columnas existentes.

Tablas:
clientes(id, nombre, ciudad, fecha_registro)
productos(id, nombre, precio, categoria)
pedidos(id, cliente_id, fecha)
detalle_pedido(id, pedido_id, producto_id, cantidad)

Pregunta original:
{pregunta}

SQL corregido:
"""

        try:
            response = requests.post(
                "http://localhost:11434/api/generate",
                json={
                    "model": MODELO,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.2
                    }
                }
            )

            sql_corregido = limpiar_sql(response.json()["response"])
            print("\n🔁 SQL corregido:\n", sql_corregido)

            return ejecutar_sql(sql_corregido)

        except Exception as e:
            return f"error reintento: {e}"

    return resultado

# ---------------------------
# RESPUESTA NATURAL
# ---------------------------
def generar_respuesta_natural(pregunta, resultado):
    prompt = f"""
Eres un asistente que explica resultados de base de datos.

Reglas:
- Responde en lenguaje natural
- Sé breve
- No muestres SQL

Pregunta: {pregunta}
Resultado: {resultado}

Respuesta:
"""

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": MODELO,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.4,
                    "num_predict": 80
                }
            }
        )

        return response.json()["response"]

    except Exception as e:
        return f"error respuesta: {e}"

# ---------------------------
# RESPUESTA INTELIGENTE
# ---------------------------
def formatear_inteligente(pregunta, resultado):
    if isinstance(resultado, str):
        return "Hubo un problema al procesar la consulta."

    if not resultado:
        return "No se encontraron resultados."

    if len(resultado) == 1 and len(resultado[0]) == 1:
        valor = resultado[0][0]

        if "cuántos" in pregunta.lower() or "cuantos" in pregunta.lower():
            return f"Hay {valor} registros."

        return f"El resultado es {valor}."

    return generar_respuesta_natural(pregunta, resultado)

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    print(">>> SISTEMA LISTO\n")

    while True:
        try:
            pregunta = input("Pregunta (o 'salir'): ")

            if pregunta.lower() == "salir":
                print("👋 Saliendo...")
                break

            # 1. generar SQL
            sql = limpiar_sql(generar_sql(pregunta))
            print("\n🧠 SQL generado:\n", sql)

            # 2. validar
            if not es_sql_valido(sql):
                print("\n🚫 SQL inválido")
                continue

            # 3. ejecutar con reintento
            resultado = ejecutar_con_reintento(sql, pregunta)

            # 4. respuesta natural
            respuesta = formatear_inteligente(pregunta, resultado)

            print("\n💬 Respuesta:\n", respuesta)

        except KeyboardInterrupt:
            print("\n👋 Interrumpido")
            break

        except Exception as e:
            print("\n❌ Error inesperado:", e)