import psycopg2
import requests
import re
import time
import traceback

print(">>> SISTEMA DINÁMICO OPTIMIZADO")

# ---------------------------
# CONFIG
# ---------------------------
URL_OLLAMA = "http://localhost:11434/api/generate"
MODELO_SQL = "llama3"

cache_sql = {}

# ---------------------------
# CONEXIÓN BD
# ---------------------------
def get_conn():
    return psycopg2.connect(
        host="localhost",
        database="empresa",
        user="admin",
        password="admin"
    )

# ---------------------------
# EJECUTAR SQL
# ---------------------------
def ejecutar_sql(query):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    except Exception as e:
        return f"error sql: {e}"

# ---------------------------
# OLLAMA
# ---------------------------
def llamar_ollama(prompt, temperature=0, max_tokens=25):
    response = requests.post(
        URL_OLLAMA,
        json={
            "model": MODELO_SQL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens
            }
        }
    )
    return response.json().get("response", "")

# ---------------------------
# LIMPIAR SQL
# ---------------------------
def limpiar_sql(sql):
    sql = re.sub(r"```sql|```", "", sql)

    match = re.search(r"(SELECT[\s\S]*?;)", sql, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    return sql.split("\n")[0].strip()

# ---------------------------
# VALIDAR SQL BÁSICO
# ---------------------------
def validar_sql(sql):
    s = sql.lower()

    if not s.startswith("select"):
        return False

    if "from" not in s:
        return False

    if any(x in s for x in ["drop", "delete", "update", "insert", "alter"]):
        return False

    return True

# ---------------------------
# GENERAR SQL (CON CACHE)
# ---------------------------
def generar_sql(pregunta):

    if pregunta in cache_sql:
        return cache_sql[pregunta]

    prompt = f"""
SQL PostgreSQL.

SOLO SQL.
SIN texto.
SIN explicación.
TERMINA con ;

IMPORTANTE:
- Usa JOINs correctos
- Incluye TODAS las tablas necesarias
- NO omitas joins

Esquema:
clientes(id,nombre,ciudad,fecha_registro)
productos(id,nombre,precio,categoria)
pedidos(id,cliente_id,fecha)
detalle_pedido(id,pedido_id,producto_id,cantidad)

Relaciones:
pedidos.cliente_id=clientes.id
detalle_pedido.pedido_id=pedidos.id
detalle_pedido.producto_id=productos.id

Pregunta: {pregunta}
"""

    sql = limpiar_sql(llamar_ollama(prompt))

    cache_sql[pregunta] = sql

    return sql

# ---------------------------
# REGENERAR SQL (SIN CACHE)
# ---------------------------
def regenerar_sql(pregunta):
    prompt = f"""
SQL PostgreSQL.

SOLO SQL.
SIN explicación.
SIN texto adicional.
TERMINA con ;

IMPORTANTE:
- Usa JOINs correctos
- Incluye TODAS las tablas necesarias
- NO omitas joins

Pregunta: {pregunta}
"""

    return limpiar_sql(llamar_ollama(prompt))

# ---------------------------
# FORMATEAR RESPUESTA
# ---------------------------
def formatear(resultado):
    if isinstance(resultado, str):
        return "❌ Error en consulta"

    if not resultado:
        return "Sin resultados"

    if len(resultado) == 1 and len(resultado[0]) == 1:
        return f"Resultado: {resultado[0][0]}"

    return "\n".join(str(r) for r in resultado[:5])

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":

    while True:
        try:
            pregunta = input("\n❓ Pregunta: ")

            if pregunta.lower() == "salir":
                break

            inicio = time.time()

            # 1. generar SQL
            sql = generar_sql(pregunta)

            print("\n🧠 SQL generado:")
            print("=" * 50)
            print(sql)
            print("=" * 50)

            # 2. validar
            if not validar_sql(sql):
                print("❌ SQL inválido")
                continue

            # 3. ejecutar
            resultado = ejecutar_sql(sql)

            # 🔥 4. REGENERACIÓN (NO CORRECCIÓN)
            if isinstance(resultado, str):
                print("⚠️ Reintentando generación SQL...")

                sql = regenerar_sql(pregunta)

                print("\n🔁 SQL nuevo:")
                print("=" * 50)
                print(sql)
                print("=" * 50)

                resultado = ejecutar_sql(sql)

            # 5. respuesta
            print("\n💬 Resultado:")
            print(formatear(resultado))

            print(f"\n⏱ Tiempo: {time.time() - inicio:.2f}s")

        except Exception as e:
            print("❌ Error:", e)
            traceback.print_exc()