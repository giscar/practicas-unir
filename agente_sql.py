import psycopg2
import requests
import re
import signal

# ---------------------------
# CONFIG
# ---------------------------
URL_OLLAMA = "http://localhost:11434/api/generate"
MODELO_SQL = "llama3"
MAX_INTENTOS = 2

# ---------------------------
# TIMEOUT
# ---------------------------
def timeout_handler(signum, frame):
    raise Exception("Timeout en ejecución SQL")

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
# LLAMADA A OLLAMA
# ---------------------------
def llamar_ollama(prompt, temperature=0, max_tokens=40):
    try:
        response = requests.post(
            URL_OLLAMA,
            json={
                "model": MODELO_SQL,
                "prompt": prompt,
                "stream": False,
                "keep_alive": "5m",
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens
                }
            }
        )

        data = response.json()
        return data.get("response", "")

    except Exception as e:
        return f"error llm: {e}"

# ---------------------------
# LIMPIAR SQL
# ---------------------------
def limpiar_sql(sql):
    if not sql:
        return ""

    sql = re.sub(r"```sql|```", "", sql)

    match = re.search(r"(SELECT[\s\S]*?;)", sql, re.IGNORECASE)

    if match:
        return match.group(1).strip()

    return sql.strip().split("\n")[0]

# ---------------------------
# VALIDAR SQL
# ---------------------------
def validar_sql(sql):
    if not sql:
        return False

    s = sql.lower()

    if not s.startswith("select"):
        return False

    if "from" not in s:
        return False

    if any(x in s for x in ["drop", "delete", "update", "insert", "alter"]):
        return False

    return True

# ---------------------------
# CORRECCIÓN CON ERROR (LLM)
# ---------------------------
def corregir_sql_con_error(pregunta, sql_erroneo, error):
    prompt = f"""
Eres experto en SQL PostgreSQL.

El siguiente SQL tiene un error:

SQL:
{sql_erroneo}

Error:
{error}

Tablas:
clientes(id, nombre, ciudad, fecha_registro)
productos(id, nombre, precio, categoria)
pedidos(id, cliente_id, fecha)
detalle_pedido(id, pedido_id, producto_id, cantidad)

Relaciones:
pedidos.cliente_id = clientes.id
detalle_pedido.pedido_id = pedidos.id
detalle_pedido.producto_id = productos.id

Corrige el SQL.

REGLAS:
- SOLO SQL
- SIN explicación
- TERMINA en ;
"""

    respuesta = llamar_ollama(prompt)
    return limpiar_sql(respuesta)

# ---------------------------
# GENERAR SQL
# ---------------------------
def generar_sql(pregunta):
    prompt = f"""
SQL PostgreSQL.

SOLO SQL.
SIN explicación.
SIN texto adicional.
TERMINA con ;

Tablas:
clientes(id, nombre, ciudad, fecha_registro)
productos(id, nombre, precio, categoria)
pedidos(id, cliente_id, fecha)
detalle_pedido(id, pedido_id, producto_id, cantidad)

Relaciones:
pedidos.cliente_id = clientes.id
detalle_pedido.pedido_id = pedidos.id
detalle_pedido.producto_id = productos.id

IMPORTANTE:
Si usas productos.nombre debes hacer JOIN con productos.

Pregunta: {pregunta}
"""

    respuesta = llamar_ollama(prompt)
    return limpiar_sql(respuesta)

# ---------------------------
# FUNCIÓN PRINCIPAL DEL AGENTE
# ---------------------------
def procesar_pregunta(pregunta):
    sql = generar_sql(pregunta)

    if not validar_sql(sql):
        return {
            "ok": False,
            "error": "SQL inválido",
            "sql": sql
        }

    sql_actual = sql
    resultado = None
    error = None

    for intento in range(MAX_INTENTOS):

        try:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(5)

            resultado = ejecutar_sql(sql_actual)

            signal.alarm(0)

            if isinstance(resultado, str) and "error sql" in resultado:
                raise Exception(resultado)

            return {
                "ok": True,
                "sql": sql_actual,
                "resultado": resultado
            }

        except Exception as e:
            error = str(e)

            if intento < MAX_INTENTOS - 1:
                sql_actual = corregir_sql_con_error(
                    pregunta,
                    sql_actual,
                    error
                )
            else:
                return {
                    "ok": False,
                    "error": error,
                    "sql": sql_actual
                }