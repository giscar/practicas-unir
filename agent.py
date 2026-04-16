import psycopg2
import requests
import re
import subprocess
import time

# ---------------------------
# (OPCIONAL) Iniciar Ollama automáticamente
# ---------------------------
def iniciar_ollama():
    try:
        subprocess.Popen(["ollama", "serve"])
        time.sleep(2)
    except:
        pass

# iniciar_ollama()  # 👉 descomenta si quieres auto-start

# ---------------------------
# Conexión a PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="empresa",
    user="admin",
    password="admin"
)

# ---------------------------
# Ejecutar SQL
# ---------------------------
def ejecutar_sql(query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        resultados = cursor.fetchall()
        cursor.close()
        return resultados
    except Exception as e:
        return f"Error SQL: {e}"

# ---------------------------
# Limpiar SQL generado
# ---------------------------
def limpiar_sql(sql):
    sql = re.sub(r"```sql|```", "", sql)
    return sql.strip()

# ---------------------------
# Validar SQL (seguridad)
# ---------------------------
def es_sql_valido(sql):
    peligrosos = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]
    if any(p in sql.upper() for p in peligrosos):
        return False

    if not sql.lower().startswith("select"):
        return False

    return True

# ---------------------------
# Formatear respuesta
# ---------------------------
def formatear_respuesta(pregunta, resultado):
    try:
        if isinstance(resultado, str):
            return resultado

        if not resultado:
            return "No se encontraron resultados"

        if "cuántos" in pregunta.lower() or "cuantos" in pregunta.lower():
            return f"El resultado es: {resultado[0][0]}"

        return "\n".join([str(fila) for fila in resultado])

    except:
        return "No se pudo interpretar el resultado"

# ---------------------------
# Generar SQL con LLM
# ---------------------------
def generar_sql(pregunta, historial):
    contexto = "\n".join(historial[-3:])  # memoria básica

    prompt = f"""
Eres un experto en SQL para PostgreSQL.

Base de datos:
- clientes(id, nombre, ciudad, fecha_registro)
- productos(id, nombre, precio, categoria)
- pedidos(id, cliente_id, fecha)
- detalle_pedido(id, pedido_id, producto_id, cantidad)

Relaciones:
- pedidos.cliente_id = clientes.id
- detalle_pedido.pedido_id = pedidos.id
- detalle_pedido.producto_id = productos.id

Historial:
{contexto}

Reglas estrictas:
- SOLO devuelve SQL
- NO expliques nada
- NO uses ``` ni texto adicional
- Solo consultas SELECT
- Usa JOIN cuando sea necesario
- Si no aplica, responde: SELECT 'No aplica';

Ejemplo:
Pregunta: total de pedidos
SQL: SELECT COUNT(*) FROM pedidos;

Pregunta: {pregunta}
SQL:
"""

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "mistral",
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.2,
                    "top_p": 0.9,
                    "num_ctx": 4096
                }
            }
        )

        return response.json()["response"]

    except Exception as e:
        return f"Error LLM: {e}"

# ---------------------------
# Flujo completo
# ---------------------------
def preguntar(pregunta, historial):
    sql = limpiar_sql(generar_sql(pregunta, historial))

    print("\nSQL generado:\n", sql)

    if not es_sql_valido(sql):
        return "Consulta bloqueada por seguridad"

    resultado = ejecutar_sql(sql)

    respuesta = formatear_respuesta(pregunta, resultado)

    # guardar memoria
    historial.append(f"Usuario: {pregunta}")
    historial.append(f"SQL: {sql}")

    return respuesta

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    historial = []

    print("=== AGENTE CONVERSACIONAL SQL (OLLAMA + POSTGRES) ===")

    while True:
        pregunta = input("\nPregunta (o 'salir'): ")

        if pregunta.lower() == "salir":
            break

        respuesta = preguntar(pregunta, historial)
        print("\nResultado:\n", respuesta)