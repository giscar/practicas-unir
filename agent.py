import psycopg2
import requests
import re

# ---------------------------
# Conexión a PostgreSQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="empresa",
    user="admin",
    password="admin"
)

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
    sql = sql.strip()
    return sql

# ---------------------------
# Validar SQL (seguridad básica)
# ---------------------------
def es_sql_valido(sql):
    peligrosos = ["DROP", "DELETE", "UPDATE", "INSERT"]
    return not any(p in sql.upper() for p in peligrosos)

def formatear_respuesta(pregunta, resultado):
    try:
        if "cuántos" in pregunta.lower() or "cuantos" in pregunta.lower():
            valor = resultado[0][0]
            return f"El resultado es: {valor}"
        
        return str(resultado)
    
    except:
        return "No se pudo interpretar el resultado"

# ---------------------------
# LLM → generar SQL
# ---------------------------
def generar_sql(pregunta):
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

Reglas:
- Solo devuelve SQL válido
- No expliques nada
- Usa JOIN cuando sea necesario
- No uses texto adicional

Pregunta: {pregunta}

SQL:
"""

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "llama3",
            "prompt": prompt,
            "stream": False
        }
    )

    return response.json()["response"]

# ---------------------------
# Flujo completo
# ---------------------------
def preguntar(pregunta):
    sql = limpiar_sql(generar_sql(pregunta))

    print("\nSQL generado:\n", sql)

    if not es_sql_valido(sql):
        return "Consulta bloqueada por seguridad"

    resultado = ejecutar_sql(sql)
    return formatear_respuesta(pregunta, resultado)

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    while True:
        pregunta = input("\nPregunta (o 'salir'): ")

        if pregunta.lower() == "salir":
            break

        respuesta = preguntar(pregunta)
        print("\nResultado:\n", respuesta)
