import psycopg2
import requests
import time
import re

# ---------------------------
# CONFIG
# ---------------------------
URL_OLLAMA = "http://localhost:11434/api/generate"
MODELO_SQL = "mistral"
MODELO_RESPUESTA = "llama3"

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
# OLLAMA SAFE
# ---------------------------
def ollama(modelo, prompt, temp=0.2, tokens=120):
    try:
        r = requests.post(URL_OLLAMA, json={
            "model": modelo,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": temp,
                "num_predict": tokens
            }
        })
        data = r.json()
        return data.get("response", f"error llm: {data}")
    except Exception as e:
        return f"error llm: {e}"

# ---------------------------
# LIMPIAR SQL (ROBUSTO)
# ---------------------------
def limpiar_sql(sql):
    if not sql:
        return ""

    sql = re.sub(r"```sql|```", "", sql)

    match = re.search(r"(SELECT[\s\S]*?;)", sql, re.IGNORECASE)

    if match:
        sql = match.group(1)
    else:
        sql = sql.split("\n")[0]

    return sql.strip()

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
# GENERAR SQL
# ---------------------------
def generar_sql(pregunta):
    prompt = f"""
Eres un generador de SQL.

Reglas estrictas:
- SOLO SQL
- SIN explicación
- TERMINA con ;

Tablas:
clientes(id, nombre, ciudad, fecha_registro)
productos(id, nombre, precio, categoria)
pedidos(id, cliente_id, fecha)
detalle_pedido(id, pedido_id, producto_id, cantidad)

Pregunta: {pregunta}
"""

    respuesta = ollama(MODELO_SQL, prompt)

    return limpiar_sql(respuesta)

# ---------------------------
# REINTENTO AUTOMÁTICO
# ---------------------------
def ejecutar_con_reintento(sql, pregunta):
    resultado = ejecutar_sql(sql)

    if isinstance(resultado, str):
        print("⚠️ Corrigiendo SQL...")

        prompt = f"""
Corrige este SQL. SOLO devuelve SQL válido:

{sql}

Error:
{resultado}
"""

        nuevo_sql = limpiar_sql(ollama(MODELO_SQL, prompt))

        print("🔁 SQL corregido:", nuevo_sql)

        if validar_sql(nuevo_sql):
            return ejecutar_sql(nuevo_sql)

    return resultado

# ---------------------------
# RESPUESTA NATURAL (fallback)
# ---------------------------
def respuesta_natural(pregunta, resultado):
    prompt = f"""
Explica el resultado de forma clara.

Pregunta: {pregunta}
Resultado: {resultado}

Respuesta:
"""
    return ollama(MODELO_RESPUESTA, prompt, 0.4, 80)

# ---------------------------
# FORMATEO INTELIGENTE
# ---------------------------
def formatear(pregunta, resultado):
    if isinstance(resultado, str):
        return "No pude procesar la consulta correctamente."

    if not resultado:
        return "No hay resultados."

    # resultado simple
    if len(resultado) == 1 and len(resultado[0]) == 1:
        return f"Resultado: {resultado[0][0]}"

    # mostrar resultados reales
    filas = "\n".join([str(r) for r in resultado[:5]])

    return f"Resultados:\n{filas}"

# ---------------------------
# CARGA DE DATOS
# ---------------------------
def cargar_datos():
    print("\n📦 Cargando datos...")

    sql = """
    DELETE FROM detalle_pedido;
    DELETE FROM pedidos;
    DELETE FROM productos;
    DELETE FROM clientes;

    INSERT INTO clientes VALUES
    (1,'Juan Perez','Lima','2024-01-10'),
    (2,'Maria Lopez','Arequipa','2024-02-15'),
    (3,'Carlos Ruiz','Lima','2024-03-01'),
    (4,'Ana Torres','Cusco','2024-03-20'),
    (5,'Luis Garcia','Lima','2024-04-05');

    INSERT INTO productos VALUES
    (1,'Laptop Lenovo',3500,'tecnologia'),
    (2,'Mouse Logitech',80,'tecnologia'),
    (3,'Teclado Mecánico',250,'tecnologia'),
    (4,'Silla Gamer',900,'muebles'),
    (5,'Escritorio',700,'muebles'),
    (6,'Monitor LG',1200,'tecnologia');

    INSERT INTO pedidos VALUES
    (1,1,'2024-05-01'),
    (2,2,'2024-05-03'),
    (3,1,'2024-05-05'),
    (4,3,'2024-05-10'),
    (5,4,'2024-05-12'),
    (6,5,'2024-05-15');

    INSERT INTO detalle_pedido VALUES
    (1,1,1,1),
    (2,1,2,2),
    (3,2,3,1),
    (4,3,4,1),
    (5,3,5,1),
    (6,4,1,1),
    (7,4,6,1),
    (8,5,2,3),
    (9,6,3,2);
    """

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Datos cargados\n")

# ---------------------------
# PRUEBAS
# ---------------------------
def ejecutar_pruebas():
    preguntas = [
        "¿Cuántos productos hay?",
        "¿Cuántos clientes hay en Lima?",
        "Lista todos los productos",
        "¿Cuál es el producto más caro?",
        "¿Qué productos ha comprado Juan Perez?",
        "¿Qué cliente ha comprado más productos?",
        "¿Cuál es la categoría más vendida?",
        "ventas por cliente",
        "dame los empleados"
    ]

    print("🚀 INICIANDO DEMO\n")

    for i, p in enumerate(preguntas, 1):
        print(f"\n🧪 Prueba {i}")
        print(f"❓ Pregunta: {p}")

        inicio = time.time()

        sql = generar_sql(p)
        print("🧠 SQL:", sql)

        if not validar_sql(sql):
            print("🚫 SQL inválido")
            continue

        resultado = ejecutar_con_reintento(sql, p)

        respuesta = formatear(p, resultado)

        fin = time.time()

        print("💬 Respuesta:", respuesta)
        print(f"⏱ Tiempo: {fin - inicio:.2f}s")

    print("\n🎯 DEMO FINALIZADA")

# ---------------------------
# MAIN
# ---------------------------
if __name__ == "__main__":
    cargar_datos()
    ejecutar_pruebas()