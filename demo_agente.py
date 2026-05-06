import psycopg2
import time

from agente_sql import procesar_pregunta
from db_setup import crear_esquema_y_cargar_datos

# ---------------------------
# CONFIG
# ---------------------------
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
# GENERAR SQL
# ---------------------------
def generar_sql(pregunta):
    respuesta = procesar_pregunta(pregunta)
    return respuesta

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

    resumen = crear_esquema_y_cargar_datos(get_conn)

    for tabla, total in resumen.items():
        print(f"  - {tabla}: {total}")

    print("✅ Datos empresariales cargados\n")

# ---------------------------
# PRUEBAS
# ---------------------------
def ejecutar_pruebas():
    preguntas = [
        "¿Cuántos clientes hay por segmento?",
        "Top 5 productos con mayores ventas",
        "Ventas totales por ciudad del cliente",
        "Margen total por categoria",
        "Pedidos por canal y estado",
        "Top 5 empleados por ventas",
        "Productos con stock por debajo del mínimo",
        "Métodos de pago con mayor monto vendido",
    ]

    print("🚀 INICIANDO DEMO\n")

    for i, p in enumerate(preguntas, 1):
        print(f"\n🧪 Prueba {i}")
        print(f"❓ Pregunta: {p}")

        inicio = time.time()

        respuesta_agente = generar_sql(p)
        sql = respuesta_agente["sql"]
        print("🔎 Origen:", respuesta_agente["origen"])
        print("🧠 SQL:", sql)

        if not respuesta_agente["ok"]:
            print("🚫 SQL inválido")
            print("❌ Error:", respuesta_agente.get("error"))
            continue

        resultado = respuesta_agente["resultado"]

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
