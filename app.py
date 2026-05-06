import time
from datetime import datetime
from decimal import Decimal

import pandas as pd
import streamlit as st

from agente_sql import calentar_modelo, ejecutar_sql, get_conn, guardar_correccion_usuario, procesar_pregunta
from db_setup import crear_esquema_y_cargar_datos


st.set_page_config(
    page_title="Agente Analítico SQL",
    layout="wide",
    page_icon="🤖"
)

PREGUNTAS_SUGERIDAS = [
    "¿Cuántos clientes hay por segmento?",
    "Top 5 productos con mayores ventas",
    "Ventas totales por ciudad del cliente",
    "Margen total por categoria",
    "Pedidos por canal y estado",
    "Top 5 empleados por ventas",
    "Productos con stock por debajo del mínimo",
    "Métodos de pago con mayor monto vendido",
]

ETIQUETAS_COLUMNAS = {
    "canal": "Canal de venta",
    "estado": "Estado del pedido",
    "conteo": "Cantidad de pedidos",
    "cantidad_pedidos": "Cantidad de pedidos",
    "pedidosporcanalyestado": "Cantidad de pedidos",
    "count": "Cantidad",
    "total_clientes": "Total de clientes",
    "cantidad_clientes": "Cantidad de clientes",
    "segmento": "Segmento de cliente",
    "producto": "Producto",
    "nombre": "Nombre",
    "empleado": "Empleado",
    "ventas": "Ventas",
    "ventas_totales": "Ventas totales",
    "total_ventas": "Ventas totales",
    "ciudad": "Ciudad",
    "categoria": "Categoría",
    "margen": "Margen",
    "costo_total": "Costo total",
    "stock_actual": "Stock actual",
    "stock_minimo": "Stock mínimo",
    "metodo": "Método de pago",
    "total_monto": "Monto total",
    "monto_total": "Monto total",
    "total": "Total",
    "cantidad": "Cantidad",
    "valor": "Valor",
    "promedio": "Promedio",
    "precio_unitario": "Precio unitario",
    "descuento": "Descuento",
    "fecha_pedido": "Fecha del pedido",
    "fecha": "Fecha",
}


def etiqueta_columna(columna):
    clave = str(columna).strip().lower()
    if clave in ETIQUETAS_COLUMNAS:
        return ETIQUETAS_COLUMNAS[clave]

    return clave.replace("_", " ").strip().capitalize()


def formatear_valor(valor, moneda=False):
    if valor is None or valor == "N/D":
        return "N/D"

    if isinstance(valor, Decimal):
        valor = float(valor)

    if isinstance(valor, int):
        return f"{valor:,}"

    if isinstance(valor, float):
        numero = f"{valor:,.2f}"
        return f"$ {numero}" if moneda else numero

    return str(valor)


def normalizar_dataframe(df):
    for columna in df.columns:
        contiene_decimal = df[columna].map(lambda valor: isinstance(valor, Decimal)).any()
        if contiene_decimal:
            df[columna] = df[columna].astype(float)

    return df


def cargar_kpis():
    consultas = {
        "Clientes": "SELECT COUNT(*) FROM clientes;",
        "Productos": "SELECT COUNT(*) FROM productos;",
        "Pedidos": "SELECT COUNT(*) FROM pedidos;",
        "Ventas": """
            SELECT COALESCE(ROUND(SUM(dp.cantidad * dp.precio_unitario * (1 - dp.descuento)), 2), 0)
            FROM detalle_pedido dp
            JOIN pedidos p ON p.id = dp.pedido_id
            WHERE p.estado <> 'cancelado';
        """,
    }

    kpis = {}
    for nombre, sql in consultas.items():
        resultado = ejecutar_sql(sql)
        if isinstance(resultado, str) or not resultado["filas"]:
            kpis[nombre] = "N/D"
        else:
            kpis[nombre] = resultado["filas"][0][0]

    return kpis


def construir_dataframe(resultado, columnas):
    if not resultado:
        return pd.DataFrame()

    if columnas and len(columnas) == len(resultado[0]):
        df = pd.DataFrame(resultado, columns=[etiqueta_columna(c) for c in columnas])
        return normalizar_dataframe(df)

    columnas_genericas = [f"columna_{i + 1}" for i in range(len(resultado[0]))]
    df = pd.DataFrame(resultado, columns=columnas_genericas)
    return normalizar_dataframe(df)


def generar_resumen_ejecutivo(df):
    if df.empty:
        return "No se encontraron registros para esta consulta."

    total_filas = len(df)
    numericas = df.select_dtypes(include="number").columns.tolist()

    if len(df.columns) == 1:
        return f"Encontré {total_filas} resultado(s) para la consulta."

    if numericas:
        metrica = numericas[0]
        dimension = next((col for col in df.columns if col != metrica), df.columns[0])
        ordenado = df.sort_values(by=metrica, ascending=False)
        lider = ordenado.iloc[0]

        return (
            f"Encontré {total_filas} registros. "
            f"El mayor valor de {metrica.lower()} corresponde a "
            f"{dimension.lower()} '{lider[dimension]}' con {formatear_valor(lider[metrica])}."
        )

    return f"Encontré {total_filas} registros agrupados para tu consulta."


def mostrar_visualizacion(df):
    if df.empty or len(df.columns) < 2:
        return

    primera = df.columns[0]
    numericas = df.select_dtypes(include="number").columns.tolist()

    if not numericas:
        return

    metrica = numericas[0]
    chart_df = df[[primera, metrica]].copy()
    chart_df[primera] = chart_df[primera].astype(str)

    st.bar_chart(chart_df.set_index(primera), height=320)


def mostrar_resultado(item, key):
    resultado = item["resultado"]
    columnas = item.get("columnas", [])

    if isinstance(resultado, str):
        st.error(resultado)
        return

    if not resultado:
        st.info("Sin resultados")
        return

    if len(resultado) == 1 and len(resultado[0]) == 1:
        st.metric("Resultado", formatear_valor(resultado[0][0]))
        return

    df = construir_dataframe(resultado, columnas)
    st.info(generar_resumen_ejecutivo(df))

    tab_tabla, tab_grafico = st.tabs(["Tabla", "Gráfico"])

    with tab_tabla:
        st.dataframe(df, use_container_width=True, hide_index=True)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name=f"consulta_{key}.csv",
            mime="text/csv",
            key=f"download_{key}"
        )

    with tab_grafico:
        mostrar_visualizacion(df)


def mostrar_aprendizaje_supervisado(item, key):
    st.caption("Si la interpretación no fue correcta, guarda el SQL correcto para esta pregunta.")
    sql_corregido = st.text_area(
        "SQL corregido",
        value=item.get("sql", ""),
        height=180,
        key=f"sql_corregido_{key}"
    )

    if st.button("Guardar corrección", key=f"guardar_correccion_{key}"):
        resultado = guardar_correccion_usuario(item["pregunta"], sql_corregido)

        if resultado["ok"]:
            st.success("Corrección guardada. La próxima ejecución usará esta consulta validada.")
            st.code(resultado["sql"], language="sql")
        else:
            st.error("No se pudo guardar la corrección")
            st.caption(resultado["error"])
            st.code(resultado.get("sql", sql_corregido), language="sql")


def mostrar_estado_agente(item):
    tiempos = item.get("tiempos", {})
    validaciones = item.get("validaciones", [])

    c1, c2, c3 = st.columns(3)
    c1.metric("Generación SQL", f"{tiempos.get('generacion_sql', 0)} s")
    c2.metric("Validación", f"{tiempos.get('validacion', 0)} s")
    c3.metric("PostgreSQL", f"{tiempos.get('ejecucion_bd', 0)} s")

    if validaciones:
        st.caption(" · ".join(f"✓ {v}" for v in validaciones))


def nombre_origen(origen):
    origen = origen or "IA"

    if "corrección" in origen.lower():
        return "Corrección supervisada"

    if "cache" in origen or "memoria" in origen:
        return "Memoria validada del agente"

    if "IA" in origen:
        return "Generado por IA local"

    return origen


if "historial" not in st.session_state:
    st.session_state.historial = []

if "pregunta_actual" not in st.session_state:
    st.session_state.pregunta_actual = ""

if "modelo_calentado" not in st.session_state:
    st.session_state.modelo_calentado = False


st.title("🤖 Agente Analítico Empresarial")
st.caption("Lenguaje natural → SQL dinámico → validación segura → PostgreSQL")

if not st.session_state.modelo_calentado:
    with st.spinner("Preparando modelo local..."):
        calentar_modelo()
        st.session_state.modelo_calentado = True

with st.container():
    kpis = cargar_kpis()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Clientes", formatear_valor(kpis["Clientes"]))
    k2.metric("Productos", formatear_valor(kpis["Productos"]))
    k3.metric("Pedidos", formatear_valor(kpis["Pedidos"]))
    k4.metric("Ventas", formatear_valor(kpis["Ventas"], moneda=True))

st.markdown("### Preguntas sugeridas")
cols = st.columns(4)
for idx, sugerida in enumerate(PREGUNTAS_SUGERIDAS):
    if cols[idx % 4].button(sugerida, key=f"sugerida_{idx}"):
        st.session_state.pregunta_actual = sugerida

st.markdown("### Consulta")
col_pregunta, col_consultar, col_limpiar, col_cargar = st.columns([6, 1, 1, 1])

with col_pregunta:
    pregunta = st.text_input(
        "Escribe tu pregunta",
        key="pregunta_actual",
        label_visibility="collapsed",
        placeholder="Ejemplo: ventas totales por ciudad del cliente"
    )

with col_consultar:
    ejecutar = st.button("Consultar", use_container_width=True)

with col_limpiar:
    limpiar = st.button("Nueva conversación", use_container_width=True)

with col_cargar:
    cargar_demo = st.button("Cargar demo", use_container_width=True)

if limpiar:
    st.session_state.historial = []
    st.rerun()

if cargar_demo:
    with st.spinner("Creando modelo empresarial y cargando datos..."):
        resumen = crear_esquema_y_cargar_datos(get_conn)
        st.session_state.historial = []
        st.success(
            "Datos demo cargados: "
            + ", ".join(f"{tabla}: {total}" for tabla, total in resumen.items())
        )

if ejecutar:
    if not pregunta.strip():
        st.warning("Ingresa una pregunta")
    else:
        inicio = time.time()

        with st.status("Agente trabajando...", expanded=True) as status:
            st.write("Seleccionando esquema relevante")
            st.write("Generando SQL con modelo local")
            st.write("Validando seguridad, tablas y columnas")
            st.write("Ejecutando consulta en PostgreSQL")

            respuesta = procesar_pregunta(pregunta)

            if respuesta["ok"]:
                status.update(label="Consulta ejecutada", state="complete", expanded=False)
            else:
                status.update(label="No se pudo resolver la consulta", state="error", expanded=True)

        if not respuesta["ok"]:
            st.error("No se pudo resolver la consulta")
            st.code(respuesta.get("sql", ""), language="sql")
            st.caption(respuesta.get("error", "Error desconocido"))
            st.session_state.historial.append({
                "pregunta": pregunta,
                "sql": respuesta.get("sql", ""),
                "origen": respuesta.get("origen", "IA"),
                "resultado": respuesta.get("resultado", []),
                "columnas": respuesta.get("columnas", []),
                "tiempos": respuesta.get("tiempos", {}),
                "validaciones": respuesta.get("validaciones", []),
                "tiempo": round(time.time() - inicio, 2),
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "error": respuesta.get("error", "Error desconocido")
            })
        else:
            st.session_state.historial.append({
                "pregunta": pregunta,
                "sql": respuesta["sql"],
                "origen": respuesta["origen"],
                "resultado": respuesta["resultado"],
                "columnas": respuesta.get("columnas", []),
                "tiempos": respuesta.get("tiempos", {}),
                "validaciones": respuesta.get("validaciones", []),
                "tiempo": round(time.time() - inicio, 2),
                "timestamp": datetime.now().strftime("%H:%M:%S")
            })
            st.success("Consulta ejecutada")

st.markdown("---")
st.markdown("### Historial de consultas")

if not st.session_state.historial:
    st.info("Aún no hay consultas")
else:
    for i, item in enumerate(reversed(st.session_state.historial)):
        st.markdown("---")

        with st.chat_message("user"):
            st.markdown(item["pregunta"])
            st.caption(item["timestamp"])

        with st.chat_message("assistant"):
            col_info, col_tiempo = st.columns([4, 1])

            with col_info:
                st.markdown("Respuesta del agente")
                st.caption(f"Fuente: {nombre_origen(item.get('origen'))}")

            with col_tiempo:
                st.metric("Tiempo total", f"{item['tiempo']} s")

            mostrar_estado_agente(item)
            if item.get("error"):
                st.error(item["error"])
            else:
                mostrar_resultado(item, i)

            with st.expander("Ver detalle técnico"):
                st.caption(f"Origen técnico: {item.get('origen', 'IA')}")
                st.code(item["sql"], language="sql")

            with st.expander("Corregir interpretación"):
                mostrar_aprendizaje_supervisado(item, i)
