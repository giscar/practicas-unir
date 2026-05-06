import streamlit as st
import pandas as pd
import time
from datetime import datetime

# IMPORTA TU AGENTE
from agente_sql import get_conn, procesar_pregunta
from db_setup import crear_esquema_y_cargar_datos

# ---------------------------
# CONFIGURACIÓN
# ---------------------------
st.set_page_config(
    page_title="Agente SQL Inteligente",
    layout="wide",
    page_icon="🤖"
)

st.title("🤖 Agente Conversacional SQL")
st.markdown("Consulta tu base de datos en lenguaje natural")

# ---------------------------
# ESTADO
# ---------------------------
if "historial" not in st.session_state:
    st.session_state.historial = []


def mostrar_resultado(resultado, key):
    if isinstance(resultado, str):
        st.error(resultado)
        return

    if not resultado:
        st.info("Sin resultados")
        return

    if len(resultado) == 1 and len(resultado[0]) == 1:
        st.metric("Resultado", resultado[0][0])
        return

    columnas = [f"columna_{i + 1}" for i in range(len(resultado[0]))]
    df = pd.DataFrame(resultado, columns=columnas)

    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="📥 Descargar resultados (CSV)",
        data=csv,
        file_name=f"consulta_{key}.csv",
        mime="text/csv",
        key=f"download_{key}"
    )

# ---------------------------
# INPUT
# ---------------------------
with st.container():
    col1, col2, col3, col4 = st.columns([4, 1, 1, 1])

    with col1:
        pregunta = st.text_input("❓ Escribe tu pregunta")

    with col2:
        ejecutar = st.button("Consultar")

    with col3:
        limpiar = st.button("Limpiar historial")

    with col4:
        cargar_demo = st.button("Cargar datos demo")

if limpiar:
    st.session_state.historial = []
    st.rerun()

if cargar_demo:
    with st.spinner("📦 Creando modelo empresarial y cargando datos..."):
        resumen = crear_esquema_y_cargar_datos(get_conn)
        st.session_state.historial = []
        st.success(
            "✅ Datos demo cargados: "
            + ", ".join(f"{tabla}: {total}" for tabla, total in resumen.items())
        )

# ---------------------------
# PROCESAMIENTO
# ---------------------------
if ejecutar:

    if not pregunta.strip():
        st.warning("⚠️ Ingresa una pregunta")
    else:
        with st.spinner("🧠 Generando y ejecutando consulta..."):

            inicio = time.time()

            try:
                respuesta = procesar_pregunta(pregunta)

                if not respuesta["ok"]:
                    st.error("❌ No se pudo resolver la consulta")
                    st.code(respuesta["sql"], language="sql")
                    st.caption(respuesta.get("error", "Error desconocido"))
                else:
                    fin = time.time()

                    # Guardar en historial
                    st.session_state.historial.append({
                        "pregunta": pregunta,
                        "sql": respuesta["sql"],
                        "origen": respuesta["origen"],
                        "resultado": respuesta["resultado"],
                        "tiempo": round(fin - inicio, 2),
                        "timestamp": datetime.now().strftime("%H:%M:%S")
                    })

                    st.success("✅ Consulta ejecutada")

            except Exception as e:
                st.error("❌ Error al procesar la consulta")
                st.exception(e)

# ---------------------------
# HISTORIAL (CHAT)
# ---------------------------
st.markdown("---")
st.subheader("💬 Historial de consultas")

if not st.session_state.historial:
    st.info("Aún no hay consultas")
else:

    # 🔥 IMPORTANTE: usamos enumerate para keys únicas
    for i, item in enumerate(reversed(st.session_state.historial)):

        st.markdown("---")

        col1, col2 = st.columns([4,1])

        with col1:
            st.markdown(f"### ❓ {item['pregunta']}")
            st.caption(f"🕒 {item['timestamp']}")

        with col2:
            st.metric("⏱ Tiempo", f"{item['tiempo']} s")

        st.caption(f"Origen SQL: {item.get('origen', 'IA')}")

        # ---------------------------
        # RESULTADO
        # ---------------------------
        mostrar_resultado(item["resultado"], i)

        # ---------------------------
        # SQL VISIBLE
        # ---------------------------
        with st.expander("🧠 Ver SQL generado"):
            st.code(item["sql"], language="sql")
