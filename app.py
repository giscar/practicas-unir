import streamlit as st
import pandas as pd
import time
from datetime import datetime

# IMPORTA TU AGENTE
from agente_sql import generar_sql, ejecutar_sql, validar_sql

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

# ---------------------------
# INPUT
# ---------------------------
with st.container():
    col1, col2 = st.columns([4,1])

    with col1:
        pregunta = st.text_input("❓ Escribe tu pregunta")

    with col2:
        ejecutar = st.button("Consultar")

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
                # 1. Generar SQL
                sql = generar_sql(pregunta)

                # 2. Validar
                if not validar_sql(sql):
                    st.error("❌ SQL inválido generado")
                else:
                    # 3. Ejecutar
                    resultado = ejecutar_sql(sql)

                    fin = time.time()

                    # Guardar en historial
                    st.session_state.historial.append({
                        "pregunta": pregunta,
                        "sql": sql,
                        "resultado": resultado,
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

        # ---------------------------
        # RESULTADO
        # ---------------------------
        if isinstance(item["resultado"], str):
            st.error(item["resultado"])
        else:
            df = pd.DataFrame(item["resultado"])

            st.dataframe(df, use_container_width=True)

            # ---------------------------
            # DESCARGA (FIX ERROR)
            # ---------------------------
            csv = df.to_csv(index=False).encode('utf-8')

            st.download_button(
                label="📥 Descargar resultados (CSV)",
                data=csv,
                file_name=f"consulta_{i}.csv",
                mime="text/csv",
                key=f"download_{i}"  # 🔥 CLAVE ÚNICA
            )

        # ---------------------------
        # SQL VISIBLE
        # ---------------------------
        with st.expander("🧠 Ver SQL generado"):
            st.code(item["sql"], language="sql")