import re
import psycopg2
import requests

from db_setup import SCHEMA_DESCRIPTION, SCHEMA_TABLES, construir_contexto_esquema


URL_OLLAMA = "http://localhost:11434/api/generate"
MODELO_SQL = "qwen2.5-coder:1.5b"
MODELO_RESPUESTA = "llama3"
MAX_INTENTOS = 3
TIMEOUT_OLLAMA = 60

cache_sql = {}

PALABRAS_SQL = {
    "on", "where", "group", "order", "limit", "join", "left", "right", "inner",
    "outer", "full", "cross", "using", "having"
}

VALORES_NEGOCIO = """
Valores conocidos:
- pedidos.estado: entregado, enviado, pendiente, cancelado
- pedidos.canal: Web, App, Tienda, Call Center, Marketplace
- pagos.estado: pagado, pendiente, rechazado
- pagos.metodo: tarjeta, transferencia, yape, plin, efectivo
"""


def columnas_por_tabla():
    columnas = {}

    for tabla, definicion in SCHEMA_TABLES.items():
        match = re.search(r"\((.*?)\)", definicion)
        if not match:
            columnas[tabla] = set()
            continue

        columnas[tabla] = {
            columna.strip()
            for columna in match.group(1).split(",")
        }

    return columnas


SCHEMA_COLUMNS = columnas_por_tabla()


def get_conn():
    return psycopg2.connect(
        host="localhost",
        database="empresa",
        user="admin",
        password="admin"
    )


def ejecutar_sql(query):
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("SET statement_timeout = 8000")
        cursor.execute(query)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    except Exception as e:
        return f"error sql: {e}"


def llamar_ollama(prompt, temperature=0, max_tokens=160):
    try:
        response = requests.post(
            URL_OLLAMA,
            timeout=TIMEOUT_OLLAMA,
            json={
                "model": MODELO_SQL,
                "prompt": prompt,
                "stream": False,
                "keep_alive": "5m",
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "num_ctx": 1536
                }
            }
        )

        data = response.json()
        return data.get("response", "")

    except Exception as e:
        return f"error llm: {e}"


def llamar_ollama_modelo(modelo, prompt, temperature=0.2, max_tokens=120):
    try:
        response = requests.post(
            URL_OLLAMA,
            timeout=TIMEOUT_OLLAMA,
            json={
                "model": modelo,
                "prompt": prompt,
                "stream": False,
                "keep_alive": "5m",
                "options": {
                    "temperature": temperature,
                    "num_predict": max_tokens,
                    "num_ctx": 1536
                }
            }
        )

        data = response.json()
        return data.get("response", "")

    except Exception as e:
        return f"error llm: {e}"


def limpiar_sql(sql):
    if not sql:
        return ""

    sql = re.sub(r"```sql|```", "", sql.strip(), flags=re.IGNORECASE)

    match = re.search(r"(SELECT[\s\S]*?;)", sql, re.IGNORECASE)
    if match:
        return match.group(1).strip()

    match = re.search(r"(SELECT[\s\S]*)", sql, re.IGNORECASE)
    if not match:
        return sql.split("\n")[0].strip()

    candidato = match.group(1).strip()
    return candidato if candidato.endswith(";") else f"{candidato};"


def normalizar_aliases_sql(sql):
    aliases = extraer_aliases(sql)

    if aliases.get("p") == "pedidos":
        sql = re.sub(r"\bpedidos\s+p\b", "pedidos pe", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bp\.", "pe.", sql)

    if aliases.get("p") == "pagos":
        sql = re.sub(r"\bpagos\s+p\b", "pagos pa", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bp\.", "pa.", sql)

    aliases = extraer_aliases(sql)

    if "pe" in aliases and "p" not in aliases:
        sql = re.sub(r"\bp\.", "pe.", sql)

    return sql


def validar_sql_basico(sql):
    if not sql:
        return False, "El modelo no devolvió SQL."

    s = sql.strip().lower()

    if not s.startswith("select"):
        return False, "La consulta debe empezar con SELECT."

    if not re.search(r"\bfrom\b", s):
        return False, "La consulta debe incluir FROM."

    prohibidas = ["drop", "delete", "update", "insert", "alter", "truncate", "create"]
    if any(re.search(rf"\b{palabra}\b", s) for palabra in prohibidas):
        return False, "Solo se permiten consultas SELECT."

    return True, ""


def extraer_aliases(sql):
    aliases = {}
    patron = re.compile(
        r"\b(?:from|join)\s+([a-z_][a-z0-9_]*)\s*(?:as\s+)?([a-z_][a-z0-9_]*)?",
        re.IGNORECASE
    )

    for tabla, alias in patron.findall(sql):
        tabla = tabla.lower()
        alias = alias.lower() if alias else tabla

        if alias in PALABRAS_SQL:
            alias = tabla

        aliases[alias] = tabla

    return aliases


def validar_tablas_y_columnas(sql):
    aliases = extraer_aliases(sql)

    if not aliases:
        return False, "No se detectaron tablas en FROM/JOIN."

    if "p" in aliases:
        return False, "No uses alias 'p'. Usa 'pe' para pedidos y 'pr' para productos."

    for alias, tabla in aliases.items():
        if tabla not in SCHEMA_COLUMNS:
            return False, f"La tabla '{tabla}' no existe en el esquema disponible."

    referencias = re.findall(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b", sql, re.IGNORECASE)

    for alias, columna in referencias:
        alias = alias.lower()
        columna = columna.lower()

        if alias not in aliases:
            return False, f"El alias '{alias}' no está definido en FROM/JOIN."

        tabla = aliases[alias]
        if columna not in SCHEMA_COLUMNS[tabla]:
            columnas_validas = ", ".join(sorted(SCHEMA_COLUMNS[tabla]))
            return False, (
                f"La columna '{alias}.{columna}' no existe. "
                f"'{alias}' apunta a '{tabla}'. Columnas válidas: {columnas_validas}."
            )

    return True, ""


def validar_sql_negocio(pregunta, sql):
    pregunta_limpia = pregunta.lower()
    sql_limpio = sql.lower()

    if "venta" in pregunta_limpia or "ventas" in pregunta_limpia:
        if "detalle_pedido" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Para preguntas de ventas usa detalle_pedido y una agregación SUM."
        if "cantidad" not in sql_limpio or "precio_unitario" not in sql_limpio:
            return False, "Ventas debe calcularse con cantidad y precio_unitario."

    if "margen" in pregunta_limpia:
        if "costo" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Margen debe usar productos.costo y SUM."

    if "top" in pregunta_limpia:
        if "order by" not in sql_limpio or "limit" not in sql_limpio:
            return False, "Las preguntas top/ranking deben usar ORDER BY y LIMIT."

    if "monto" in pregunta_limpia and "pago" in pregunta_limpia:
        if "pagos" not in sql_limpio or "monto" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Para montos de pago usa pagos.monto y SUM."

    if "stock" in pregunta_limpia and "minimo" in pregunta_limpia:
        if "stock_minimo" not in sql_limpio:
            return False, "Para stock bajo compara inventario.stock con inventario.stock_minimo."

    return True, ""


def validar_sql_detallado(pregunta, sql):
    validaciones = [
        validar_sql_basico,
        lambda q: validar_tablas_y_columnas(q),
        lambda q: validar_sql_negocio(pregunta, q),
    ]

    for validacion in validaciones:
        ok, error = validacion(sql)
        if not ok:
            return False, error

    return True, ""


def construir_prompt_sql(pregunta, contexto_esquema, sql_previo=None, error_previo=None):
    bloque_correccion = ""

    if sql_previo or error_previo:
        bloque_correccion = f"""
Corrige el intento anterior.
SQL anterior:
{sql_previo}

Error detectado:
{error_previo}
"""

    return f"""
Eres un generador de SQL PostgreSQL para analítica empresarial.

Devuelve una sola consulta SQL.

Reglas estrictas:
- Responde SOLO SQL, sin explicación ni markdown.
- La consulta debe empezar con SELECT y terminar con punto y coma.
- Usa únicamente tablas y columnas listadas en el esquema relevante.
- No inventes columnas calculadas como ventas; calcula la métrica.
- No uses valores que no existan en la lista de valores conocidos.
- No uses INSERT, UPDATE, DELETE, DROP, CREATE, ALTER ni TRUNCATE.
- Usa alias claros: c=clientes, pe=pedidos, dp=detalle_pedido, pr=productos, ca=categorias, e=empleados, pa=pagos.
- Nunca uses p como alias.
- Nunca uses pe.nombre: pedidos no tiene columna nombre.
- Nunca uses pe.metodo ni pe.monto: metodo y monto están en pagos pa.
- Si preguntas por productos, el nombre del producto es pr.nombre y requiere productos pr.
- Si preguntas por clientes, clientes.nombre/ciudad/segmento están en clientes.
- Si preguntas por top o ranking, usa ORDER BY y LIMIT.

Métricas obligatorias:
- ventas = dp.cantidad * dp.precio_unitario * (1 - dp.descuento)
- margen = ventas - dp.cantidad * pr.costo
- stock bajo = i.stock < i.stock_minimo

{VALORES_NEGOCIO}

{contexto_esquema}

{bloque_correccion}

Pregunta: {pregunta}
SQL:
""".strip()


def generar_sql_modelo(pregunta, contexto_esquema, sql_previo=None, error_previo=None):
    prompt = construir_prompt_sql(
        pregunta,
        contexto_esquema,
        sql_previo=sql_previo,
        error_previo=error_previo
    )
    respuesta = llamar_ollama(prompt)
    return normalizar_aliases_sql(limpiar_sql(respuesta)), respuesta


def generar_sql(pregunta):
    return generar_sql_con_metadatos(pregunta)["sql"]


def generar_sql_con_metadatos(pregunta):
    pregunta_cache = pregunta.strip().lower()

    if pregunta_cache in cache_sql:
        return cache_sql[pregunta_cache]

    contexto_esquema = construir_contexto_esquema(pregunta)
    sql, respuesta = generar_sql_modelo(pregunta, contexto_esquema)
    ok, error = validar_sql_detallado(pregunta, sql)

    metadata = {
        "sql": sql,
        "origen": f"IA ({MODELO_SQL})" if ok else f"IA inválida ({MODELO_SQL})",
        "error_modelo": None if ok else respuesta,
        "error_validacion": "" if ok else error,
        "contexto": contexto_esquema
    }

    if ok:
        cache_sql[pregunta_cache] = metadata

    return metadata


def generar_respuesta_natural(pregunta, resultado):
    prompt = f"""
Explica este resultado de forma breve y ejecutiva.

Pregunta: {pregunta}
Resultado: {resultado}

Respuesta:
"""
    return llamar_ollama_modelo(MODELO_RESPUESTA, prompt, 0.3, 90).strip()


def procesar_pregunta(pregunta):
    pregunta_cache = pregunta.strip().lower()

    if pregunta_cache in cache_sql:
        metadata = cache_sql[pregunta_cache]
        resultado_cache = ejecutar_sql(metadata["sql"])

        if not isinstance(resultado_cache, str):
            return {
                "ok": True,
                "sql": metadata["sql"],
                "resultado": resultado_cache,
                "origen": f"{metadata['origen']} cache"
            }

    contexto_esquema = construir_contexto_esquema(pregunta)
    sql_actual = None
    error_actual = None
    metadata = None

    for intento in range(1, MAX_INTENTOS + 1):
        sql_actual, respuesta_modelo = generar_sql_modelo(
            pregunta,
            contexto_esquema,
            sql_previo=sql_actual,
            error_previo=error_actual
        )

        ok, error_validacion = validar_sql_detallado(pregunta, sql_actual)
        metadata = {
            "sql": sql_actual,
            "origen": f"IA ({MODELO_SQL}) intento {intento}",
            "contexto": contexto_esquema,
            "respuesta_modelo": respuesta_modelo
        }

        if not ok:
            error_actual = error_validacion
            continue

        try:
            resultado = ejecutar_sql(sql_actual)

            if isinstance(resultado, str) and "error sql" in resultado:
                raise Exception(resultado)

            cache_sql[pregunta_cache] = {
                "sql": sql_actual,
                "origen": f"IA ({MODELO_SQL})",
                "contexto": contexto_esquema
            }

            return {
                "ok": True,
                "sql": sql_actual,
                "resultado": resultado,
                "origen": metadata["origen"]
            }

        except Exception as e:
            error_actual = str(e)

    return {
        "ok": False,
        "error": error_actual or "No se pudo generar SQL válido.",
        "sql": sql_actual or "",
        "origen": metadata["origen"] if metadata else f"IA ({MODELO_SQL})"
    }
