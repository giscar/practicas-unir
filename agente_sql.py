import re
import os
import sqlite3
import time
import psycopg2
import requests

from db_setup import SCHEMA_DESCRIPTION, SCHEMA_TABLES, construir_contexto_esquema


URL_OLLAMA = "http://localhost:11434/api/generate"
MODELO_SQL = os.getenv("MODELO_SQL", "qwen2.5-coder:1.5b")
MODELO_RESPUESTA = os.getenv("MODELO_RESPUESTA", "llama3")
MAX_INTENTOS = int(os.getenv("MAX_INTENTOS", "3"))
TIMEOUT_OLLAMA = int(os.getenv("TIMEOUT_OLLAMA", "60"))
OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "30m")
SQL_CACHE_DB = os.getenv("SQL_CACHE_DB", "cache_sql.sqlite3")

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

DICCIONARIO_DATOS = """
Diccionario de datos de negocio:
- clientes: personas o empresas que compran. Usa c.nombre para identificar cliente.
- pedidos: cabecera comercial/logística del pedido. Canal, estado, fecha, cliente, empleado y sucursal viven aquí.
- detalle_pedido: líneas del pedido. Úsala para ventas de productos, cantidades, descuentos y margen.
- productos: catálogo. pr.precio es precio de lista; pr.costo sirve para margen.
- categorias: agrupación de productos.
- empleados: vendedor o responsable asociado al pedido.
- sucursales: tienda/sede asociada al pedido o inventario.
- pagos: pagos realizados por pedido. pa.monto es dinero pagado; pa.metodo es método de pago.
- inventario: stock por producto y sucursal.

Reglas semánticas:
- ingresos/facturación/monto cobrado = SUM(pa.monto) desde pagos pa con pa.estado = 'pagado'.
- ventas = SUM(dp.cantidad * dp.precio_unitario * (1 - dp.descuento)).
- margen = SUM(dp.cantidad * ((dp.precio_unitario * (1 - dp.descuento)) - pr.costo)).
- pedidos por canal/estado = COUNT(*) desde pedidos pe, sin unir pagos salvo que la pregunta hable de pagos o montos.
- pagos se une con pedidos mediante pa.pedido_id = pe.id.
- detalle_pedido se une con pedidos mediante dp.pedido_id = pe.id.
- detalle_pedido se une con productos mediante dp.producto_id = pr.id.
- No existe dp.pagamento_id, dp.pago_id, pe.monto, pe.metodo ni pe.nombre.
"""


def normalizar_clave_pregunta(pregunta):
    return re.sub(r"\s+", " ", pregunta.strip().lower())


def init_cache_persistente():
    conn = sqlite3.connect(SQL_CACHE_DB)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sql_cache (
                pregunta TEXT PRIMARY KEY,
                sql TEXT NOT NULL,
                modelo TEXT NOT NULL,
                origen TEXT NOT NULL,
                contexto TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                used_count INTEGER DEFAULT 0
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def leer_cache_persistente(pregunta):
    init_cache_persistente()
    conn = sqlite3.connect(SQL_CACHE_DB)
    try:
        cursor = conn.execute(
            "SELECT sql, modelo, origen, contexto FROM sql_cache WHERE pregunta = ?",
            (normalizar_clave_pregunta(pregunta),)
        )
        row = cursor.fetchone()
        if not row:
            return None

        conn.execute(
            "UPDATE sql_cache SET used_count = used_count + 1 WHERE pregunta = ?",
            (normalizar_clave_pregunta(pregunta),)
        )
        conn.commit()

        return {
            "sql": row[0],
            "modelo": row[1],
            "origen": row[2],
            "contexto": row[3] or ""
        }
    finally:
        conn.close()


def guardar_cache_persistente(pregunta, sql, origen, contexto):
    init_cache_persistente()
    conn = sqlite3.connect(SQL_CACHE_DB)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO sql_cache (pregunta, sql, modelo, origen, contexto, used_count)
            VALUES (?, ?, ?, ?, ?, COALESCE(
                (SELECT used_count FROM sql_cache WHERE pregunta = ?),
                0
            ))
            """,
            (
                normalizar_clave_pregunta(pregunta),
                sql,
                MODELO_SQL,
                origen,
                contexto,
                normalizar_clave_pregunta(pregunta)
            )
        )
        conn.commit()
    finally:
        conn.close()


def guardar_correccion_usuario(pregunta, sql):
    sql_limpio = preparar_sql_para_pregunta(pregunta, sql)
    ok, error = validar_sql_detallado(pregunta, sql_limpio)

    if not ok:
        return {
            "ok": False,
            "error": error,
            "sql": sql_limpio
        }

    resultado = ejecutar_sql(sql_limpio)
    if isinstance(resultado, str):
        return {
            "ok": False,
            "error": resultado,
            "sql": sql_limpio
        }

    contexto = construir_contexto_esquema(pregunta)
    origen = "Corrección supervisada"
    cache_sql[normalizar_clave_pregunta(pregunta)] = {
        "sql": sql_limpio,
        "origen": origen,
        "contexto": contexto
    }
    guardar_cache_persistente(pregunta, sql_limpio, origen, contexto)

    return {
        "ok": True,
        "sql": sql_limpio,
        "filas": len(resultado["filas"]),
        "columnas": resultado["columnas"]
    }


def limpiar_cache_persistente():
    init_cache_persistente()
    conn = sqlite3.connect(SQL_CACHE_DB)
    try:
        conn.execute("DELETE FROM sql_cache")
        conn.commit()
    finally:
        conn.close()


def invalidar_cache_persistente(pregunta):
    init_cache_persistente()
    conn = sqlite3.connect(SQL_CACHE_DB)
    try:
        conn.execute(
            "DELETE FROM sql_cache WHERE pregunta = ?",
            (normalizar_clave_pregunta(pregunta),)
        )
        conn.commit()
    finally:
        conn.close()


def calentar_modelo():
    return llamar_ollama("Responde SOLO: SELECT 1;", max_tokens=8)


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
        columnas = [desc[0] for desc in cursor.description] if cursor.description else []
        cursor.close()
        conn.close()
        return {
            "filas": res,
            "columnas": columnas
        }
    except Exception as e:
        return f"error sql: {e}"


def ejecutar_sql_simple(query):
    resultado = ejecutar_sql(query)
    if isinstance(resultado, str):
        return resultado
    return resultado["filas"]


def llamar_ollama(prompt, temperature=0, max_tokens=160):
    try:
        response = requests.post(
            URL_OLLAMA,
            timeout=TIMEOUT_OLLAMA,
            json={
                "model": MODELO_SQL,
                "prompt": prompt,
                "stream": False,
                "keep_alive": OLLAMA_KEEP_ALIVE,
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
                "keep_alive": OLLAMA_KEEP_ALIVE,
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

    sql = re.sub(r"\bpe\.pa\.", "pa.", sql)
    sql = re.sub(r"\bp\.pa\.", "pa.", sql)

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


def reparar_errores_comunes_sql(sql):
    if not sql:
        return sql

    sql = re.sub(r"\bpe\.pa\.", "pa.", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bp\.pa\.", "pa.", sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"JOIN\s+pagos\s+pa\s+ON\s+dp\.(?:pagamento_id|pago_id)\s*=\s*pa\.id",
        "JOIN pagos pa ON pe.id = pa.pedido_id",
        sql,
        flags=re.IGNORECASE
    )
    sql = re.sub(
        r"JOIN\s+pagos\s+pa\s+ON\s+pa\.id\s*=\s+dp\.(?:pagamento_id|pago_id)",
        "JOIN pagos pa ON pa.pedido_id = pe.id",
        sql,
        flags=re.IGNORECASE
    )
    sql = re.sub(
        r"SUM\s*\(\s*pa\.metodo\s*=\s*'[^']+'\s+AND\s+pa\.monto\s*>\s*0\s*\)",
        "SUM(pa.monto)",
        sql,
        flags=re.IGNORECASE
    )
    sql = re.sub(
        r"SUM\s*\(\s*pa\.monto\s*>\s*0\s+AND\s+pa\.metodo\s*=\s*'[^']+'\s*\)",
        "SUM(pa.monto)",
        sql,
        flags=re.IGNORECASE
    )

    return sql


def eliminar_joins_no_usados(sql):
    patron_join = re.compile(
        r"\s+JOIN\s+([a-z_][a-z0-9_]*)\s+([a-z_][a-z0-9_]*)\s+ON\s+[\s\S]*?(?=\s+(?:JOIN|WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)|;)",
        re.IGNORECASE
    )

    sql_limpio = sql
    for match in list(patron_join.finditer(sql)):
        alias = match.group(2)
        resto_sql = sql_limpio.replace(match.group(0), " ")

        if not re.search(rf"\b{alias}\.", resto_sql, re.IGNORECASE):
            sql_limpio = sql_limpio.replace(match.group(0), " ")

    return re.sub(r"\n\s*\n", "\n", sql_limpio).strip()


def pregunta_pide_ranking(pregunta):
    pregunta_limpia = pregunta.lower()
    return any(
        palabra in pregunta_limpia
        for palabra in ["top", "más", "mas", "mayor", "mayores", "mejor", "mejores"]
    )


def alias_metrica_principal(sql):
    match_select = re.search(r"\bselect\b([\s\S]+?)\bfrom\b", sql, re.IGNORECASE)
    if not match_select:
        return None

    aliases = re.findall(
        r"\b(?:sum|count|avg|min|max)\s*\([\s\S]*?\)\s+as\s+([a-z_][a-z0-9_]*)",
        match_select.group(1),
        flags=re.IGNORECASE
    )
    if not aliases:
        aliases = re.findall(
            r"\bas\s+([a-z_][a-z0-9_]*)",
            match_select.group(1),
            flags=re.IGNORECASE
        )

    prioridades = [
        "rentabilidad", "margen", "utilidad", "ventas", "ingresos",
        "facturacion", "monto", "total", "cantidad", "conteo"
    ]

    for prioridad in prioridades:
        for alias in aliases:
            if prioridad in alias.lower():
                return alias

    return aliases[-1] if aliases else None


def aplicar_reglas_intencion(pregunta, sql):
    sql_limpio = sql.strip()

    if pregunta_pide_ranking(pregunta) and "order by" not in sql_limpio.lower():
        alias = alias_metrica_principal(sql_limpio)
        if alias:
            sql_limpio = re.sub(r";\s*$", "", sql_limpio)
            sql_limpio = f"{sql_limpio}\nORDER BY {alias} DESC;"

    if "top" in pregunta.lower() and "limit" not in sql_limpio.lower():
        sql_limpio = re.sub(r";\s*$", "", sql_limpio)
        sql_limpio = f"{sql_limpio}\nLIMIT 5;"

    return sql_limpio


def sanitizar_sql(sql):
    sql_limpio = limpiar_sql(sql)
    sql_limpio = reparar_errores_comunes_sql(sql_limpio)
    sql_limpio = normalizar_aliases_sql(sql_limpio)
    sql_limpio = reparar_errores_comunes_sql(sql_limpio)
    return eliminar_joins_no_usados(sql_limpio)


def preparar_sql_para_pregunta(pregunta, sql):
    return aplicar_reglas_intencion(pregunta, sanitizar_sql(sql))


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
    pide_rentabilidad = any(
        palabra in pregunta_limpia
        for palabra in ["margen", "rentable", "rentables", "rentabilidad"]
    )
    pide_cobros = any(
        palabra in pregunta_limpia
        for palabra in ["facturacion", "facturación", "cobrado", "cobrados", "recaudado", "recaudacion", "recaudación"]
    ) or ("ingreso" in pregunta_limpia and ("pago" in pregunta_limpia or "pagado" in pregunta_limpia))

    if "venta" in pregunta_limpia or "ventas" in pregunta_limpia:
        if "detalle_pedido" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Para preguntas de ventas usa detalle_pedido y una agregación SUM."
        if "cantidad" not in sql_limpio or "precio_unitario" not in sql_limpio:
            return False, "Ventas debe calcularse con cantidad y precio_unitario."

    if pide_rentabilidad:
        if "costo" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Rentabilidad/margen debe usar productos.costo y SUM."

    if "top" in pregunta_limpia:
        if "order by" not in sql_limpio or "limit" not in sql_limpio:
            return False, "Las preguntas top/ranking deben usar ORDER BY y LIMIT."

    if any(palabra in pregunta_limpia for palabra in ["más", "mas", "mayor", "mayores", "mejor", "mejores"]):
        if "order by" not in sql_limpio:
            return False, "Las preguntas de ranking deben ordenar el resultado con ORDER BY."

    if "monto" in pregunta_limpia and "pago" in pregunta_limpia:
        if "pagos" not in sql_limpio or "monto" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Para montos de pago usa pagos.monto y SUM."

    if pide_cobros and not pide_rentabilidad:
        if "pagos" not in sql_limpio or "pa.monto" not in sql_limpio or "sum(" not in sql_limpio:
            return False, "Cobros/facturación debe usar pagos.monto con SUM."
        if "pa.estado" not in sql_limpio or "pagado" not in sql_limpio:
            return False, "Cobros/facturación debe filtrar pagos confirmados con pa.estado = 'pagado'."

    if (
        "pedido" in pregunta_limpia
        and ("canal" in pregunta_limpia or "estado" in pregunta_limpia)
        and "pago" not in pregunta_limpia
        and "monto" not in pregunta_limpia
    ):
        if "pedidos" not in sql_limpio or "count(" not in sql_limpio:
            return False, "Pedidos por canal/estado debe usar pedidos y COUNT."
        if "pagos" in sql_limpio:
            return False, "No filtres por pagos cuando la pregunta solo pide pedidos."

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

    metricas = metricas_para_pregunta(pregunta)

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
- Usa alias de salida descriptivos en snake_case, por ejemplo cantidad_pedidos, ventas_totales, margen_total.
- Nunca uses p como alias.
- Nunca uses pe.nombre: pedidos no tiene columna nombre.
- Nunca uses pe.metodo ni pe.monto: metodo y monto están en pagos pa.
- Si preguntas por productos, el nombre del producto es pr.nombre y requiere productos pr.
- Si preguntas por clientes, clientes.nombre/ciudad/segmento están en clientes.
- Si preguntas por top o ranking, usa ORDER BY y LIMIT.

Métricas relevantes:
{metricas}

{VALORES_NEGOCIO}

{DICCIONARIO_DATOS}

{contexto_esquema}

{bloque_correccion}

Pregunta: {pregunta}
SQL:
""".strip()


def construir_prompt_corrector(pregunta, contexto_esquema, sql_fallido, error_detectado):
    metricas = metricas_para_pregunta(pregunta)

    return f"""
Eres un corrector experto de SQL PostgreSQL para un agente analítico.

Tu tarea es reparar una consulta fallida. Devuelve SOLO una consulta SQL válida.

Reglas:
- Responde únicamente SQL, sin explicación ni markdown.
- Mantén la intención de la pregunta original.
- Usa únicamente tablas y columnas del esquema.
- Corrige joins, aliases, columnas inexistentes y métricas mal calculadas.
- La consulta debe empezar con SELECT y terminar con punto y coma.
- No uses INSERT, UPDATE, DELETE, DROP, CREATE, ALTER ni TRUNCATE.
- Usa aliases: c=clientes, pe=pedidos, dp=detalle_pedido, pr=productos, ca=categorias, e=empleados, s=sucursales, pa=pagos, i=inventario.
- No uses p como alias.
- Usa aliases de salida en snake_case y entendibles.

Métricas relevantes:
{metricas}

{VALORES_NEGOCIO}

{DICCIONARIO_DATOS}

{contexto_esquema}

Pregunta original:
{pregunta}

SQL fallido:
{sql_fallido}

Error detectado:
{error_detectado}

SQL corregido:
""".strip()


def metricas_para_pregunta(pregunta):
    p = pregunta.lower()
    metricas = []
    pide_rentabilidad = any(palabra in p for palabra in ["margen", "rentable", "rentables", "rentabilidad"])
    pide_cobros = any(
        palabra in p
        for palabra in ["facturacion", "facturación", "cobrado", "cobrados", "recaudado", "recaudacion", "recaudación"]
    ) or ("ingreso" in p and ("pago" in p or "pagado" in p))

    if "venta" in p or "ventas" in p:
        metricas.append("- ventas = dp.cantidad * dp.precio_unitario * (1 - dp.descuento)")

    if pide_rentabilidad:
        metricas.append("- rentabilidad/margen = SUM(dp.cantidad * ((dp.precio_unitario * (1 - dp.descuento)) - pr.costo))")

    if "stock" in p:
        metricas.append("- stock bajo = i.stock < i.stock_minimo")

    if "pago" in p or "monto" in p:
        metricas.append("- monto vendido por metodo = SUM(pa.monto)")

    if pide_cobros and not pide_rentabilidad:
        metricas.append("- cobros/facturación = SUM(pa.monto) desde pagos pa con pa.estado = 'pagado'")

    if "pedido" in p and ("canal" in p or "estado" in p) and "pago" not in p:
        metricas.append("- pedidos por canal/estado = COUNT(*) desde pedidos pe, sin unir pagos")

    if not metricas:
        metricas.append("- conteos = COUNT(*)")

    return "\n".join(metricas)


def generar_sql_modelo(pregunta, contexto_esquema, sql_previo=None, error_previo=None):
    prompt = construir_prompt_sql(
        pregunta,
        contexto_esquema,
        sql_previo=sql_previo,
        error_previo=error_previo
    )
    respuesta = llamar_ollama(prompt)
    return preparar_sql_para_pregunta(pregunta, respuesta), respuesta


def corregir_sql_modelo(pregunta, contexto_esquema, sql_fallido, error_detectado):
    sql_preparado = preparar_sql_para_pregunta(pregunta, sql_fallido or "")
    ok_preparado, error_preparado = validar_sql_detallado(pregunta, sql_preparado)

    if ok_preparado:
        return sql_preparado, "Corrección determinística"

    prompt = construir_prompt_corrector(
        pregunta,
        contexto_esquema,
        sql_preparado or sql_fallido,
        f"{error_detectado}. Revisión local: {error_preparado}"
    )
    respuesta = llamar_ollama(prompt, max_tokens=190)
    return preparar_sql_para_pregunta(pregunta, respuesta), respuesta


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


def procesar_pregunta(pregunta, forzar_ia=False):
    pregunta_cache = normalizar_clave_pregunta(pregunta)

    if not forzar_ia and pregunta_cache in cache_sql:
        metadata = cache_sql[pregunta_cache]
        ok_cache, _ = validar_sql_detallado(pregunta, metadata["sql"])

        if ok_cache:
            resultado_cache = ejecutar_sql(metadata["sql"])

            if not isinstance(resultado_cache, str):
                return {
                    "ok": True,
                    "sql": metadata["sql"],
                    "resultado": resultado_cache["filas"],
                    "columnas": resultado_cache["columnas"],
                    "origen": f"{metadata['origen']} cache",
                    "tiempos": {
                        "generacion_sql": 0,
                        "validacion": 0,
                        "ejecucion_bd": 0,
                    },
                    "validaciones": ["Cache SQL", "Consulta SELECT", "Columnas válidas", "Ejecución PostgreSQL"]
                }
        else:
            cache_sql.pop(pregunta_cache, None)

    if not forzar_ia:
        metadata_persistente = leer_cache_persistente(pregunta)

        if metadata_persistente:
            ok_cache, _ = validar_sql_detallado(pregunta, metadata_persistente["sql"])

            if ok_cache:
                resultado_cache = ejecutar_sql(metadata_persistente["sql"])

                if not isinstance(resultado_cache, str):
                    cache_sql[pregunta_cache] = metadata_persistente
                    return {
                        "ok": True,
                        "sql": metadata_persistente["sql"],
                        "resultado": resultado_cache["filas"],
                        "columnas": resultado_cache["columnas"],
                        "origen": f"{metadata_persistente['origen']} memoria",
                        "tiempos": {
                            "generacion_sql": 0,
                            "validacion": 0,
                            "ejecucion_bd": 0,
                        },
                        "validaciones": ["Memoria SQL", "Consulta SELECT", "Columnas válidas", "Ejecución PostgreSQL"]
                    }
            else:
                invalidar_cache_persistente(pregunta)

    contexto_esquema = construir_contexto_esquema(pregunta)
    sql_actual = None
    error_actual = None
    metadata = None
    tiempos = {
        "generacion_sql": 0,
        "validacion": 0,
        "ejecucion_bd": 0,
    }
    validaciones_ok = []
    modo_actual = "generacion"

    for intento in range(1, MAX_INTENTOS + 1):
        inicio_generacion = time.time()
        if intento == 1:
            sql_actual, respuesta_modelo = generar_sql_modelo(
                pregunta,
                contexto_esquema
            )
            modo_actual = "generacion"
        else:
            sql_actual, respuesta_modelo = corregir_sql_modelo(
                pregunta,
                contexto_esquema,
                sql_actual,
                error_actual
            )
            modo_actual = "correccion"
        tiempos["generacion_sql"] += time.time() - inicio_generacion

        inicio_validacion = time.time()
        ok, error_validacion = validar_sql_detallado(pregunta, sql_actual)
        tiempos["validacion"] += time.time() - inicio_validacion
        metadata = {
            "sql": sql_actual,
            "origen": (
                f"IA ({MODELO_SQL}) intento {intento}"
                if modo_actual == "generacion"
                else f"IA correctora ({MODELO_SQL}) intento {intento}"
            ),
            "contexto": contexto_esquema,
            "respuesta_modelo": respuesta_modelo
        }

        if not ok:
            error_actual = error_validacion
            continue

        try:
            validaciones_ok = [
                "Consulta SELECT segura",
                "Tablas y columnas válidas",
                "Reglas de negocio verificadas",
            ]
            if modo_actual == "correccion":
                validaciones_ok.append("Autocorrección aplicada")

            inicio_bd = time.time()
            resultado = ejecutar_sql(sql_actual)
            tiempos["ejecucion_bd"] += time.time() - inicio_bd

            if isinstance(resultado, str) and "error sql" in resultado:
                raise Exception(resultado)

            cache_sql[pregunta_cache] = {
                "sql": sql_actual,
                "origen": (
                    f"IA ({MODELO_SQL})"
                    if modo_actual == "generacion"
                    else f"IA correctora ({MODELO_SQL})"
                ),
                "contexto": contexto_esquema
            }
            guardar_cache_persistente(
                pregunta,
                sql_actual,
                (
                    f"IA ({MODELO_SQL})"
                    if modo_actual == "generacion"
                    else f"IA correctora ({MODELO_SQL})"
                ),
                contexto_esquema
            )

            return {
                "ok": True,
                "sql": sql_actual,
                "resultado": resultado["filas"],
                "columnas": resultado["columnas"],
                "origen": metadata["origen"],
                "tiempos": {k: round(v, 2) for k, v in tiempos.items()},
                "validaciones": [*validaciones_ok, "Ejecución PostgreSQL"]
            }

        except Exception as e:
            error_actual = str(e)

    return {
        "ok": False,
        "error": error_actual or "No se pudo generar SQL válido.",
        "sql": sql_actual or "",
        "origen": metadata["origen"] if metadata else f"IA ({MODELO_SQL})",
        "tiempos": {k: round(v, 2) for k, v in tiempos.items()},
        "validaciones": validaciones_ok
    }
