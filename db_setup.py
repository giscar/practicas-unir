import random
import re
import unicodedata
from datetime import date, timedelta

from psycopg2.extras import execute_values


SCHEMA_DESCRIPTION = """
Tablas:
clientes(id, nombre, email, telefono, ciudad, pais, segmento, fecha_registro)
categorias(id, nombre, departamento)
proveedores(id, nombre, pais, contacto)
productos(id, nombre, categoria_id, proveedor_id, precio, costo, activo)
sucursales(id, nombre, ciudad, region)
empleados(id, nombre, rol, sucursal_id, fecha_ingreso)
pedidos(id, cliente_id, empleado_id, sucursal_id, fecha, estado, canal)
detalle_pedido(id, pedido_id, producto_id, cantidad, precio_unitario, descuento)
pagos(id, pedido_id, fecha, monto, metodo, estado)
envios(id, pedido_id, ciudad_destino, empresa_envio, estado, fecha_envio, fecha_entrega)
inventario(id, producto_id, sucursal_id, stock, stock_minimo)

Relaciones:
productos.categoria_id = categorias.id
productos.proveedor_id = proveedores.id
empleados.sucursal_id = sucursales.id
pedidos.cliente_id = clientes.id
pedidos.empleado_id = empleados.id
pedidos.sucursal_id = sucursales.id
detalle_pedido.pedido_id = pedidos.id
detalle_pedido.producto_id = productos.id
pagos.pedido_id = pedidos.id
envios.pedido_id = pedidos.id
inventario.producto_id = productos.id
inventario.sucursal_id = sucursales.id
"""

SCHEMA_TABLES = {
    "clientes": "clientes(id, nombre, email, telefono, ciudad, pais, segmento, fecha_registro)",
    "categorias": "categorias(id, nombre, departamento)",
    "proveedores": "proveedores(id, nombre, pais, contacto)",
    "productos": "productos(id, nombre, categoria_id, proveedor_id, precio, costo, activo)",
    "sucursales": "sucursales(id, nombre, ciudad, region)",
    "empleados": "empleados(id, nombre, rol, sucursal_id, fecha_ingreso)",
    "pedidos": "pedidos(id, cliente_id, empleado_id, sucursal_id, fecha, estado, canal)",
    "detalle_pedido": "detalle_pedido(id, pedido_id, producto_id, cantidad, precio_unitario, descuento)",
    "pagos": "pagos(id, pedido_id, fecha, monto, metodo, estado)",
    "envios": "envios(id, pedido_id, ciudad_destino, empresa_envio, estado, fecha_envio, fecha_entrega)",
    "inventario": "inventario(id, producto_id, sucursal_id, stock, stock_minimo)",
}

SCHEMA_RELATIONS = {
    ("productos", "categorias"): "productos.categoria_id = categorias.id",
    ("productos", "proveedores"): "productos.proveedor_id = proveedores.id",
    ("empleados", "sucursales"): "empleados.sucursal_id = sucursales.id",
    ("pedidos", "clientes"): "pedidos.cliente_id = clientes.id",
    ("pedidos", "empleados"): "pedidos.empleado_id = empleados.id",
    ("pedidos", "sucursales"): "pedidos.sucursal_id = sucursales.id",
    ("detalle_pedido", "pedidos"): "detalle_pedido.pedido_id = pedidos.id",
    ("detalle_pedido", "productos"): "detalle_pedido.producto_id = productos.id",
    ("pagos", "pedidos"): "pagos.pedido_id = pedidos.id",
    ("envios", "pedidos"): "envios.pedido_id = pedidos.id",
    ("inventario", "productos"): "inventario.producto_id = productos.id",
    ("inventario", "sucursales"): "inventario.sucursal_id = sucursales.id",
}

KEYWORD_TABLES = {
    "cliente": {"clientes"},
    "clientes": {"clientes"},
    "segmento": {"clientes"},
    "ciudad": {"clientes"},
    "producto": {"productos"},
    "productos": {"productos"},
    "categoria": {"categorias", "productos"},
    "categorias": {"categorias", "productos"},
    "proveedor": {"proveedores", "productos"},
    "proveedores": {"proveedores", "productos"},
    "sucursal": {"sucursales"},
    "sucursales": {"sucursales"},
    "empleado": {"empleados"},
    "empleados": {"empleados"},
    "vendedor": {"empleados"},
    "pedido": {"pedidos"},
    "pedidos": {"pedidos"},
    "venta": {"pedidos", "detalle_pedido", "productos"},
    "ventas": {"pedidos", "detalle_pedido", "productos"},
    "margen": {"pedidos", "detalle_pedido", "productos", "categorias"},
    "canal": {"pedidos"},
    "estado": {"pedidos"},
    "pago": {"pagos", "pedidos"},
    "pagos": {"pagos", "pedidos"},
    "metodo": {"pagos"},
    "metodos": {"pagos"},
    "envio": {"envios", "pedidos"},
    "envios": {"envios", "pedidos"},
    "stock": {"inventario", "productos", "sucursales"},
    "inventario": {"inventario", "productos", "sucursales"},
}


def normalizar_pregunta(pregunta):
    texto = pregunta.strip().lower()
    texto = "".join(
        char for char in unicodedata.normalize("NFD", texto)
        if unicodedata.category(char) != "Mn"
    )
    return re.sub(r"\s+", " ", texto)


def tablas_para_pregunta(pregunta):
    texto = normalizar_pregunta(pregunta)
    tablas = set()

    for palabra, tablas_relacionadas in KEYWORD_TABLES.items():
        if palabra in texto:
            tablas.update(tablas_relacionadas)

    if not tablas:
        return {"clientes", "productos", "pedidos", "detalle_pedido"}

    if "detalle_pedido" in tablas:
        tablas.update({"pedidos", "productos"})
    if "pedidos" in tablas and "ventas" in texto:
        tablas.add("detalle_pedido")
    if "productos" in tablas and ("categoria" in texto or "margen" in texto):
        tablas.add("categorias")
    if "empleados" in tablas:
        tablas.update({"pedidos", "detalle_pedido"})
    if "pagos" in tablas:
        tablas.add("pedidos")
    if "inventario" in tablas:
        tablas.update({"productos", "sucursales"})

    return tablas


def construir_contexto_esquema(pregunta):
    tablas = tablas_para_pregunta(pregunta)
    lineas_tablas = [SCHEMA_TABLES[nombre] for nombre in sorted(tablas)]
    relaciones = []

    for (origen, destino), relacion in SCHEMA_RELATIONS.items():
        if origen in tablas and destino in tablas:
            relaciones.append(relacion)

    contexto = ["Tablas relevantes:", *lineas_tablas]

    if relaciones:
        contexto.extend(["", "Relaciones relevantes:", *relaciones])

    return "\n".join(contexto)


CREATE_SCHEMA_SQL = """
DROP TABLE IF EXISTS envios CASCADE;
DROP TABLE IF EXISTS pagos CASCADE;
DROP TABLE IF EXISTS detalle_pedido CASCADE;
DROP TABLE IF EXISTS pedidos CASCADE;
DROP TABLE IF EXISTS inventario CASCADE;
DROP TABLE IF EXISTS empleados CASCADE;
DROP TABLE IF EXISTS sucursales CASCADE;
DROP TABLE IF EXISTS productos CASCADE;
DROP TABLE IF EXISTS proveedores CASCADE;
DROP TABLE IF EXISTS categorias CASCADE;
DROP TABLE IF EXISTS clientes CASCADE;

CREATE TABLE clientes (
    id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    email TEXT NOT NULL,
    telefono TEXT NOT NULL,
    ciudad TEXT NOT NULL,
    pais TEXT NOT NULL,
    segmento TEXT NOT NULL,
    fecha_registro DATE NOT NULL
);

CREATE TABLE categorias (
    id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    departamento TEXT NOT NULL
);

CREATE TABLE proveedores (
    id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    pais TEXT NOT NULL,
    contacto TEXT NOT NULL
);

CREATE TABLE productos (
    id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    categoria_id INTEGER NOT NULL REFERENCES categorias(id),
    proveedor_id INTEGER NOT NULL REFERENCES proveedores(id),
    precio NUMERIC(10, 2) NOT NULL,
    costo NUMERIC(10, 2) NOT NULL,
    activo BOOLEAN NOT NULL
);

CREATE TABLE sucursales (
    id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    ciudad TEXT NOT NULL,
    region TEXT NOT NULL
);

CREATE TABLE empleados (
    id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    rol TEXT NOT NULL,
    sucursal_id INTEGER NOT NULL REFERENCES sucursales(id),
    fecha_ingreso DATE NOT NULL
);

CREATE TABLE pedidos (
    id INTEGER PRIMARY KEY,
    cliente_id INTEGER NOT NULL REFERENCES clientes(id),
    empleado_id INTEGER NOT NULL REFERENCES empleados(id),
    sucursal_id INTEGER NOT NULL REFERENCES sucursales(id),
    fecha DATE NOT NULL,
    estado TEXT NOT NULL,
    canal TEXT NOT NULL
);

CREATE TABLE detalle_pedido (
    id INTEGER PRIMARY KEY,
    pedido_id INTEGER NOT NULL REFERENCES pedidos(id),
    producto_id INTEGER NOT NULL REFERENCES productos(id),
    cantidad INTEGER NOT NULL,
    precio_unitario NUMERIC(10, 2) NOT NULL,
    descuento NUMERIC(5, 2) NOT NULL
);

CREATE TABLE pagos (
    id INTEGER PRIMARY KEY,
    pedido_id INTEGER NOT NULL REFERENCES pedidos(id),
    fecha DATE NOT NULL,
    monto NUMERIC(12, 2) NOT NULL,
    metodo TEXT NOT NULL,
    estado TEXT NOT NULL
);

CREATE TABLE envios (
    id INTEGER PRIMARY KEY,
    pedido_id INTEGER NOT NULL REFERENCES pedidos(id),
    ciudad_destino TEXT NOT NULL,
    empresa_envio TEXT NOT NULL,
    estado TEXT NOT NULL,
    fecha_envio DATE,
    fecha_entrega DATE
);

CREATE TABLE inventario (
    id INTEGER PRIMARY KEY,
    producto_id INTEGER NOT NULL REFERENCES productos(id),
    sucursal_id INTEGER NOT NULL REFERENCES sucursales(id),
    stock INTEGER NOT NULL,
    stock_minimo INTEGER NOT NULL,
    UNIQUE(producto_id, sucursal_id)
);

CREATE INDEX idx_pedidos_fecha ON pedidos(fecha);
CREATE INDEX idx_pedidos_cliente ON pedidos(cliente_id);
CREATE INDEX idx_detalle_pedido ON detalle_pedido(pedido_id);
CREATE INDEX idx_detalle_producto ON detalle_pedido(producto_id);
CREATE INDEX idx_pagos_pedido ON pagos(pedido_id);
"""


def crear_esquema_y_cargar_datos(get_conn, total_clientes=300, total_productos=120, total_pedidos=2000):
    random.seed(42)

    nombres = [
        "Ana", "Carlos", "Maria", "Luis", "Lucia", "Jorge", "Valeria", "Diego",
        "Sofia", "Miguel", "Elena", "Raul", "Camila", "Andres", "Patricia"
    ]
    apellidos = [
        "Garcia", "Lopez", "Perez", "Torres", "Ramirez", "Flores", "Vargas",
        "Castillo", "Rojas", "Mendoza", "Herrera", "Chavez"
    ]
    ciudades = ["Lima", "Arequipa", "Cusco", "Trujillo", "Piura", "Chiclayo", "Ica", "Tacna"]
    segmentos = ["Corporativo", "Pyme", "Retail", "Mayorista", "Premium"]
    canales = ["Web", "App", "Tienda", "Call Center", "Marketplace"]
    estados_pedido = ["entregado", "entregado", "entregado", "enviado", "pendiente", "cancelado"]
    metodos_pago = ["tarjeta", "transferencia", "yape", "plin", "efectivo"]
    empresas_envio = ["DHL", "Olva Courier", "Shalom", "Urbano", "Interno"]

    categorias = [
        (1, "Laptops", "Tecnologia"),
        (2, "Perifericos", "Tecnologia"),
        (3, "Monitores", "Tecnologia"),
        (4, "Mobiliario", "Oficina"),
        (5, "Impresoras", "Oficina"),
        (6, "Software", "Servicios"),
        (7, "Audio", "Tecnologia"),
        (8, "Redes", "Tecnologia"),
        (9, "Seguridad", "Servicios"),
        (10, "Accesorios", "Tecnologia"),
        (11, "Ergonomia", "Oficina"),
        (12, "Almacenamiento", "Tecnologia"),
    ]
    proveedores = [
        (i, f"Proveedor {i:02d}", random.choice(["Peru", "Chile", "Colombia", "Mexico", "USA"]), f"contacto{i}@proveedor.com")
        for i in range(1, 11)
    ]
    sucursales = [
        (1, "Lima Centro", "Lima", "Costa"),
        (2, "Lima Norte", "Lima", "Costa"),
        (3, "Arequipa Mall", "Arequipa", "Sur"),
        (4, "Cusco Real", "Cusco", "Sur"),
        (5, "Trujillo Plaza", "Trujillo", "Norte"),
        (6, "Piura Open", "Piura", "Norte"),
    ]

    clientes = []
    for i in range(1, total_clientes + 1):
        nombre = f"{random.choice(nombres)} {random.choice(apellidos)}"
        clientes.append((
            i,
            nombre,
            f"cliente{i:04d}@empresa-demo.com",
            f"9{random.randint(10000000, 99999999)}",
            random.choice(ciudades),
            "Peru",
            random.choice(segmentos),
            date(2022, 1, 1) + timedelta(days=random.randint(0, 900)),
        ))

    productos = []
    for i in range(1, total_productos + 1):
        categoria_id = random.randint(1, len(categorias))
        costo = round(random.uniform(30, 3500), 2)
        precio = round(costo * random.uniform(1.18, 1.75), 2)
        productos.append((
            i,
            f"Producto {i:03d}",
            categoria_id,
            random.randint(1, len(proveedores)),
            precio,
            costo,
            random.random() > 0.05,
        ))

    empleados = []
    roles = ["Vendedor", "Asesor", "Supervisor", "Ejecutivo B2B"]
    for i in range(1, 25):
        empleados.append((
            i,
            f"{random.choice(nombres)} {random.choice(apellidos)}",
            random.choice(roles),
            random.randint(1, len(sucursales)),
            date(2021, 1, 1) + timedelta(days=random.randint(0, 1200)),
        ))

    inventario = []
    inventario_id = 1
    for producto_id in range(1, total_productos + 1):
        for sucursal_id in range(1, len(sucursales) + 1):
            inventario.append((inventario_id, producto_id, sucursal_id, random.randint(0, 250), random.randint(10, 35)))
            inventario_id += 1

    pedidos = []
    detalles = []
    pagos = []
    envios = []
    detalle_id = 1
    fecha_inicio = date(2023, 1, 1)
    precio_por_producto = {producto[0]: producto[4] for producto in productos}

    for pedido_id in range(1, total_pedidos + 1):
        cliente = random.choice(clientes)
        empleado = random.choice(empleados)
        sucursal_id = empleado[3]
        fecha = fecha_inicio + timedelta(days=random.randint(0, 850))
        estado = random.choice(estados_pedido)
        canal = random.choice(canales)
        pedidos.append((pedido_id, cliente[0], empleado[0], sucursal_id, fecha, estado, canal))

        total_pedido = 0
        for producto_id in random.sample(range(1, total_productos + 1), random.randint(1, 5)):
            cantidad = random.randint(1, 6)
            precio = precio_por_producto[producto_id]
            descuento = random.choice([0, 0, 0.05, 0.10, 0.15])
            total_pedido += float(precio) * cantidad * (1 - descuento)
            detalles.append((detalle_id, pedido_id, producto_id, cantidad, precio, descuento))
            detalle_id += 1

        pago_estado = "rechazado" if estado == "cancelado" else random.choice(["pagado", "pagado", "pagado", "pendiente"])
        pagos.append((pedido_id, pedido_id, fecha + timedelta(days=random.randint(0, 2)), round(total_pedido, 2), random.choice(metodos_pago), pago_estado))

        fecha_envio = None if estado in ("pendiente", "cancelado") else fecha + timedelta(days=random.randint(1, 3))
        fecha_entrega = fecha_envio + timedelta(days=random.randint(1, 7)) if estado == "entregado" else None
        envios.append((pedido_id, pedido_id, cliente[4], random.choice(empresas_envio), estado, fecha_envio, fecha_entrega))

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(CREATE_SCHEMA_SQL)
                execute_values(cursor, "INSERT INTO categorias VALUES %s", categorias)
                execute_values(cursor, "INSERT INTO proveedores VALUES %s", proveedores)
                execute_values(cursor, "INSERT INTO sucursales VALUES %s", sucursales)
                execute_values(cursor, "INSERT INTO clientes VALUES %s", clientes)
                execute_values(cursor, "INSERT INTO productos VALUES %s", productos)
                execute_values(cursor, "INSERT INTO empleados VALUES %s", empleados)
                execute_values(cursor, "INSERT INTO inventario VALUES %s", inventario)
                execute_values(cursor, "INSERT INTO pedidos VALUES %s", pedidos)
                execute_values(cursor, "INSERT INTO detalle_pedido VALUES %s", detalles)
                execute_values(cursor, "INSERT INTO pagos VALUES %s", pagos)
                execute_values(cursor, "INSERT INTO envios VALUES %s", envios)
    finally:
        conn.close()

    return {
        "clientes": len(clientes),
        "productos": len(productos),
        "pedidos": len(pedidos),
        "detalle_pedido": len(detalles),
        "pagos": len(pagos),
        "envios": len(envios),
        "inventario": len(inventario),
    }
