# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "19f06c36-eac5-44ae-9c50-b2cd5eed283c",
# META       "default_lakehouse_name": "LakehouseBronze",
# META       "default_lakehouse_workspace_id": "9c6ddd32-90c4-4f65-9cb9-92c963a18a5a",
# META       "known_lakehouses": [
# META         {
# META           "id": "19f06c36-eac5-44ae-9c50-b2cd5eed283c"
# META         },
# META         {
# META           "id": "db429297-f382-4510-b581-7fe5f68732dc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook: Bronze → Silver
# ## Proyecto TechRetail Corp — Bootcamp Data Fabric
# 
# Este notebook toma las 7 tablas crudas de `LakehouseBronze` (esquema `bronze`) 
# y produce las tablas limpias de `LakehouseSilver`, aplicando las reglas de 
# transformación y calidad de datos definidas en el documento funcional del proyecto.
# 
# **Reglas generales de esta capa:**
# - Ningún campo se completa con supuestos cuando la regla dice separar/rechazar.
# - Los nulos que representan datos de negocio válidos (ej. venta sin cliente) 
#   se mantienen como nulos aquí; se resuelven con llave surrogada en Gold.
# - Cada tabla se sobrescribe completa en cada corrida (`mode="overwrite"`).
# 
# **Orden de procesamiento:** de la tabla más simple a la más compleja.

# CELL ********************

from pyspark.sql.functions import (
    to_date, to_timestamp, trim, initcap, col, when,
    regexp_replace, lit, coalesce
)

ESQUEMA_BRONZE = "LakehouseBronze.bronze"
ESQUEMA_SILVER = "LakehouseSilver.silver"
print("Notebook inicializado. Esquema origen:", ESQUEMA_BRONZE, "| Esquema destino:", ESQUEMA_SILVER)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 1. tiendas → silver.dim_tienda
# 
# **Regla:** carga completa (catálogo maestro). Única transformación: 
# `fecha_apertura` de texto (`yyyy/MM/dd`) a tipo Fecha.

# CELL ********************

df_tiendas_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.tiendas")

df_dim_tienda = df_tiendas_bronze.withColumn(
    "fecha_apertura",
    to_date("fecha_apertura", "yyyy/MM/dd")
)

df_dim_tienda.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.dim_tienda")

print(f"Filas escritas en dim_tienda: {df_dim_tienda.count()}")
df_dim_tienda.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 2. productos → silver.dim_producto
# 
# **Reglas:**
# - `nombre_producto`: trim (espacios al inicio/final por captura manual en ERP).
# - `categoria`: trim + Formato Título (casing inconsistente: "ELECTRONICA", 
#   "electronica", "Electronica" → "Electronica").

# CELL ********************

df_productos_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.productos")

df_dim_producto = (
    df_productos_bronze
    .withColumn("nombre_producto", trim(col("nombre_producto")))
    .withColumn("categoria", initcap(trim(col("categoria"))))
)

df_dim_producto.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.dim_producto")

print(f"Filas escritas en dim_producto: {df_dim_producto.count()}")
df_dim_producto.select("categoria").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 3. empleados → silver.dim_empleado
# 
# **Reglas:**
# - `fecha_ingreso`: texto → Fecha.
# - `documento_identidad`: trim (espacios extra por error de digitación).
# - `salario`: campo sensible, sin transformación aquí — la protección de acceso 
#   se aplica después, en Gold/Power BI (CLS) y en el SQL endpoint (Dynamic Data Masking).

# CELL ********************

df_empleados_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.empleados")

df_dim_empleado = (
    df_empleados_bronze
    .withColumn("fecha_ingreso", to_date(col("fecha_ingreso"), "yyyy/MM/dd"))
    .withColumn("documento_identidad", trim(col("documento_identidad")))
)

df_dim_empleado.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.dim_empleado")

print(f"Filas escritas en dim_empleado: {df_dim_empleado.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 4. clientes → silver.dim_cliente
# 
# **Reglas:**
# - `fecha_registro`: texto → Fecha.
# - `tier_lealtad`: nulos → `"Sin Tier"` (~3% llega vacío; cliente registrado 
#   sin tier asignado aún, no es un error de captura).

# CELL ********************

df_clientes_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.clientes")

df_dim_cliente = (
    df_clientes_bronze
    .withColumn("fecha_registro", to_date(col("fecha_registro"), "yyyy/MM/dd"))
    .withColumn("tier_lealtad", coalesce(col("tier_lealtad"), lit("Sin Tier")))
)

df_dim_cliente.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.dim_cliente")

print(f"Filas escritas en dim_cliente: {df_dim_cliente.count()}")
df_dim_cliente.groupBy("tier_lealtad").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 5. presupuesto_metas → silver.presupuesto
# 
# **Regla:** fuente ya limpia desde el origen (Finanzas). Única transformación: 
# `anio_mes` de texto (`yyyy/MM`) a tipo Fecha, tomando el primer día del mes.

# CELL ********************

df_presupuesto_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.presupuesto_metas")

df_presupuesto = df_presupuesto_bronze.withColumn(
    "anio_mes",
    to_date(col("anio_mes"), "yyyy/MM")
)

df_presupuesto.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.presupuesto")

print(f"Filas escritas en presupuesto: {df_presupuesto.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 6. inventario_snapshot → silver.inventario
# 
# **Nota:** esta tabla no forma parte del mapping original del documento funcional, 
# pero se incluye para dar soporte al problem statement de Supply Chain 
# ("tengo que exportar y reenviar manualmente el inventario") y al requisito 
# del Data Agent de responder también sobre inventario.
# 
# **Regla:** fuente ya limpia. Única transformación: `anio_mes` a tipo Fecha, 
# mismo criterio que presupuesto.

# CELL ********************

df_inventario_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.inventario_snapshot")

df_inventario = df_inventario_bronze.withColumn(
    "anio_mes",
    to_date(col("anio_mes"), "yyyy/MM")
)

df_inventario.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.inventario")

print(f"Filas escritas en inventario: {df_inventario.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 7. ventas_transacciones → silver.ventas_enriquecidas / silver.rechazos_ventas
# 
# **La tabla con más reglas de calidad del proyecto — arquitectura de dos salidas.**
# 
# | Regla | Tratamiento |
# |---|---|
# | Duplicados exactos (~1%) | Eliminar |
# | `cliente_id` vacío (~40%) | Mantener nulo — dato de negocio válido, no error |
# | `sku` vacío (~3%) | Separar a `rechazos_ventas` — no completar con supuestos |
# | `precio_unitario` con coma decimal (~50%) | Normalizar a punto, convertir a Decimal |
# | `descuento_pct` vacío (~8%) | Reemplazar por `0` |
# | `medio_pago` casing inconsistente | Trim + Formato Título |
# | `venta_total` | Campo calculado: `cantidad × precio_unitario × (1 − descuento_pct/100)` |
# 
# Las filas válidas van a `ventas_enriquecidas`; las que no tienen `sku` se separan 
# a `rechazos_ventas` para revisión posterior, **sin descartarlas silenciosamente**.

# MARKDOWN ********************

# ---
# ## 7. ventas_transacciones → silver.ventas_enriquecidas / silver.rechazos_ventas
# 
# **La tabla con más reglas de calidad del proyecto — arquitectura de dos salidas.**
# Construcción paso a paso: cada celda aplica una regla, sobre el resultado de la anterior.

# CELL ********************

display(df_tiendas_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.ventas_transacciones")

conteo_original = df_ventas_bronze.count()

# Regla: eliminar filas duplicadas exactas (mismo transaccion_id repetido
# por reintento de integración, ~1% del volumen)
df_ventas_sin_dup = df_ventas_bronze.dropDuplicates()

conteo_sin_dup = df_ventas_sin_dup.count()

print(f"Filas originales: {conteo_original}")
print(f"Filas tras eliminar duplicados: {conteo_sin_dup}")
print(f"Duplicados eliminados: {conteo_original - conteo_sin_dup}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_tipada = (
    df_ventas_sin_dup
    .withColumnRenamed("fecha", "fecha_venta")
    .withColumn("fecha_venta", to_date(col("fecha_venta"), "yyyy/MM/dd"))
    .withColumnRenamed("hora", "hora_venta")
    .withColumn(
        "precio_unitario",
        regexp_replace(col("precio_unitario"), ",", ".").cast("decimal(10,2)")
    )
)

df_ventas_tipada.select("fecha_venta", "hora_venta", "precio_unitario").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_nulos = (
    df_ventas_tipada
    # cliente_id vacío (~40%) -> mantener nulo, es dato de negocio válido
    # (no se toca; el cast asegura que quede como entero nulo, no string vacío)
    .withColumn("cliente_id", col("cliente_id").cast("int"))
    # descuento_pct vacío (~8%) -> reemplazar por 0
    .withColumn("descuento_pct", coalesce(col("descuento_pct").cast("int"), lit(0)))
)

print("Nulos en cliente_id (deben mantenerse):")
df_ventas_nulos.filter(col("cliente_id").isNull()).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_medio_pago = df_ventas_nulos.withColumn(
    "medio_pago",
    initcap(trim(col("medio_pago")))
)

df_ventas_medio_pago.select("medio_pago").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_ventas_con_total = df_ventas_medio_pago.withColumn(
    "venta_total",
    col("cantidad") * col("precio_unitario") * (lit(1) - col("descuento_pct") / 100)
)

df_ventas_con_total.select("cantidad", "precio_unitario", "descuento_pct", "venta_total").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Separación en dos salidas: `ventas_enriquecidas` y `rechazos_ventas`
# 
# Las filas sin `sku` no se completan con supuestos — se separan a una tabla
# de rechazos para revisión posterior, sin descartarlas silenciosamente.

# CELL ********************

# Filas válidas: sí tienen sku
df_ventas_enriquecidas = df_ventas_con_total.filter(col("sku").isNotNull() & (col("sku") != ""))

# Filas rechazadas: sku vacío o nulo
df_rechazos_ventas = df_ventas_con_total.filter(col("sku").isNull() | (col("sku") == ""))

df_ventas_enriquecidas.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.ventas_enriquecidas")
df_rechazos_ventas.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.rechazos_ventas")

print(f"Filas en ventas_enriquecidas: {df_ventas_enriquecidas.count()}")
print(f"Filas en rechazos_ventas: {df_rechazos_ventas.count()}")
print(f"Total (debe coincidir con el conteo tras deduplicar): {df_ventas_enriquecidas.count() + df_rechazos_ventas.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Resultado final — ventas_transacciones
# 
# | Métrica | Valor |
# |---|---|
# | Filas originales (Bronze) | 18,180 |
# | Duplicados eliminados | 180 |
# | Filas tras deduplicar | 18,000 |
# | → `ventas_enriquecidas` | 17,503 |
# | → `rechazos_ventas` | 497 |
# 
# Suma verificada: 17,503 + 497 = 18,000 ✓ (ninguna fila perdida en el proceso).

# MARKDOWN ********************

# ---
# ## 8. Streaming (KQL Database) → silver.ventas_tiempo_real
# 
# **Origen distinto a las 7 tablas anteriores:** esta tabla no viene de un CSV 
# batch, sino del generador sintético de eventos POS, vía Eventstream → 
# KQL Database.
# 
# **Regla:** tabla complementaria de monitoreo — NO se integra al modelo 
# estrella ni sube a Gold. Su único propósito es detectar anomalías 
# (descuentos ≥ 50%) en el momento.
# 
# **Prerrequisito:** el generador `generador_ventas_tiempo_real.py` debe estar 
# corriendo, con el Eventstream y la KQL Database ya configurados y recibiendo 
# datos, antes de ejecutar esta celda.

# CELL ********************

df_streaming_bronze = spark.read.table(f"{ESQUEMA_BRONZE}.ventas_tiempo_real")

df_ventas_tiempo_real = (
    df_streaming_bronze
    .withColumn("timestamp_evento", to_timestamp(col("timestamp_evento")))
    .withColumn(
        "venta_total",
        col("cantidad") * col("precio_unitario") * (lit(1) - col("descuento_pct") / 100)
    )
    .withColumn("es_anomalia", when(col("descuento_pct") >= 50, True).otherwise(False))
)

df_ventas_tiempo_real.write.mode("overwrite").saveAsTable(f"{ESQUEMA_SILVER}.ventas_tiempo_real")

print(f"Filas escritas en ventas_tiempo_real: {df_ventas_tiempo_real.count()}")
df_ventas_tiempo_real.filter(col("es_anomalia") == True).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
