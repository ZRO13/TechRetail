# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "db429297-f382-4510-b581-7fe5f68732dc",
# META       "default_lakehouse_name": "LakehouseSilver",
# META       "default_lakehouse_workspace_id": "9c6ddd32-90c4-4f65-9cb9-92c963a18a5a",
# META       "known_lakehouses": [
# META         {
# META           "id": "db429297-f382-4510-b581-7fe5f68732dc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# 1. Configuración de esquemas
ESQUEMA_SILVER = "LakehouseSilver.silver"
# Nota: La escritura exacta hacia el Warehouse la configuraremos 
# al final, primero armaremos los DataFrames.

print(f"Notebook Gold inicializado. Leyendo desde: {ESQUEMA_SILVER}")

# 2. Construir DimTienda

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_silver_tienda = spark.read.table(f"{ESQUEMA_SILVER}.dim_tienda")

# Agregamos la llave surrogada 'tienda_key' usando row_number para que sea secuencial (1, 2, 3...)
windowSpec = Window.orderBy("tienda_id")
df_dim_tienda = df_silver_tienda.withColumn("tienda_key", row_number().over(windowSpec))

# Reordenamos columnas para que la llave quede al principio
columnas_tienda = ["tienda_key"] + [c for c in df_silver_tienda.columns]
df_dim_tienda = df_dim_tienda.select(*columnas_tienda)

print(f"DimTienda creada con {df_dim_tienda.count()} filas.")
df_dim_tienda.show(3)

# 3. Construir DimProducto
df_silver_producto = spark.read.table(f"{ESQUEMA_SILVER}.dim_producto")

# Agregamos la llave surrogada 'producto_key'
windowSpecProd = Window.orderBy("sku")
df_dim_producto = df_silver_producto.withColumn("producto_key", row_number().over(windowSpecProd))

columnas_prod = ["producto_key"] + [c for c in df_silver_producto.columns]
df_dim_producto = df_dim_producto.select(*columnas_prod)

print(f"DimProducto creada con {df_dim_producto.count()} filas.")
df_dim_producto.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit, explode, sequence, to_date, year, month, quarter, date_format

# --- 3. Construir DimEmpleado ---
df_silver_empleado = spark.read.table(f"{ESQUEMA_SILVER}.dim_empleado")
windowSpecEmp = Window.orderBy("empleado_id")
df_dim_empleado = df_silver_empleado.withColumn("empleado_key", row_number().over(windowSpecEmp))

columnas_emp = ["empleado_key"] + [c for c in df_silver_empleado.columns]
df_dim_empleado = df_dim_empleado.select(*columnas_emp)

print(f"DimEmpleado creada con {df_dim_empleado.count()} filas.")

# --- 4. Construir DimCliente (Con la regla del -1) ---
df_silver_cliente = spark.read.table(f"{ESQUEMA_SILVER}.dim_cliente")
windowSpecCli = Window.orderBy("cliente_id")
df_dim_cliente_base = df_silver_cliente.withColumn("cliente_key", row_number().over(windowSpecCli))

columnas_cli = ["cliente_key"] + [c for c in df_silver_cliente.columns]
df_dim_cliente_base = df_dim_cliente_base.select(*columnas_cli)

# Crear la fila artificial para el "Cliente No Identificado"
df_cliente_dummy = spark.sql("""
    SELECT 
        -1 as cliente_key,
        CAST(NULL as INT) as cliente_id,
        'Cliente No Identificado' as nombre_completo,
        CAST(NULL as DATE) as fecha_registro,
        'Sin Tier' as tier_lealtad,
        CAST(NULL as STRING) as ciudad
""")

# Unir la fila dummy con los clientes reales
df_dim_cliente = df_cliente_dummy.unionByName(df_dim_cliente_base)
print(f"DimCliente creada con {df_dim_cliente.count()} filas (incluyendo el -1).")

# --- 5. Construir DimFecha (Generada desde cero para 2025) ---
df_dim_fecha = spark.sql("""
    SELECT explode(sequence(to_date('2025-01-01'), to_date('2025-12-31'), interval 1 day)) as fecha
""")

df_dim_fecha = (
    df_dim_fecha
    .withColumn("fecha_key", date_format(col("fecha"), "yyyyMMdd").cast("int"))
    .withColumn("anio", year(col("fecha")))
    .withColumn("mes", month(col("fecha")))
    .withColumn("trimestre", quarter(col("fecha")))
    .withColumn("nombre_mes", date_format(col("fecha"), "MMMM"))
    .withColumn("dia_semana", date_format(col("fecha"), "EEEE"))
)

# Reordenar según el esquema requerido
df_dim_fecha = df_dim_fecha.select("fecha_key", "fecha", "anio", "mes", "trimestre", "nombre_mes", "dia_semana")
print(f"DimFecha creada con {df_dim_fecha.count()} filas (365 días).")
df_dim_fecha.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import date_format, coalesce, lit, col

# 1. Leer ventas_enriquecidas desde Silver
df_silver_ventas = spark.read.table(f"{ESQUEMA_SILVER}.ventas_enriquecidas")

# 2. Generar fecha_key (YYYYMMDD como entero) desde la fecha_venta original
df_ventas_base = df_silver_ventas.withColumn(
    "fecha_key", 
    date_format(col("fecha_venta"), "yyyyMMdd").cast("int")
)

# 3. Cruzar con las dimensiones para traer las llaves surrogadas (_key)
# Usamos 'left' join para no perder ventas si no hay match en la dimensión
df_fact = df_ventas_base.join(df_dim_tienda, "tienda_id", "left")
df_fact = df_fact.join(df_dim_producto, "sku", "left")
df_fact = df_fact.join(df_dim_empleado, "empleado_id", "left")

# En el cliente, si es nulo (venta sin lealtad), forzamos la llave al -1 que creamos
df_fact = df_fact.join(df_dim_cliente, "cliente_id", "left")
df_fact = df_fact.withColumn("cliente_key", coalesce(col("cliente_key"), lit(-1)))

# 4. Calcular costo_total (cantidad * costo_unitario que trajimos de DimProducto)
df_fact = df_fact.withColumn("costo_total", col("cantidad") * col("costo_unitario"))

# 5. Seleccionar estrictamente las columnas finales indicadas en el esquema Gold
columnas_hechos = [
    "transaccion_id", 
    "fecha_key", 
    "tienda_key", 
    "producto_key", 
    "empleado_key", 
    "cliente_key", 
    "cantidad", 
    "precio_unitario", 
    "descuento_pct", 
    "venta_total", 
    "costo_total"
]

df_fact_ventas = df_fact.select(*columnas_hechos)

print(f"FactVentas creada con {df_fact_ventas.count()} filas.")
df_fact_ventas.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum as _sum, round as _round, concat_ws, lpad, col, lit, to_date

# --- 1. Construir ventas_diarias_por_tienda ---
df_ventas_diarias = (
    df_fact_ventas.groupBy("tienda_key", "fecha_key")
    .agg(
        _sum("venta_total").alias("venta_total"),
        _sum("cantidad").alias("unidades")
    )
)

# --- 2. Construir real_vs_presupuesto ---
df_presupuesto_silver = spark.read.table(f"{ESQUEMA_SILVER}.presupuesto")

# Cruzamos las ventas diarias con DimTienda y DimFecha para recuperar IDs naturales
df_base_real = (
    df_ventas_diarias
    .join(df_dim_tienda, "tienda_key", "inner")
    .join(df_dim_fecha, "fecha_key", "inner")
)

# Construimos anio_mes como tipo Fecha (primer día del mes) para cruzar con el presupuesto
df_base_real = df_base_real.withColumn(
    "anio_mes", 
    to_date(concat_ws("-", col("anio"), lpad(col("mes"), 2, "0"), lit("01")), "yyyy-MM-dd")
)

# Agrupamos la venta real por tienda_id y mes
df_venta_mensual = (
    df_base_real.groupBy("tienda_id", "anio_mes")
    .agg(_sum("venta_total").alias("venta_total_real"))
)

# Cruzamos contra la meta de la tabla de presupuesto
df_real_vs_presupuesto = df_venta_mensual.join(
    df_presupuesto_silver, 
    ["tienda_id", "anio_mes"], 
    "left"
)

# Calculamos la variación porcentual y seleccionamos las columnas finales
df_real_vs_presupuesto = df_real_vs_presupuesto.withColumn(
    "variacion_pct",
    _round(((col("venta_total_real") - col("meta_venta")) / col("meta_venta")) * 100, 2)
).select("tienda_id", "anio_mes", "venta_total_real", "meta_venta", "variacion_pct")

print(f"ventas_diarias_por_tienda creada con {df_ventas_diarias.count()} filas.")
print(f"real_vs_presupuesto creada con {df_real_vs_presupuesto.count()} filas.")
df_real_vs_presupuesto.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Iniciando escritura temporal en Lakehouse (Puente hacia Gold)...")

# 1. Guardar las 5 Dimensiones con prefijo gold_
df_dim_tienda.write.mode("overwrite").saveAsTable("gold_dim_tienda")
df_dim_producto.write.mode("overwrite").saveAsTable("gold_dim_producto")
df_dim_empleado.write.mode("overwrite").saveAsTable("gold_dim_empleado")
df_dim_cliente.write.mode("overwrite").saveAsTable("gold_dim_cliente")
df_dim_fecha.write.mode("overwrite").saveAsTable("gold_dim_fecha")
print("Dimensiones guardadas exitosamente en el Lakehouse.")

# 2. Guardar la Tabla de Hechos
df_fact_ventas.write.mode("overwrite").saveAsTable("gold_fact_ventas")
print("Tabla FactVentas guardada exitosamente en el Lakehouse.")

# 3. Guardar las Tablas Agregadas de Negocio
df_ventas_diarias.write.mode("overwrite").saveAsTable("gold_ventas_diarias_por_tienda")
df_real_vs_presupuesto.write.mode("overwrite").saveAsTable("gold_real_vs_presupuesto")
print("Tablas agregadas de negocio guardadas exitosamente en el Lakehouse.")

print("¡Datos listos en el puente! Próxima parada: WarehouseGold.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
