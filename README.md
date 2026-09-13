# TechRetail Corp — Plataforma de Datos en Microsoft Fabric

**Proyecto Final — Bootcamp Data Fabric y Arquitecturas de Datos Modernas (Código Facilito)**

## Descripción

TechRetail Corp es una cadena de retail mediana con presencia en varios países de LATAM (México, Argentina, Chile, Colombia y Perú), con tiendas físicas y canal online. Hoy sus reportes de ventas se arman a mano desde exports diarios en CSV, generando hasta 2 días de latencia frente a la operación real y sin ninguna fuente única de verdad.

Este repositorio contiene el desarrollo completo de la migración de su plataforma de datos a Microsoft Fabric: ingesta (batch + tiempo real), transformación, modelado dimensional, seguridad y consumo con IA, centralizando datos que hoy viven dispersos en el sistema POS, el ERP de catálogo, RRHH, el CRM de lealtad y el equipo de Finanzas.

## Arquitectura

Arquitectura Medallion completa dentro de un único Workspace de Fabric:

```
Fuentes (batch: 7 CSV) ──► LakehouseBronze ──► LakehouseSilver ──► WarehouseGold ──► Power BI / Data Agent
Fuentes (streaming: generador POS) ──► Eventstream ──► KQL Database ──► (monitoreo en tiempo real)
```

- **Bronze**: copia fiel de las fuentes de origen, sin transformación.
- **Silver**: limpieza, tipado y reglas de calidad de datos (deduplicación, normalización de formatos regionales, separación de rechazos).
- **Gold**: modelo dimensional en estrella (`FactVentas` + 5 dimensiones) más tablas agregadas de negocio, listo para consumo.

## Qué se implementó

- **Ingesta batch**: Pipeline / Copy Activity para 7 fuentes en CSV (tiendas, productos, empleados, clientes, presupuesto, ventas, inventario).
- **Ingesta en tiempo real**: Eventstream + KQL Database para detectar transacciones anómalas (descuentos ≥ 50%) al momento, vía un generador sintético de eventos POS.
- **Modelo dimensional en Gold**: tabla de hechos `FactVentas` con llaves surrogadas, dimensiones de Tienda, Producto, Empleado, Cliente y Fecha, incluyendo el patrón "Unknown Member" (`cliente_key = -1`) para ventas sin cliente registrado.
- **Seguridad en tres capas**:
  - Row-Level Security por región en el modelo semántico de Power BI.
  - Column-Level / Object-Level Security sobre el salario de empleados.
  - Dynamic Data Masking sobre documento de identidad, aplicado directamente en el Warehouse vía T-SQL.
- **Data Agent** conectado a la capa Gold, publicado en Microsoft Teams, para consultas del negocio en lenguaje natural.
- **Git integration y Deployment Pipeline** (Dev → Prod) para versionado y despliegue controlado.

## Estructura del repositorio

```
/notebooks         → Notebooks PySpark (Bronze→Silver, Silver→Gold)
/pipelines          → Definiciones de Data Pipelines
/docs               → Informe funcional/técnico y diagramas de arquitectura
```

## Fuentes de datos

| Archivo | Origen | Contenido |
|---|---|---|
| `ventas_transacciones.csv` | Sistema POS | Transacciones de venta diarias |
| `tiendas.csv` | ERP | Catálogo de tiendas |
| `productos.csv` | ERP | Catálogo de productos |
| `empleados.csv` | RRHH | Datos de empleados (incluye campos sensibles) |
| `clientes.csv` | CRM / Lealtad | Programa de lealtad |
| `presupuesto_metas.csv` | Finanzas | Metas por tienda/mes |
| `inventario_snapshot.csv` | Supply Chain | Snapshot mensual de inventario por tienda/categoría |
| `generador_ventas_tiempo_real.py` | Streaming | Simulador de transacciones POS en vivo |

## Autor

Proyecto individual — Bootcamp Data Fabric y Arquitecturas de Datos Modernas, Código Facilito.
