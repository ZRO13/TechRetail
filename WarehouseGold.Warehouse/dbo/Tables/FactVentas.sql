CREATE TABLE [dbo].[FactVentas] (
    [transaccion_id]  VARCHAR (8000)  NULL,
    [fecha_key]       INT             NULL,
    [tienda_key]      INT             NULL,
    [producto_key]    INT             NULL,
    [empleado_key]    INT             NULL,
    [cliente_key]     INT             NULL,
    [cantidad]        VARCHAR (8000)  NULL,
    [precio_unitario] DECIMAL (10, 2) NULL,
    [descuento_pct]   INT             NULL,
    [venta_total]     FLOAT (53)      NULL,
    [costo_total]     FLOAT (53)      NULL
);


GO