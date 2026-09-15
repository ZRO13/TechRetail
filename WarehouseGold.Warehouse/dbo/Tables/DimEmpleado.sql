CREATE TABLE [dbo].[DimEmpleado] (
    [empleado_key]        INT                                                               NULL,
    [empleado_id]         VARCHAR (8000)                                                    NULL,
    [nombre_completo]     VARCHAR (8000)                                                    NULL,
    [tienda_id]           VARCHAR (8000)                                                    NULL,
    [puesto]              VARCHAR (8000)                                                    NULL,
    [fecha_ingreso]       DATE                                                              NULL,
    [salario]             VARCHAR (8000)                                                    NULL,
    [documento_identidad] VARCHAR (8000) MASKED WITH (FUNCTION = 'partial(2, "XXXXXX", 2)') NULL
);


GO