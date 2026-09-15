CREATE TABLE [dbo].[DimTienda] (
    [tienda_key]     INT            NULL,
    [tienda_id]      VARCHAR (8000) NULL,
    [nombre_tienda]  VARCHAR (8000) NULL,
    [ciudad]         VARCHAR (8000) NULL,
    [pais]           VARCHAR (8000) NULL,
    [region]         VARCHAR (8000) NULL,
    [formato_tienda] VARCHAR (8000) NULL,
    [fecha_apertura] DATE           NULL
);


GO