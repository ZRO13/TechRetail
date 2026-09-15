CREATE TABLE [dbo].[DimCliente] (
    [cliente_key]     INT            NULL,
    [cliente_id]      VARCHAR (8000) NULL,
    [nombre_completo] VARCHAR (8000) NULL,
    [fecha_registro]  DATE           NULL,
    [tier_lealtad]    VARCHAR (8000) NULL,
    [ciudad]          VARCHAR (8000) NULL
);


GO