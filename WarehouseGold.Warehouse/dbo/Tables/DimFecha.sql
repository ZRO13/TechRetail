CREATE TABLE [dbo].[DimFecha] (
    [fecha_key]  INT            NULL,
    [fecha]      DATE           NULL,
    [anio]       INT            NULL,
    [mes]        INT            NULL,
    [trimestre]  INT            NULL,
    [nombre_mes] VARCHAR (8000) NULL,
    [dia_semana] VARCHAR (8000) NULL
);


GO