USE MSSQL_DW;
GO

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

CREATE PROCEDURE dbo.sp_get_cart_recommendations
    @ProductIds NVARCHAR(500),
    @TopN INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Simplificar: buscar reglas directamente sin parsear
    SELECT TOP (@TopN)
        par.RuleID,
        par.AntecedentProductIds,
        par.ConsequentProductIds,
        par.AntecedentNames,
        par.ConsequentNames,
        par.Support,
        par.Confidence,
        par.Lift,
        par.FechaCalculo,
        (par.Lift * par.Confidence) AS Score
    FROM dwh.ProductAssociationRules par
    WHERE par.Activo = 1
      AND (
          CHARINDEX(',' + par.AntecedentProductIds + ',', ',' + @ProductIds + ',') > 0
          OR CHARINDEX(',' + REPLACE(par.AntecedentProductIds, ',', ',,') + ',', ',' + @ProductIds + ',') > 0
      )
    ORDER BY par.Lift DESC, par.Confidence DESC;
END;
GO

PRINT 'sp_get_cart_recommendations recreado exitosamente';
GO
