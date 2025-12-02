-- ============================================================================
-- 06-sp_get_product_recommendations.sql
-- Stored Procedures para consultar recomendaciones de productos basadas en Apriori
-- ============================================================================

USE MSSQL_DW;
GO

-- ============================================================================
-- SP: Obtener recomendaciones para un producto específico
-- ============================================================================
IF OBJECT_ID('dbo.sp_get_product_recommendations', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_get_product_recommendations;
GO

CREATE PROCEDURE dbo.sp_get_product_recommendations
    @ProductId INT,
    @TopN INT = 10
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Validar que el producto existe
        IF NOT EXISTS (SELECT 1 FROM dwh.DimProduct WHERE id = @ProductId)
        BEGIN
            RAISERROR('Producto no encontrado', 16, 1);
            RETURN;
        END
        
        -- Buscar reglas donde el producto es antecedente
        SELECT TOP (@TopN)
            par.RuleID,
            par.ConsequentProductIds,
            par.ConsequentNames,
            par.Support,
            par.Confidence,
            par.Lift,
            par.FechaCalculo
        FROM dwh.ProductAssociationRules par
        WHERE par.Activo = 1
          AND (
              par.AntecedentProductIds = CAST(@ProductId AS NVARCHAR(50))  -- Solo este producto
              OR par.AntecedentProductIds LIKE CAST(@ProductId AS NVARCHAR(50)) + ',%'  -- Primero en lista
              OR par.AntecedentProductIds LIKE '%,' + CAST(@ProductId AS NVARCHAR(50)) + ',%'  -- En medio
              OR par.AntecedentProductIds LIKE '%,' + CAST(@ProductId AS NVARCHAR(50))  -- Último en lista
          )
        ORDER BY par.Lift DESC, par.Confidence DESC;
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

GRANT EXECUTE ON dbo.sp_get_product_recommendations TO public;
GO

-- ============================================================================
-- SP: Obtener recomendaciones para un carrito de compras
-- ============================================================================
IF OBJECT_ID('dbo.sp_get_cart_recommendations', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_get_cart_recommendations;
GO

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

CREATE PROCEDURE dbo.sp_get_cart_recommendations
    @ProductIds NVARCHAR(500),  -- IDs separados por coma: "1,5,10"
    @TopN INT = 10
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;
    SET QUOTED_IDENTIFIER ON;
    SET ANSI_NULLS ON;
    SET ANSI_PADDING ON;
    SET ANSI_WARNINGS ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    
    BEGIN TRY
        -- Usar tabla variable
        DECLARE @CartProducts TABLE (ProductId INT);
        
        -- Parsear manualmente sin STRING_SPLIT
        DECLARE @Pos INT = 1;
        DECLARE @NextPos INT;
        DECLARE @ValueStr NVARCHAR(50);
        
        SET @ProductIds = LTRIM(RTRIM(@ProductIds)) + ',';
        
        WHILE @Pos <= LEN(@ProductIds)
        BEGIN
            SET @NextPos = CHARINDEX(',', @ProductIds, @Pos);
            IF @NextPos > @Pos
            BEGIN
                SET @ValueStr = LTRIM(RTRIM(SUBSTRING(@ProductIds, @Pos, @NextPos - @Pos)));
                IF ISNUMERIC(@ValueStr) = 1
                BEGIN
                    INSERT INTO @CartProducts (ProductId) VALUES (CAST(@ValueStr AS INT));
                END
            END
            SET @Pos = @NextPos + 1;
        END
        
        -- Buscar reglas donde alguno de los productos del carrito es antecedente
        -- y el consecuente NO está en el carrito
        SELECT TOP (@TopN)
            par.RuleID,
            par.ConsequentProductIds,
            par.ConsequentNames,
            par.Support,
            par.Confidence,
            par.Lift,
            par.FechaCalculo,
            -- Calcular score ponderado (lift * confidence)
            (par.Lift * par.Confidence) AS Score
        FROM dwh.ProductAssociationRules par
        WHERE par.Activo = 1
          AND EXISTS (
              -- Verificar que al menos un producto del carrito está en antecedentes
              SELECT 1 FROM @CartProducts cp
              WHERE par.AntecedentProductIds = CAST(cp.ProductId AS NVARCHAR(50))
                 OR par.AntecedentProductIds LIKE CAST(cp.ProductId AS NVARCHAR(50)) + ',%'
                 OR par.AntecedentProductIds LIKE '%,' + CAST(cp.ProductId AS NVARCHAR(50)) + ',%'
                 OR par.AntecedentProductIds LIKE '%,' + CAST(cp.ProductId AS NVARCHAR(50))
          )
          AND NOT EXISTS (
              -- Excluir si el consecuente ya está en el carrito
              SELECT 1 FROM @CartProducts cp
              WHERE par.ConsequentProductIds = CAST(cp.ProductId AS NVARCHAR(50))
                 OR par.ConsequentProductIds LIKE CAST(cp.ProductId AS NVARCHAR(50)) + ',%'
                 OR par.ConsequentProductIds LIKE '%,' + CAST(cp.ProductId AS NVARCHAR(50)) + ',%'
                 OR par.ConsequentProductIds LIKE '%,' + CAST(cp.ProductId AS NVARCHAR(50))
          )
        ORDER BY Score DESC, par.Lift DESC;
        
    END TRY
    BEGIN CATCH
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

GRANT EXECUTE ON dbo.sp_get_cart_recommendations TO public;
GO

-- ============================================================================
-- SP: Obtener top reglas de asociación (para análisis)
-- ============================================================================
IF OBJECT_ID('dbo.sp_get_top_association_rules', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_get_top_association_rules;
GO

CREATE PROCEDURE dbo.sp_get_top_association_rules
    @TopN INT = 20,
    @OrderBy NVARCHAR(20) = 'Lift'  -- 'Lift', 'Confidence', 'Support'
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        IF @OrderBy = 'Confidence'
        BEGIN
            SELECT TOP (@TopN) * 
            FROM dwh.ProductAssociationRules
            WHERE Activo = 1
            ORDER BY Confidence DESC, Lift DESC;
        END
        ELSE IF @OrderBy = 'Support'
        BEGIN
            SELECT TOP (@TopN) * 
            FROM dwh.ProductAssociationRules
            WHERE Activo = 1
            ORDER BY Support DESC, Lift DESC;
        END
        ELSE -- Default: Lift
        BEGIN
            SELECT TOP (@TopN) * 
            FROM dwh.ProductAssociationRules
            WHERE Activo = 1
            ORDER BY Lift DESC, Confidence DESC;
        END
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;
GO

GRANT EXECUTE ON dbo.sp_get_top_association_rules TO public;
GO

-- ============================================================================
-- SP: Estadísticas de reglas de asociación
-- ============================================================================
IF OBJECT_ID('dbo.sp_get_apriori_stats', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_get_apriori_stats;
GO

CREATE PROCEDURE dbo.sp_get_apriori_stats
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        COUNT(*) AS TotalReglas,
        SUM(CASE WHEN Activo = 1 THEN 1 ELSE 0 END) AS ReglasActivas,
        MAX(FechaCalculo) AS UltimaActualizacion,
        AVG(CASE WHEN Activo = 1 THEN Support ELSE NULL END) AS SupportPromedio,
        AVG(CASE WHEN Activo = 1 THEN Confidence ELSE NULL END) AS ConfidencePromedio,
        AVG(CASE WHEN Activo = 1 THEN Lift ELSE NULL END) AS LiftPromedio,
        MAX(CASE WHEN Activo = 1 THEN Lift ELSE NULL END) AS LiftMaximo,
        MIN(CASE WHEN Activo = 1 THEN Support ELSE NULL END) AS SupportMinimo
    FROM dwh.ProductAssociationRules;
END;
GO

GRANT EXECUTE ON dbo.sp_get_apriori_stats TO public;
GO

PRINT '✓ Stored Procedures de Apriori creados exitosamente';
PRINT '  - sp_get_product_recommendations';
PRINT '  - sp_get_cart_recommendations';
PRINT '  - sp_get_top_association_rules';
PRINT '  - sp_get_apriori_stats';
GO
