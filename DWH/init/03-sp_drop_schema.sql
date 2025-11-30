-- ============================================================================
-- 03-sp_drop_schema.sql
-- Stored Procedure para eliminar schema completo del DWH
-- ============================================================================

USE MSSQL_DW;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('dbo.sp_drop_schema', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_drop_schema;
GO

-- Crear procedimiento
CREATE PROCEDURE dbo.sp_drop_schema
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        PRINT '========================================';
        PRINT 'ELIMINANDO SCHEMA DWH';
        PRINT '========================================';
        
        -- Eliminar tablas de hechos
        IF OBJECT_ID('dwh.FactTargetSales', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.FactTargetSales;
            PRINT '[OK] FactTargetSales eliminada';
        END
        
        IF OBJECT_ID('dwh.MetasVentas', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.MetasVentas;
            PRINT '[OK] MetasVentas eliminada';
        END
        
        IF OBJECT_ID('dwh.FactSales', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.FactSales;
            PRINT '[OK] FactSales eliminada';
        END
        
        -- Eliminar dimensiones
        IF OBJECT_ID('dwh.DimOrder', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimOrder;
            PRINT '[OK] DimOrder eliminada';
        END
        
        IF OBJECT_ID('dwh.DimProduct', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimProduct;
            PRINT '[OK] DimProduct eliminada';
        END
        
        IF OBJECT_ID('dwh.DimCustomer', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimCustomer;
            PRINT '[OK] DimCustomer eliminada';
        END
        
        IF OBJECT_ID('dwh.DimChannel', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimChannel;
            PRINT '[OK] DimChannel eliminada';
        END
        
        IF OBJECT_ID('dwh.DimCategory', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimCategory;
            PRINT '[OK] DimCategory eliminada';
        END
        
        IF OBJECT_ID('dwh.DimTime', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimTime;
            PRINT '[OK] DimTime eliminada';
        END
        
        IF OBJECT_ID('dwh.DimExchangeRate', 'U') IS NOT NULL
        BEGIN
            DROP TABLE dwh.DimExchangeRate;
            PRINT '[OK] DimExchangeRate eliminada';
        END
        
        -- Eliminar tablas de staging
        IF OBJECT_ID('staging.source_tracking', 'U') IS NOT NULL
        BEGIN
            DROP TABLE staging.source_tracking;
            PRINT '[OK] staging.source_tracking eliminada';
        END
        
        IF OBJECT_ID('staging.tipo_cambio', 'U') IS NOT NULL
        BEGIN
            DROP TABLE staging.tipo_cambio;
            PRINT '[OK] staging.tipo_cambio eliminada';
        END
        
        IF OBJECT_ID('staging.map_producto', 'U') IS NOT NULL
        BEGIN
            DROP TABLE staging.map_producto;
            PRINT '[OK] staging.map_producto eliminada';
        END
        
        PRINT '';
        PRINT '========================================';
        PRINT 'SCHEMA DWH ELIMINADO EXITOSAMENTE';
        PRINT '========================================';
        PRINT 'Todas las tablas han sido eliminadas.';
        PRINT 'Use sp_init_schema para recrear.';
        PRINT '========================================';
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- Otorgar permisos
GRANT EXECUTE ON dbo.sp_drop_schema TO public;
GO

PRINT '[OK] Stored Procedure dbo.sp_drop_schema creado exitosamente';
GO
