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
        IF OBJECT_ID('FactTargetSales', 'U') IS NOT NULL
        BEGIN
            DROP TABLE FactTargetSales;
            PRINT '[OK] FactTargetSales eliminada';
        END
        
        IF OBJECT_ID('MetasVentas', 'U') IS NOT NULL
        BEGIN
            DROP TABLE MetasVentas;
            PRINT '[OK] MetasVentas eliminada';
        END
        
        IF OBJECT_ID('FactSales', 'U') IS NOT NULL
        BEGIN
            DROP TABLE FactSales;
            PRINT '[OK] FactSales eliminada';
        END
        
        -- Eliminar dimensiones
        IF OBJECT_ID('DimOrder', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimOrder;
            PRINT '[OK] DimOrder eliminada';
        END
        
        IF OBJECT_ID('DimProduct', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimProduct;
            PRINT '[OK] DimProduct eliminada';
        END
        
        IF OBJECT_ID('DimCustomer', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimCustomer;
            PRINT '[OK] DimCustomer eliminada';
        END
        
        IF OBJECT_ID('DimChannel', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimChannel;
            PRINT '[OK] DimChannel eliminada';
        END
        
        IF OBJECT_ID('DimCategory', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimCategory;
            PRINT '[OK] DimCategory eliminada';
        END
        
        IF OBJECT_ID('DimTime', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimTime;
            PRINT '[OK] DimTime eliminada';
        END
        
        IF OBJECT_ID('DimExchangeRate', 'U') IS NOT NULL
        BEGIN
            DROP TABLE DimExchangeRate;
            PRINT '[OK] DimExchangeRate eliminada';
        END
        
        -- Eliminar tablas de staging
        IF OBJECT_ID('staging_source_tracking', 'U') IS NOT NULL
        BEGIN
            DROP TABLE staging_source_tracking;
            PRINT '[OK] staging_source_tracking eliminada';
        END
        
        IF OBJECT_ID('staging_tipo_cambio', 'U') IS NOT NULL
        BEGIN
            DROP TABLE staging_tipo_cambio;
            PRINT '[OK] staging_tipo_cambio eliminada';
        END
        
        IF OBJECT_ID('staging_map_producto', 'U') IS NOT NULL
        BEGIN
            DROP TABLE staging_map_producto;
            PRINT '[OK] staging_map_producto eliminada';
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
