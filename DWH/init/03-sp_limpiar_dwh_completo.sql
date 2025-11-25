-- ============================================================================
-- 03-sp_limpiar_dwh_completo.sql
-- Stored Procedure para limpiar COMPLETAMENTE el DWH (incluye exchange rates)
-- ============================================================================

USE MSSQL_DW;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('dbo.sp_limpiar_dwh_completo', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_limpiar_dwh_completo;
GO

-- Crear procedimiento
CREATE PROCEDURE dbo.sp_limpiar_dwh_completo
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Limpiar tablas de hechos (primero por FKs)
        IF OBJECT_ID('FactTargetSales', 'U') IS NOT NULL DELETE FROM FactTargetSales;
        IF OBJECT_ID('MetasVentas', 'U') IS NOT NULL DELETE FROM MetasVentas;
        IF OBJECT_ID('FactSales', 'U') IS NOT NULL DELETE FROM FactSales;
        
        -- Limpiar dimensiones
        IF OBJECT_ID('DimOrder', 'U') IS NOT NULL DELETE FROM DimOrder;
        IF OBJECT_ID('DimProduct', 'U') IS NOT NULL DELETE FROM DimProduct;
        IF OBJECT_ID('DimCustomer', 'U') IS NOT NULL DELETE FROM DimCustomer;
        IF OBJECT_ID('DimChannel', 'U') IS NOT NULL DELETE FROM DimChannel;
        IF OBJECT_ID('DimCategory', 'U') IS NOT NULL DELETE FROM DimCategory;
        IF OBJECT_ID('DimTime', 'U') IS NOT NULL DELETE FROM DimTime;
        IF OBJECT_ID('DimExchangeRate', 'U') IS NOT NULL DELETE FROM DimExchangeRate;
        
        -- Limpiar staging
        IF OBJECT_ID('staging_source_tracking', 'U') IS NOT NULL DELETE FROM staging_source_tracking;
        IF OBJECT_ID('staging_map_producto', 'U') IS NOT NULL DELETE FROM staging_map_producto;
        IF OBJECT_ID('staging_tipo_cambio', 'U') IS NOT NULL DELETE FROM staging_tipo_cambio;
        
        -- Resetear identidades (solo si existen)
        IF OBJECT_ID('DimCategory', 'U') IS NOT NULL DBCC CHECKIDENT ('DimCategory', RESEED, 0);
        IF OBJECT_ID('DimChannel', 'U') IS NOT NULL DBCC CHECKIDENT ('DimChannel', RESEED, 0);
        IF OBJECT_ID('DimCustomer', 'U') IS NOT NULL DBCC CHECKIDENT ('DimCustomer', RESEED, 0);
        IF OBJECT_ID('DimProduct', 'U') IS NOT NULL DBCC CHECKIDENT ('DimProduct', RESEED, 0);
        IF OBJECT_ID('DimOrder', 'U') IS NOT NULL DBCC CHECKIDENT ('DimOrder', RESEED, 0);
        IF OBJECT_ID('DimTime', 'U') IS NOT NULL DBCC CHECKIDENT ('DimTime', RESEED, 0);
        IF OBJECT_ID('DimExchangeRate', 'U') IS NOT NULL DBCC CHECKIDENT ('DimExchangeRate', RESEED, 0);
        IF OBJECT_ID('FactSales', 'U') IS NOT NULL DBCC CHECKIDENT ('FactSales', RESEED, 0);
        IF OBJECT_ID('FactTargetSales', 'U') IS NOT NULL DBCC CHECKIDENT ('FactTargetSales', RESEED, 0);
        IF OBJECT_ID('MetasVentas', 'U') IS NOT NULL DBCC CHECKIDENT ('MetasVentas', RESEED, 0);
        IF OBJECT_ID('staging_source_tracking', 'U') IS NOT NULL DBCC CHECKIDENT ('staging_source_tracking', RESEED, 0);
        IF OBJECT_ID('staging_map_producto', 'U') IS NOT NULL DBCC CHECKIDENT ('staging_map_producto', RESEED, 0);
        IF OBJECT_ID('staging_tipo_cambio', 'U') IS NOT NULL DBCC CHECKIDENT ('staging_tipo_cambio', RESEED, 0);
        
        COMMIT TRANSACTION;
        
        -- Resumen
        PRINT '========================================';
        PRINT 'DATA WAREHOUSE LIMPIADO COMPLETAMENTE';
        PRINT '========================================';
        PRINT 'Dimensiones: 0 registros';
        PRINT 'Hechos:      0 registros';
        PRINT 'Staging:     0 registros';
        PRINT 'Tipos Cambio: ELIMINADOS';
        PRINT '========================================';
        PRINT 'NOTA: Debe volver a cargar exchange rates';
        PRINT '========================================';
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- Otorgar permisos
GRANT EXECUTE ON dbo.sp_limpiar_dwh_completo TO public;
GO

PRINT '[OK] Stored Procedure dbo.sp_limpiar_dwh_completo creado exitosamente';
GO
