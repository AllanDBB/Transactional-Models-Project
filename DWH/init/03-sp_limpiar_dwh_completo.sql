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
        IF OBJECT_ID('dwh.FactTargetSales', 'U') IS NOT NULL DELETE FROM dwh.FactTargetSales;
        IF OBJECT_ID('dwh.MetasVentas', 'U') IS NOT NULL DELETE FROM dwh.MetasVentas;
        IF OBJECT_ID('dwh.FactSales', 'U') IS NOT NULL DELETE FROM dwh.FactSales;
        
        -- Limpiar dimensiones
        IF OBJECT_ID('dwh.DimOrder', 'U') IS NOT NULL DELETE FROM dwh.DimOrder;
        IF OBJECT_ID('dwh.DimProduct', 'U') IS NOT NULL DELETE FROM dwh.DimProduct;
        IF OBJECT_ID('dwh.DimCustomer', 'U') IS NOT NULL DELETE FROM dwh.DimCustomer;
        IF OBJECT_ID('dwh.DimChannel', 'U') IS NOT NULL DELETE FROM dwh.DimChannel;
        IF OBJECT_ID('dwh.DimCategory', 'U') IS NOT NULL DELETE FROM dwh.DimCategory;
        IF OBJECT_ID('dwh.DimTime', 'U') IS NOT NULL DELETE FROM dwh.DimTime;
        IF OBJECT_ID('dwh.DimExchangeRate', 'U') IS NOT NULL DELETE FROM dwh.DimExchangeRate;
        
        -- Limpiar staging
        IF OBJECT_ID('staging.source_tracking', 'U') IS NOT NULL DELETE FROM staging.source_tracking;
        IF OBJECT_ID('staging.map_producto', 'U') IS NOT NULL DELETE FROM staging.map_producto;
        IF OBJECT_ID('staging.tipo_cambio', 'U') IS NOT NULL DELETE FROM staging.tipo_cambio;
        
        -- Resetear identidades (solo si existen)
        IF OBJECT_ID('dwh.DimCategory', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimCategory', RESEED, 0);
        IF OBJECT_ID('dwh.DimChannel', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimChannel', RESEED, 0);
        IF OBJECT_ID('dwh.DimCustomer', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimCustomer', RESEED, 0);
        IF OBJECT_ID('dwh.DimProduct', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimProduct', RESEED, 0);
        IF OBJECT_ID('dwh.DimOrder', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimOrder', RESEED, 0);
        IF OBJECT_ID('dwh.DimTime', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimTime', RESEED, 0);
        IF OBJECT_ID('dwh.DimExchangeRate', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.DimExchangeRate', RESEED, 0);
        IF OBJECT_ID('dwh.FactSales', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.FactSales', RESEED, 0);
        IF OBJECT_ID('dwh.FactTargetSales', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.FactTargetSales', RESEED, 0);
        IF OBJECT_ID('dwh.MetasVentas', 'U') IS NOT NULL DBCC CHECKIDENT ('dwh.MetasVentas', RESEED, 0);
        IF OBJECT_ID('staging.source_tracking', 'U') IS NOT NULL DBCC CHECKIDENT ('staging.source_tracking', RESEED, 0);
        IF OBJECT_ID('staging.map_producto', 'U') IS NOT NULL DBCC CHECKIDENT ('staging.map_producto', RESEED, 0);
        IF OBJECT_ID('staging.tipo_cambio', 'U') IS NOT NULL DBCC CHECKIDENT ('staging.tipo_cambio', RESEED, 0);
        
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
