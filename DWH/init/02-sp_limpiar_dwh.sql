-- ============================================================================
-- 02-sp_limpiar_dwh.sql
-- Stored Procedure para limpiar todas las tablas del DWH
-- ============================================================================

USE MSSQL_DW;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('dbo.sp_limpiar_dwh', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_limpiar_dwh;
GO

-- Crear procedimiento
CREATE PROCEDURE dbo.sp_limpiar_dwh
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Limpiar tablas de hechos (primero por FKs)
        DELETE FROM FactTargetSales;
        DELETE FROM MetasVentas;
        DELETE FROM FactSales;
        
        -- Limpiar dimensiones
        DELETE FROM DimOrder;
        DELETE FROM DimProduct;
        DELETE FROM DimCustomer;
        DELETE FROM DimChannel;
        DELETE FROM DimCategory;
        DELETE FROM DimTime;
        -- NO limpiamos DimExchangeRate (datos del BCCR)
        
        -- Limpiar staging
        DELETE FROM staging_source_tracking;
        DELETE FROM staging_map_producto;
        -- NO limpiamos staging_tipo_cambio (datos del BCCR)
        
        -- Resetear identidades
        DBCC CHECKIDENT ('DimCategory', RESEED, 0);
        DBCC CHECKIDENT ('DimChannel', RESEED, 0);
        DBCC CHECKIDENT ('DimCustomer', RESEED, 0);
        DBCC CHECKIDENT ('DimProduct', RESEED, 0);
        DBCC CHECKIDENT ('DimOrder', RESEED, 0);
        DBCC CHECKIDENT ('FactSales', RESEED, 0);
        DBCC CHECKIDENT ('FactTargetSales', RESEED, 0);
        DBCC CHECKIDENT ('MetasVentas', RESEED, 0);
        DBCC CHECKIDENT ('staging_source_tracking', RESEED, 0);
        DBCC CHECKIDENT ('staging_map_producto', RESEED, 0);
        
        COMMIT TRANSACTION;
        
        -- Resumen
        PRINT '========================================';
        PRINT 'DATA WAREHOUSE LIMPIADO';
        PRINT '========================================';
        PRINT 'Dimensiones: 0 registros';
        PRINT 'Hechos:      0 registros';
        PRINT 'Staging:     0 registros';
        PRINT 'Tipos Cambio: PRESERVADOS';
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
GRANT EXECUTE ON dbo.sp_limpiar_dwh TO public;
GO

PRINT '[OK] Stored Procedure dbo.sp_limpiar_dwh creado exitosamente';
GO
