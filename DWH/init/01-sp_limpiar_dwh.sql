-- ============================================================================
-- 01-sp_limpiar_dwh.sql
-- Stored Procedure para limpiar DWH completo
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
        
        PRINT '========================================';
        PRINT 'LIMPIANDO DWH...';
        PRINT '========================================';
        
        -- Limpiar tabla de hechos primero
        DELETE FROM FactSales;
        PRINT '[OK] FactSales limpiada';
        
        -- Limpiar dimensiones
        DELETE FROM DimOrder;
        PRINT '[OK] DimOrder limpiada';
        
        DELETE FROM DimCustomer;
        PRINT '[OK] DimCustomer limpiada';
        
        DELETE FROM DimProduct;
        PRINT '[OK] DimProduct limpiada';
        
        DELETE FROM DimChannel;
        PRINT '[OK] DimChannel limpiada';
        
        DELETE FROM DimCategory;
        PRINT '[OK] DimCategory limpiada';
        
        DELETE FROM DimTime;
        PRINT '[OK] DimTime limpiada';
        
        -- Limpiar tablas de staging
        DELETE FROM staging_map_producto;
        PRINT '[OK] staging_map_producto limpiada';
        
        DELETE FROM staging_tipo_cambio;
        PRINT '[OK] staging_tipo_cambio limpiada';
        
        DELETE FROM staging_source_tracking;
        PRINT '[OK] staging_source_tracking limpiada';
        
        -- Resetear identidades de dimensiones
        DBCC CHECKIDENT ('DimCustomer', RESEED, 0);
        DBCC CHECKIDENT ('DimProduct', RESEED, 0);
        DBCC CHECKIDENT ('DimCategory', RESEED, 0);
        DBCC CHECKIDENT ('DimChannel', RESEED, 0);
        DBCC CHECKIDENT ('DimOrder', RESEED, 0);
        DBCC CHECKIDENT ('FactSales', RESEED, 0);
        
        COMMIT TRANSACTION;
        
        -- Resumen final
        PRINT '';
        PRINT '========================================';
        PRINT 'DWH COMPLETAMENTE LIMPIADO';
        PRINT '========================================';
        
        DECLARE @CountFactSales INT, @CountCustomer INT, @CountProduct INT;
        DECLARE @CountCategory INT, @CountChannel INT, @CountTime INT;
        DECLARE @CountOrder INT, @CountStaging INT;
        
        SELECT @CountFactSales = COUNT(*) FROM FactSales;
        SELECT @CountCustomer = COUNT(*) FROM DimCustomer;
        SELECT @CountProduct = COUNT(*) FROM DimProduct;
        SELECT @CountCategory = COUNT(*) FROM DimCategory;
        SELECT @CountChannel = COUNT(*) FROM DimChannel;
        SELECT @CountTime = COUNT(*) FROM DimTime;
        SELECT @CountOrder = COUNT(*) FROM DimOrder;
        SELECT @CountStaging = COUNT(*) FROM staging_map_producto;
        
        PRINT 'FactSales:     ' + CAST(@CountFactSales AS NVARCHAR(10));
        PRINT 'DimCustomer:   ' + CAST(@CountCustomer AS NVARCHAR(10));
        PRINT 'DimProduct:    ' + CAST(@CountProduct AS NVARCHAR(10));
        PRINT 'DimCategory:   ' + CAST(@CountCategory AS NVARCHAR(10));
        PRINT 'DimChannel:    ' + CAST(@CountChannel AS NVARCHAR(10));
        PRINT 'DimTime:       ' + CAST(@CountTime AS NVARCHAR(10));
        PRINT 'DimOrder:      ' + CAST(@CountOrder AS NVARCHAR(10));
        PRINT 'Staging:       ' + CAST(@CountStaging AS NVARCHAR(10));
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
