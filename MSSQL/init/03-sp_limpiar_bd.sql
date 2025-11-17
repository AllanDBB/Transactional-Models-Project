-- ============================================================================
-- 03-sp_limpiar_bd.sql
-- Stored Procedure para limpiar BD Transaccional MSSQL
-- ============================================================================

USE SalesDB_MSSQL;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('sales_ms.sp_limpiar_bd', 'P') IS NOT NULL
    DROP PROCEDURE sales_ms.sp_limpiar_bd;
GO

-- Crear procedimiento
CREATE PROCEDURE sales_ms.sp_limpiar_bd
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Limpiar datos en orden (respetando FKs)
        DELETE FROM sales_ms.OrdenDetalle;
        DELETE FROM sales_ms.Orden;
        DELETE FROM sales_ms.Producto;
        DELETE FROM sales_ms.Cliente;
        
        -- Resetear identidades
        DBCC CHECKIDENT ('sales_ms.Cliente', RESEED, 0);
        DBCC CHECKIDENT ('sales_ms.Producto', RESEED, 0);
        DBCC CHECKIDENT ('sales_ms.Orden', RESEED, 0);
        DBCC CHECKIDENT ('sales_ms.OrdenDetalle', RESEED, 0);
        
        COMMIT TRANSACTION;
        
        -- Resumen
        PRINT '========================================';
        PRINT 'BD TRANSACCIONAL LIMPIADA';
        PRINT '========================================';
        PRINT 'Clientes:  0';
        PRINT 'Productos: 0';
        PRINT 'Órdenes:   0';
        PRINT 'Detalles:  0';
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
GRANT EXECUTE ON sales_ms.sp_limpiar_bd TO public;
GO

PRINT '[OK] Stored Procedure sales_ms.sp_limpiar_bd creado exitosamente';
GO
