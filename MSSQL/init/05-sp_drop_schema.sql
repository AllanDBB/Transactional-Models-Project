-- ============================================================================
-- 05-sp_drop_schema.sql
-- Stored Procedure para eliminar schema completo de BD Transaccional MSSQL
-- ============================================================================

USE SalesDB_MSSQL;
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
        PRINT 'ELIMINANDO SCHEMA BD TRANSACCIONAL';
        PRINT '========================================';
        
        -- Eliminar tablas en orden (respetando FKs)
        IF OBJECT_ID('sales_ms.OrdenDetalle', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.OrdenDetalle;
            PRINT '[OK] Tabla OrdenDetalle eliminada';
        END
        
        IF OBJECT_ID('sales_ms.Orden', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.Orden;
            PRINT '[OK] Tabla Orden eliminada';
        END
        
        IF OBJECT_ID('sales_ms.Producto', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.Producto;
            PRINT '[OK] Tabla Producto eliminada';
        END
        
        IF OBJECT_ID('sales_ms.Cliente', 'U') IS NOT NULL
        BEGIN
            DROP TABLE sales_ms.Cliente;
            PRINT '[OK] Tabla Cliente eliminada';
        END
        
        -- Eliminar schema si existe y está vacío
        IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales_ms')
        BEGIN
            -- Verificar que no haya objetos en el schema
            IF NOT EXISTS (
                SELECT * FROM sys.objects 
                WHERE schema_id = SCHEMA_ID('sales_ms')
            )
            BEGIN
                DROP SCHEMA sales_ms;
                PRINT '[OK] Schema sales_ms eliminado';
            END
            ELSE
            BEGIN
                PRINT '[WARN] Schema sales_ms contiene objetos, no se eliminó';
            END
        END
        
        PRINT '';
        PRINT '========================================';
        PRINT 'SCHEMA ELIMINADO EXITOSAMENTE';
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
