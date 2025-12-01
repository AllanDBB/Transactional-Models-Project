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
        DELETE FROM dwh.FactTargetSales;
        DELETE FROM dwh.MetasVentas;
        DELETE FROM dwh.FactSales;
        
        -- Limpiar dimensiones
        DELETE FROM dwh.DimOrder;
        DELETE FROM dwh.DimProduct;
        DELETE FROM dwh.DimCustomer;
        DELETE FROM dwh.DimChannel;
        DELETE FROM dwh.DimCategory;
        DELETE FROM dwh.DimTime;
        -- NO limpiamos dwh.DimExchangeRate (datos del BCCR)
        
        -- Limpiar staging
        DELETE FROM staging.source_tracking;
        DELETE FROM staging.map_producto;
        -- MongoDB
        DELETE FROM staging.mongo_orders;
        DELETE FROM staging.mongo_order_items;
        DELETE FROM staging.mongo_customers;
        -- MSSQL Source
        DELETE FROM staging.mssql_customers;
        DELETE FROM staging.mssql_products;
        DELETE FROM staging.mssql_sales;
        -- MySQL
        DELETE FROM staging.mysql_customers;
        DELETE FROM staging.mysql_products;
        DELETE FROM staging.mysql_sales;
        -- Neo4j
        DELETE FROM staging.neo4j_nodes;
        DELETE FROM staging.neo4j_edges;
        DELETE FROM staging.neo4j_order_items;
        -- Supabase
        DELETE FROM staging.supabase_users;
        DELETE FROM staging.supabase_products;
        DELETE FROM staging.supabase_orders;
        DELETE FROM staging.supabase_order_items;
        -- NO limpiamos staging.tipo_cambio (datos del BCCR preservados)
        
        -- Resetear identidades (DimTime no tiene IDENTITY)
        DBCC CHECKIDENT ('dwh.DimCategory', RESEED, 0);
        DBCC CHECKIDENT ('dwh.DimChannel', RESEED, 0);
        DBCC CHECKIDENT ('dwh.DimCustomer', RESEED, 0);
        DBCC CHECKIDENT ('dwh.DimProduct', RESEED, 0);
        DBCC CHECKIDENT ('dwh.DimOrder', RESEED, 0);
        DBCC CHECKIDENT ('dwh.FactSales', RESEED, 0);
        DBCC CHECKIDENT ('dwh.FactTargetSales', RESEED, 0);
        DBCC CHECKIDENT ('dwh.MetasVentas', RESEED, 0);
        DBCC CHECKIDENT ('staging.source_tracking', RESEED, 0);
        DBCC CHECKIDENT ('staging.map_producto', RESEED, 0);
        DBCC CHECKIDENT ('staging.tipo_cambio', RESEED, 0);
        -- MongoDB
        DBCC CHECKIDENT ('staging.mongo_orders', RESEED, 0);
        DBCC CHECKIDENT ('staging.mongo_order_items', RESEED, 0);
        DBCC CHECKIDENT ('staging.mongo_customers', RESEED, 0);
        -- MSSQL Source
        DBCC CHECKIDENT ('staging.mssql_customers', RESEED, 0);
        DBCC CHECKIDENT ('staging.mssql_products', RESEED, 0);
        DBCC CHECKIDENT ('staging.mssql_sales', RESEED, 0);
        -- MySQL
        DBCC CHECKIDENT ('staging.mysql_customers', RESEED, 0);
        DBCC CHECKIDENT ('staging.mysql_products', RESEED, 0);
        DBCC CHECKIDENT ('staging.mysql_sales', RESEED, 0);
        -- Neo4j
        DBCC CHECKIDENT ('staging.neo4j_nodes', RESEED, 0);
        DBCC CHECKIDENT ('staging.neo4j_edges', RESEED, 0);
        DBCC CHECKIDENT ('staging.neo4j_order_items', RESEED, 0);
        -- Supabase
        DBCC CHECKIDENT ('staging.supabase_users', RESEED, 0);
        DBCC CHECKIDENT ('staging.supabase_products', RESEED, 0);
        DBCC CHECKIDENT ('staging.supabase_orders', RESEED, 0);
        DBCC CHECKIDENT ('staging.supabase_order_items', RESEED, 0);
        
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
