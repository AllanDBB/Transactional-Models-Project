-- ============================================================================
-- 00-sp_init_schema.sql
-- Stored Procedure para inicializar schema de BD Transaccional MSSQL
-- ============================================================================

USE SalesDB_MSSQL;
GO

-- Eliminar procedimiento si existe
IF OBJECT_ID('dbo.sp_init_schema', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_init_schema;
GO

-- Crear procedimiento
CREATE PROCEDURE dbo.sp_init_schema
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        PRINT '========================================';
        PRINT 'INICIALIZANDO SCHEMA BD TRANSACCIONAL';
        PRINT '========================================';
        
        -- Verificar si el schema ya existe
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales_ms')
        BEGIN
            EXEC('CREATE SCHEMA sales_ms');
            PRINT '[OK] Schema sales_ms creado';
        END
        ELSE
        BEGIN
            PRINT '[INFO] Schema sales_ms ya existe';
        END
        
        -- Eliminar tablas si existen (en orden inverso por FKs)
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
        
        -- Crear tabla Cliente
        CREATE TABLE sales_ms.Cliente (
            ClienteId INT IDENTITY PRIMARY KEY,
            Nombre NVARCHAR(120) NOT NULL,
            Email NVARCHAR(150) UNIQUE,
            Genero NVARCHAR(12) CHECK (Genero IN ('Masculino','Femenino')),
            Pais NVARCHAR(60) NOT NULL,
            FechaRegistro DATE NOT NULL DEFAULT (GETDATE())
        );
        PRINT '[OK] Tabla Cliente creada';
        
        -- Crear tabla Producto
        CREATE TABLE sales_ms.Producto (
            ProductoId INT IDENTITY PRIMARY KEY,
            SKU NVARCHAR(40) UNIQUE NOT NULL,
            Nombre NVARCHAR(150) NOT NULL,
            Categoria NVARCHAR(80) NOT NULL
        );
        PRINT '[OK] Tabla Producto creada';
        
        -- Crear tabla Orden
        CREATE TABLE sales_ms.Orden (
            OrdenId INT IDENTITY PRIMARY KEY,
            ClienteId INT NOT NULL FOREIGN KEY REFERENCES sales_ms.Cliente(ClienteId),
            Fecha DATETIME2 NOT NULL DEFAULT (SYSDATETIME()),
            Canal NVARCHAR(20) NOT NULL CHECK (Canal IN ('WEB','TIENDA','APP')),
            Moneda CHAR(3) NOT NULL DEFAULT 'USD',
            Total DECIMAL(18,2) NOT NULL
        );
        PRINT '[OK] Tabla Orden creada';
        
        -- Crear tabla OrdenDetalle
        CREATE TABLE sales_ms.OrdenDetalle (
            OrdenDetalleId INT IDENTITY PRIMARY KEY,
            OrdenId INT NOT NULL FOREIGN KEY REFERENCES sales_ms.Orden(OrdenId),
            ProductoId INT NOT NULL FOREIGN KEY REFERENCES sales_ms.Producto(ProductoId),
            Cantidad INT NOT NULL CHECK (Cantidad > 0),
            PrecioUnit DECIMAL(18,2) NOT NULL,
            DescuentoPct DECIMAL(5,2) NULL
        );
        PRINT '[OK] Tabla OrdenDetalle creada';
        
        -- Crear índices
        CREATE INDEX IX_Orden_Fecha ON sales_ms.Orden(Fecha);
        CREATE INDEX IX_Detalle_Prod ON sales_ms.OrdenDetalle(ProductoId);
        PRINT '[OK] Índices creados';
        
        PRINT '';
        PRINT '========================================';
        PRINT 'SCHEMA INICIALIZADO EXITOSAMENTE';
        PRINT '========================================';
        PRINT 'Tablas creadas:';
        PRINT '  - sales_ms.Cliente';
        PRINT '  - sales_ms.Producto';
        PRINT '  - sales_ms.Orden';
        PRINT '  - sales_ms.OrdenDetalle';
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
GRANT EXECUTE ON dbo.sp_init_schema TO public;
GO

PRINT '[OK] Stored Procedure dbo.sp_init_schema creado exitosamente';
GO
