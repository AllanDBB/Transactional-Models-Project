-- ============================================
-- Crear base de datos SalesDB_MSSQL (Transaccional)
-- ============================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SalesDB_MSSQL')
BEGIN
    CREATE DATABASE SalesDB_MSSQL;
    PRINT 'Base de datos SalesDB_MSSQL creada exitosamente';
END
ELSE
BEGIN
    PRINT 'Base de datos SalesDB_MSSQL ya existe';
END
GO

USE SalesDB_MSSQL;
GO

PRINT 'Usando base de datos SalesDB_MSSQL';
GO
