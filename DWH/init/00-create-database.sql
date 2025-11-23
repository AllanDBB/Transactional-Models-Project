-- ============================================================================
-- 00-create-database.sql
-- Crear base de datos MSSQL_DW (debe ejecutarse primero)
-- ============================================================================

-- Crear base de datos si no existe
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'MSSQL_DW')
BEGIN
    CREATE DATABASE [MSSQL_DW];
    PRINT '[OK] Base de datos MSSQL_DW creada';
END
ELSE
BEGIN
    PRINT '[INFO] Base de datos MSSQL_DW ya existe';
END
GO
