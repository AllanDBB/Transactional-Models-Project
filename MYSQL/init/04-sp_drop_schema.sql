-- ============================================================================
-- 05-sp_drop_schema.sql
-- Stored Procedure para eliminar schema completo de BD Transaccional MySQL
-- ============================================================================

USE sales_mysql;

-- Eliminar procedimiento si existe
DROP PROCEDURE IF EXISTS sp_drop_schema;

-- Crear procedimiento
DELIMITER //

CREATE PROCEDURE sp_drop_schema()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT 'Error al eliminar schema' AS error_message;
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT '========================================' AS '';
    SELECT 'ELIMINANDO SCHEMA BD TRANSACCIONAL' AS '';
    SELECT '========================================' AS '';

    -- Eliminar tablas en orden (respetando FKs)
    DROP TABLE IF EXISTS OrdenDetalle;
    SELECT '[OK] Tabla OrdenDetalle eliminada' AS '';

    DROP TABLE IF EXISTS Orden;
    SELECT '[OK] Tabla Orden eliminada' AS '';

    DROP TABLE IF EXISTS Producto;
    SELECT '[OK] Tabla Producto eliminada' AS '';

    DROP TABLE IF EXISTS Cliente;
    SELECT '[OK] Tabla Cliente eliminada' AS '';

    COMMIT;

    SELECT '' AS '';
    SELECT '========================================' AS '';
    SELECT 'SCHEMA ELIMINADO EXITOSAMENTE' AS '';
    SELECT '========================================' AS '';
    SELECT 'Todas las tablas han sido eliminadas.' AS '';
    SELECT 'Use sp_init_schema para recrear.' AS '';
    SELECT '========================================' AS '';

END //

DELIMITER ;

SELECT '[OK] Stored Procedure sp_drop_schema creado exitosamente' AS '';
