-- ============================================================================
-- 03-sp_limpiar_bd.sql
-- Stored Procedure para limpiar BD Transaccional MySQL
-- ============================================================================

USE sales_mysql;

-- Eliminar procedimiento si existe
DROP PROCEDURE IF EXISTS sp_limpiar_bd;

-- Crear procedimiento
DELIMITER //

CREATE PROCEDURE sp_limpiar_bd()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT 'Error al limpiar la base de datos' AS error_message;
        ROLLBACK;
    END;

    START TRANSACTION;

    -- Limpiar datos en orden (respetando FKs)
    DELETE FROM OrdenDetalle;
    DELETE FROM Orden;
    DELETE FROM Producto;
    DELETE FROM Cliente;

    -- Resetear AUTO_INCREMENT
    ALTER TABLE Cliente AUTO_INCREMENT = 1;
    ALTER TABLE Producto AUTO_INCREMENT = 1;
    ALTER TABLE Orden AUTO_INCREMENT = 1;
    ALTER TABLE OrdenDetalle AUTO_INCREMENT = 1;

    COMMIT;

    -- Resumen
    SELECT '========================================' AS '';
    SELECT 'BD TRANSACCIONAL LIMPIADA' AS '';
    SELECT '========================================' AS '';
    SELECT 'Clientes:  0' AS '';
    SELECT 'Productos: 0' AS '';
    SELECT 'Órdenes:   0' AS '';
    SELECT 'Detalles:  0' AS '';
    SELECT '========================================' AS '';

END //

DELIMITER ;

SELECT '[OK] Stored Procedure sp_limpiar_bd creado exitosamente' AS '';
