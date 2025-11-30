-- ============================================================================
-- 00-sp_init_schema.sql
-- Stored Procedure para inicializar schema de BD Transaccional MySQL
-- ============================================================================

USE sales_mysql;

-- Eliminar procedimiento si existe
DROP PROCEDURE IF EXISTS sp_init_schema;

-- Crear procedimiento
DELIMITER //

CREATE PROCEDURE sp_init_schema()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT 'Error al inicializar schema' AS error_message;
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT '========================================' AS '';
    SELECT 'INICIALIZANDO SCHEMA BD TRANSACCIONAL' AS '';
    SELECT '========================================' AS '';

    -- Eliminar tablas si existen (en orden inverso por FKs)
    DROP TABLE IF EXISTS OrdenDetalle;
    SELECT '[OK] Tabla OrdenDetalle eliminada (si existía)' AS '';

    DROP TABLE IF EXISTS Orden;
    SELECT '[OK] Tabla Orden eliminada (si existía)' AS '';

    DROP TABLE IF EXISTS Producto;
    SELECT '[OK] Tabla Producto eliminada (si existía)' AS '';

    DROP TABLE IF EXISTS Cliente;
    SELECT '[OK] Tabla Cliente eliminada (si existía)' AS '';

    -- Crear tabla Cliente
    CREATE TABLE Cliente (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(120) NOT NULL,
        correo VARCHAR(150),
        genero ENUM('M','F','X') DEFAULT 'M',
        pais VARCHAR(60) NOT NULL,
        created_at VARCHAR(10) NOT NULL,
        INDEX IX_Cliente_correo (correo)
    );
    SELECT '[OK] Tabla Cliente creada' AS '';

    -- Crear tabla Producto
    CREATE TABLE Producto (
        id INT AUTO_INCREMENT PRIMARY KEY,
        codigo_alt VARCHAR(64) UNIQUE NOT NULL,
        nombre VARCHAR(150) NOT NULL,
        categoria VARCHAR(80) NOT NULL,
        INDEX IX_Producto_categoria (categoria)
    );
    SELECT '[OK] Tabla Producto creada' AS '';

    -- Crear tabla Orden
    CREATE TABLE Orden (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cliente_id INT NOT NULL,
        fecha VARCHAR(19) NOT NULL,
        canal VARCHAR(20) NOT NULL,
        moneda CHAR(3) NOT NULL,
        total VARCHAR(20) NOT NULL,
        FOREIGN KEY (cliente_id) REFERENCES Cliente(id),
        INDEX IX_Orden_cliente (cliente_id),
        INDEX IX_Orden_fecha (fecha)
    );
    SELECT '[OK] Tabla Orden creada' AS '';

    -- Crear tabla OrdenDetalle
    CREATE TABLE OrdenDetalle (
        id INT AUTO_INCREMENT PRIMARY KEY,
        orden_id INT NOT NULL,
        producto_id INT NOT NULL,
        cantidad INT NOT NULL,
        precio_unit VARCHAR(20) NOT NULL,
        FOREIGN KEY (orden_id) REFERENCES Orden(id),
        FOREIGN KEY (producto_id) REFERENCES Producto(id),
        INDEX IX_OrdenDetalle_orden (orden_id),
        INDEX IX_OrdenDetalle_producto (producto_id)
    );
    SELECT '[OK] Tabla OrdenDetalle creada' AS '';

    COMMIT;

    SELECT '' AS '';
    SELECT '========================================' AS '';
    SELECT 'SCHEMA INICIALIZADO EXITOSAMENTE' AS '';
    SELECT '========================================' AS '';
    SELECT 'Tablas creadas:' AS '';
    SELECT '  - Cliente' AS '';
    SELECT '  - Producto' AS '';
    SELECT '  - Orden' AS '';
    SELECT '  - OrdenDetalle' AS '';
    SELECT '========================================' AS '';

END //

DELIMITER ;

SELECT '[OK] Stored Procedure sp_init_schema creado exitosamente' AS '';
