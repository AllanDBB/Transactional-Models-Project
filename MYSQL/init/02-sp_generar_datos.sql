-- ============================================================================
-- 04-sp_generar_datos.sql
-- Stored Procedure para generar datos de prueba en BD Transaccional MySQL
-- Genera ~600 clientes, ~500 productos, ~5000 órdenes, ~25000 detalles
-- ============================================================================

USE sales_mysql;

-- Eliminar procedimiento si existe
DROP PROCEDURE IF EXISTS sp_generar_datos;

-- Crear procedimiento
DELIMITER //

CREATE PROCEDURE sp_generar_datos()
BEGIN
    DECLARE v_counter INT DEFAULT 1;
    DECLARE v_max_clientes INT;
    DECLARE v_max_productos INT;
    DECLARE v_num_ordenes INT DEFAULT 5000;
    DECLARE v_cliente_id INT;
    DECLARE v_producto_id INT;
    DECLARE v_cantidad INT;
    DECLARE v_precio_unit DECIMAL(18,2);
    DECLARE v_total DECIMAL(18,2);
    DECLARE v_fecha VARCHAR(19);
    DECLARE v_canal VARCHAR(20);
    DECLARE v_moneda VARCHAR(3);
    DECLARE v_orden_id INT;
    DECLARE v_num_detalles INT;
    DECLARE v_detalles_counter INT;
    DECLARE v_base_precio DECIMAL(18,2);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT 'Error durante la generación de datos' AS error_message;
        ROLLBACK;
    END;

    START TRANSACTION;

    SELECT '========================================' AS '';
    SELECT 'GENERANDO DATOS DE PRUEBA' AS '';
    SELECT '========================================' AS '';

    -- ==========================================
    -- 1. GENERAR CLIENTES (600)
    -- ==========================================
    INSERT INTO Cliente (nombre, correo, genero, pais, created_at)
    SELECT
        CONCAT(
            CASE
                WHEN (t.id % 42) = 1 THEN 'Juan García'
                WHEN (t.id % 42) = 2 THEN 'María López'
                WHEN (t.id % 42) = 3 THEN 'Carlos Rodríguez'
                WHEN (t.id % 42) = 4 THEN 'Ana Martínez'
                WHEN (t.id % 42) = 5 THEN 'Pedro Sánchez'
                WHEN (t.id % 42) = 6 THEN 'Laura Hernández'
                WHEN (t.id % 42) = 7 THEN 'Miguel Flores'
                WHEN (t.id % 42) = 8 THEN 'Isabel Mora'
                WHEN (t.id % 42) = 9 THEN 'Diego Ramírez'
                WHEN (t.id % 42) = 10 THEN 'Elena Vega'
                WHEN (t.id % 42) = 11 THEN 'Antonio Díaz'
                WHEN (t.id % 42) = 12 THEN 'Rosa Jiménez'
                WHEN (t.id % 42) = 13 THEN 'Fernando Acosta'
                WHEN (t.id % 42) = 14 THEN 'Sofía Campos'
                WHEN (t.id % 42) = 15 THEN 'Raúl Ortiz'
                WHEN (t.id % 42) = 16 THEN 'Gabriela Brenes'
                WHEN (t.id % 42) = 17 THEN 'Andrés Castro'
                WHEN (t.id % 42) = 18 THEN 'Patricia Solís'
                WHEN (t.id % 42) = 19 THEN 'Ricardo Medina'
                WHEN (t.id % 42) = 20 THEN 'Cristina Arias'
                WHEN (t.id % 42) = 21 THEN 'Julio Delgado'
                WHEN (t.id % 42) = 22 THEN 'Sandra Vargas'
                WHEN (t.id % 42) = 23 THEN 'Hector Zamora'
                WHEN (t.id % 42) = 24 THEN 'Verónica Segura'
                WHEN (t.id % 42) = 25 THEN 'Eduardo Navarro'
                WHEN (t.id % 42) = 26 THEN 'Margarita Rojas'
                WHEN (t.id % 42) = 27 THEN 'Sergio Quirós'
                WHEN (t.id % 42) = 28 THEN 'Beatriz Reyes'
                WHEN (t.id % 42) = 29 THEN 'Javier Montoya'
                WHEN (t.id % 42) = 30 THEN 'Dolores Aguilar'
                WHEN (t.id % 42) = 31 THEN 'Samuel Vindas'
                WHEN (t.id % 42) = 32 THEN 'Catalina Chaves'
                WHEN (t.id % 42) = 33 THEN 'Vicente Gómez'
                WHEN (t.id % 42) = 34 THEN 'Marta Durán'
                WHEN (t.id % 42) = 35 THEN 'Bonifacio Araya'
                WHEN (t.id % 42) = 36 THEN 'Lorena Espinoza'
                WHEN (t.id % 42) = 37 THEN 'Octavio Paniagua'
                WHEN (t.id % 42) = 38 THEN 'Valentina Soto'
                WHEN (t.id % 42) = 39 THEN 'Gustavo Benavides'
                WHEN (t.id % 42) = 40 THEN 'Adriana Salazar'
                WHEN (t.id % 42) = 41 THEN 'Leopoldo Cordero'
                ELSE 'Pilar González'
            END,
            ' ',
            LPAD(FLOOR(t.id / 42) + 1, 3, '0')
        ) AS nombre,
        LOWER(CONCAT(
            REPLACE(
                CASE
                    WHEN (t.id % 42) = 1 THEN 'juan.garcia'
                    WHEN (t.id % 42) = 2 THEN 'maria.lopez'
                    WHEN (t.id % 42) = 3 THEN 'carlos.rodriguez'
                    WHEN (t.id % 42) = 4 THEN 'ana.martinez'
                    WHEN (t.id % 42) = 5 THEN 'pedro.sanchez'
                    WHEN (t.id % 42) = 6 THEN 'laura.hernandez'
                    WHEN (t.id % 42) = 7 THEN 'miguel.flores'
                    WHEN (t.id % 42) = 8 THEN 'isabel.mora'
                    WHEN (t.id % 42) = 9 THEN 'diego.ramirez'
                    WHEN (t.id % 42) = 10 THEN 'elena.vega'
                    WHEN (t.id % 42) = 11 THEN 'antonio.diaz'
                    WHEN (t.id % 42) = 12 THEN 'rosa.jimenez'
                    WHEN (t.id % 42) = 13 THEN 'fernando.acosta'
                    WHEN (t.id % 42) = 14 THEN 'sofia.campos'
                    WHEN (t.id % 42) = 15 THEN 'raul.ortiz'
                    WHEN (t.id % 42) = 16 THEN 'gabriela.brenes'
                    WHEN (t.id % 42) = 17 THEN 'andres.castro'
                    WHEN (t.id % 42) = 18 THEN 'patricia.solis'
                    WHEN (t.id % 42) = 19 THEN 'ricardo.medina'
                    WHEN (t.id % 42) = 20 THEN 'cristina.arias'
                    WHEN (t.id % 42) = 21 THEN 'julio.delgado'
                    WHEN (t.id % 42) = 22 THEN 'sandra.vargas'
                    WHEN (t.id % 42) = 23 THEN 'hector.zamora'
                    WHEN (t.id % 42) = 24 THEN 'veronica.segura'
                    WHEN (t.id % 42) = 25 THEN 'eduardo.navarro'
                    WHEN (t.id % 42) = 26 THEN 'margarita.rojas'
                    WHEN (t.id % 42) = 27 THEN 'sergio.quiros'
                    WHEN (t.id % 42) = 28 THEN 'beatriz.reyes'
                    WHEN (t.id % 42) = 29 THEN 'javier.montoya'
                    WHEN (t.id % 42) = 30 THEN 'dolores.aguilar'
                    WHEN (t.id % 42) = 31 THEN 'samuel.vindas'
                    WHEN (t.id % 42) = 32 THEN 'catalina.chaves'
                    WHEN (t.id % 42) = 33 THEN 'vicente.gomez'
                    WHEN (t.id % 42) = 34 THEN 'marta.duran'
                    WHEN (t.id % 42) = 35 THEN 'bonifacio.araya'
                    WHEN (t.id % 42) = 36 THEN 'lorena.espinoza'
                    WHEN (t.id % 42) = 37 THEN 'octavio.paniagua'
                    WHEN (t.id % 42) = 38 THEN 'valentina.soto'
                    WHEN (t.id % 42) = 39 THEN 'gustavo.benavides'
                    WHEN (t.id % 42) = 40 THEN 'adriana.salazar'
                    WHEN (t.id % 42) = 41 THEN 'leopoldo.cordero'
                    ELSE 'pilar.gonzalez'
                END,
                ' ',
                ''
            ),
            LPAD(FLOOR(t.id / 42) + 1, 3, '0'),
            '@example.com'
        )) AS correo,
        CASE
            WHEN (t.id % 2) = 0 THEN 'M'
            ELSE 'F'
        END AS genero,
        CASE
            WHEN t.id % 3 = 0 THEN 'Costa Rica'
            WHEN t.id % 3 = 1 THEN 'Panamá'
            ELSE 'Nicaragua'
        END AS pais,
        DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL (t.id % 365) DAY), '%Y-%m-%d') AS created_at
    FROM (
        SELECT (@row:=@row+1) AS id
        FROM (SELECT @row:=0) init,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t1,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t2,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t3,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) t4,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6) t5
        LIMIT 600
    ) t;

    SELECT '[OK] 600 Clientes generados' AS '';

    -- ==========================================
    -- 2. GENERAR PRODUCTOS (500)
    -- ==========================================
    INSERT INTO Producto (codigo_alt, nombre, categoria)
    SELECT
        CONCAT('ALT-', LPAD(t.id, 5, '0')) AS codigo_alt,
        CASE
            WHEN (t.id % 5) = 0 THEN CONCAT('Laptop Pro ', t.id)
            WHEN (t.id % 5) = 1 THEN CONCAT('Monitor 27" ', t.id)
            WHEN (t.id % 5) = 2 THEN CONCAT('Teclado Mecánico ', t.id)
            WHEN (t.id % 5) = 3 THEN CONCAT('Mouse Inalámbrico ', t.id)
            ELSE CONCAT('Webcam HD ', t.id)
        END AS nombre,
        CASE
            WHEN (t.id % 5) = 0 THEN 'Computadoras'
            WHEN (t.id % 5) = 1 THEN 'Periféricos de Visualización'
            WHEN (t.id % 5) = 2 THEN 'Periféricos de Entrada'
            WHEN (t.id % 5) = 3 THEN 'Accesorios'
            ELSE 'Accesorios'
        END AS categoria
    FROM (
        SELECT (@prow:=@prow+1) AS id
        FROM (SELECT @prow:=0) init,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) pt1,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) pt2,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
              UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) pt3,
             (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) pt4
        LIMIT 500
    ) t;

    SELECT '[OK] 500 Productos generados' AS '';

    -- ==========================================
    -- 3. OBTENER MÁXIMOS
    -- ==========================================
    SELECT MAX(id) INTO v_max_clientes FROM Cliente;
    SELECT MAX(id) INTO v_max_productos FROM Producto;

    -- ==========================================
    -- 4. GENERAR ÓRDENES Y DETALLES
    -- ==========================================
    SET v_counter = 1;

    WHILE v_counter <= v_num_ordenes DO
        -- Cliente aleatorio
        SET v_cliente_id = (v_counter % v_max_clientes) + 1;

        -- Fecha en rango 2024-2025
        SET v_fecha = DATE_FORMAT(
            DATE_SUB(CURDATE(), INTERVAL (v_counter % 730) DAY),
            '%Y-%m-%d %H:%i:%s'
        );

        -- Canal aleatorio
        SET v_canal = CASE
            WHEN (v_counter % 3) = 0 THEN 'WEB'
            WHEN (v_counter % 3) = 1 THEN 'TIENDA'
            ELSE 'APP'
        END;

        -- Moneda mixta
        SET v_moneda = CASE
            WHEN (v_counter % 2) = 0 THEN 'USD'
            ELSE 'CRC'
        END;

        -- Generar total temporal (será calculado después)
        SET v_total = '0.00';

        -- Insertar orden
        INSERT INTO Orden (cliente_id, fecha, canal, moneda, total)
        VALUES (v_cliente_id, v_fecha, v_canal, v_moneda, v_total);

        SET v_orden_id = LAST_INSERT_ID();

        -- Número de detalles (2-5 items por orden)
        SET v_num_detalles = 2 + (v_counter % 4);
        SET v_detalles_counter = 0;
        SET v_total = 0;

        -- Generar detalles para esta orden
        WHILE v_detalles_counter < v_num_detalles DO
            SET v_producto_id = ((v_counter + v_detalles_counter) % v_max_productos) + 1;
            SET v_cantidad = 1 + (v_detalles_counter % 5);

            -- Generar precio base según producto
            SET v_base_precio = 50 + ((v_producto_id % 100) * 10);
            SET v_precio_unit = v_base_precio;

            -- Formato con posible coma/punto para simular heterogeneidad
            IF (v_detalles_counter % 2) = 0 THEN
                -- Formato con coma: "1,250.50"
                INSERT INTO OrdenDetalle (orden_id, producto_id, cantidad, precio_unit)
                VALUES (v_orden_id, v_producto_id, v_cantidad,
                        REPLACE(FORMAT(v_precio_unit, 2), '.', ','));
            ELSE
                -- Formato con punto: "1250.50"
                INSERT INTO OrdenDetalle (orden_id, producto_id, cantidad, precio_unit)
                VALUES (v_orden_id, v_producto_id, v_cantidad,
                        FORMAT(v_precio_unit, 2));
            END IF;

            SET v_total = v_total + (v_cantidad * v_precio_unit);
            SET v_detalles_counter = v_detalles_counter + 1;
        END WHILE;

        -- Actualizar total de la orden (con formato de string)
        IF (v_counter % 3) = 0 THEN
            -- Formato con coma: "1,250.50"
            UPDATE Orden SET total = REPLACE(FORMAT(v_total, 2), '.', ',')
            WHERE id = v_orden_id;
        ELSE
            -- Formato con punto: "1250.50"
            UPDATE Orden SET total = FORMAT(v_total, 2)
            WHERE id = v_orden_id;
        END IF;

        SET v_counter = v_counter + 1;

        -- Log cada 500 órdenes
        IF (v_counter % 500) = 0 THEN
            SELECT CONCAT('[OK] ', v_counter, ' órdenes generadas') AS '';
        END IF;
    END WHILE;

    COMMIT;

    SELECT '' AS '';
    SELECT '========================================' AS '';
    SELECT 'DATOS GENERADOS EXITOSAMENTE' AS '';
    SELECT '========================================' AS '';
    SELECT 'Clientes:          600' AS '';
    SELECT 'Productos:         500' AS '';
    SELECT 'Órdenes:           5,000' AS '';
    SELECT 'Detalles generados: ~25,000' AS '';
    SELECT '========================================' AS '';

END //

DELIMITER ;

SELECT '[OK] Stored Procedure sp_generar_datos creado exitosamente' AS '';
