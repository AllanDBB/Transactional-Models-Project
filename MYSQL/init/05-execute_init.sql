-- ============================================================================
-- 02-execute_init.sql
-- Ejecuta automáticamente los procedimientos almacenados al inicializar
-- ============================================================================

USE sales_mysql;

-- Ejecutar procedimiento para crear schema
CALL sp_init_schema();

-- Pequeña pausa para asegurar que el schema esté completo
SELECT SLEEP(1) AS 'Esperando inicialización del schema...';

-- Ejecutar procedimiento para generar datos de prueba
//TODO : Para temas del proyecto se debe triggear desde la UI
CALL sp_generar_datos();

SELECT '========================================' AS '';
SELECT 'INICIALIZACIÓN COMPLETADA EXITOSAMENTE' AS '';
SELECT '========================================' AS '';