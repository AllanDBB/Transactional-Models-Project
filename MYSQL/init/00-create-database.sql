-- ============================================
-- Crear base de datos sales_mysql (Transaccional)
-- ============================================

-- Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS sales_mysql
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

SELECT 'Base de datos sales_mysql creada exitosamente' AS '';

-- Seleccionar base de datos
USE sales_mysql;

SELECT 'Usando base de datos sales_mysql' AS '';
