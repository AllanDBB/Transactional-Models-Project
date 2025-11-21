-- ======================================
-- Tabla: Cliente
-- ======================================
CREATE TABLE Cliente (
  id INT AUTO_INCREMENT PRIMARY KEY,
  nombre VARCHAR(120) NOT NULL,
  correo VARCHAR(150),
  genero ENUM('M','F','X') DEFAULT 'M',         -- Heterogeneidad respecto a SQL Server
  pais VARCHAR(60) NOT NULL,
  created_at VARCHAR(10) NOT NULL               -- Formato 'YYYY-MM-DD' (parsing en ETL)
);

-- ======================================
-- Tabla: Producto
-- ======================================
CREATE TABLE Producto (
  id INT AUTO_INCREMENT PRIMARY KEY,
  codigo_alt VARCHAR(64) UNIQUE NOT NULL,       -- Código alterno (no coincide con SKU oficial)
  nombre VARCHAR(150) NOT NULL,
  categoria VARCHAR(80) NOT NULL
);

-- ======================================
-- Tabla: Orden
-- ======================================
CREATE TABLE Orden (
  id INT AUTO_INCREMENT PRIMARY KEY,
  cliente_id INT NOT NULL,
  fecha VARCHAR(19) NOT NULL,                   -- 'YYYY-MM-DD HH:MM:SS' (guardado como texto)
  canal VARCHAR(20) NOT NULL,                   -- Libre, sin control de dominio
  moneda CHAR(3) NOT NULL,                      -- Ej: 'USD' o 'CRC'
  total VARCHAR(20) NOT NULL,                   -- Puede contener comas o puntos (string)
  FOREIGN KEY (cliente_id) REFERENCES Cliente(id)
);

CREATE INDEX IX_Orden_cliente ON Orden(cliente_id);

-- ======================================
-- Tabla: OrdenDetalle
-- ======================================
CREATE TABLE OrdenDetalle (
  id INT AUTO_INCREMENT PRIMARY KEY,
  orden_id INT NOT NULL,
  producto_id INT NOT NULL,
  cantidad INT NOT NULL,
  precio_unit VARCHAR(20) NOT NULL,             -- String con comas/puntos
  FOREIGN KEY (orden_id) REFERENCES Orden(id),
  FOREIGN KEY (producto_id) REFERENCES Producto(id)
);

CREATE INDEX IX_Detalle_producto ON OrdenDetalle(producto_id);