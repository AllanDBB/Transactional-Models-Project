# ETL Usage Guide

## Quick Start

### 1. Instalación de Dependencias
```bash
cd MYSQL/etl
pip install -r requirements.txt
```

### 2. Configuración del Ambiente
```bash
# Copiar template de configuración
cp .env.example .env
```

### 3. Ejecutar ETL
```bash
python run_etl.py
```

El ETL creará un archivo de log en `logs/etl_mysql_YYYYMMDD_HHMMSS.log`

---

## Escenarios de Uso

### Escenario 1: Primer Carga (Fresh Load)
**Situación:** Primera vez cargando datos al DWH, o quieres empezar desde cero.

**Pasos:**

1. Limpiar DWH existente (Opcional pero recomendado):
```sql
-- Ejecutar en MSSQL DW
TRUNCATE TABLE FactSales
TRUNCATE TABLE DimProduct
TRUNCATE TABLE DimCustomer
TRUNCATE TABLE DimTime
TRUNCATE TABLE DimChannel
TRUNCATE TABLE DimCategory
```

2. O descomenta en `run_etl.py` línea ~205:
```python
loader.truncate_tables(['FactSales', 'DimProduct', 'DimCustomer', 'DimChannel', 'DimCategory', 'DimTime'])
```

3. Ejecutar ETL:
```bash
python run_etl.py
```

**Resultado esperado:**
```
Categories: 10 total (10 new)
Channels: 5 total (5 new)
Customers: 150 total (150 new)
Dates: 365 total (365 new)
Products: 200 total (200 new)
Orders: 500 total (500 new)
FactSales: 2500 inserted, 0 skipped
```

---

### Escenario 2: Carga Incremental (Append Mode)
**Situación:** El DWH ya tiene datos, quieres agregar nuevos registros sin duplicar.

**Pasos:**

1. Mantén comentada la línea `truncate_tables()`:
```python
# loader.truncate_tables([...])  # Comentada
```

2. Ejecutar ETL:
```bash
python run_etl.py
```

**El ETL automáticamente:**
- ✅ Detecta categorías existentes y reutiliza sus IDs
- ✅ Detecta clientes existentes (por email) y evita duplicados
- ✅ Detecta productos existentes (por SKU) y evita duplicados
- ✅ Salta líneas de FactSales si producto/cliente/orden no existen
- ✅ Loguea qué registros se reutilizaron vs. se insertaron

**Resultado esperado:**
```
Categories: 10 total (0 new)        # Todas ya existen
Channels: 5 total (0 new)           # Todas ya existen
Customers: 155 total (5 new)        # 5 clientes nuevos
Dates: 380 total (15 new)           # 15 días nuevos
Products: 200 total (0 new)         # Todos ya existen
Orders: 520 total (20 new)          # 20 órdenes nuevas
FactSales: 2600 inserted, 5 skipped # 5 líneas saltadas por FK
```

---

### Escenario 3: Re-ejecución Segura (Multiple Runs)
**Situación:** Ejecutar ETL múltiples veces sin cambios en datos (testing, validación).

**Pasos:**

```bash
python run_etl.py  # Run 1
python run_etl.py  # Run 2 (seguro, sin duplicados)
python run_etl.py  # Run 3 (seguro, sin duplicados)
```

**Garantía del ETL:**
- ✅ Las dimensiones no se duplican (idempotentes)
- ✅ Los mappings reutilisan IDs existentes
- ✅ FactSales reutiliza órdenes existentes
- ✅ Logs muestran 0 nuevos registros en Run 2 y 3

**Resultado esperado:**
```
Run 1: Categories: 10 total (10 new), Customers: 150 total (150 new), ...
Run 2: Categories: 10 total (0 new), Customers: 150 total (0 new), ...
Run 3: Categories: 10 total (0 new), Customers: 150 total (0 new), ...
```

---

### Escenario 4: Actualización de Datos (Refresh Parcial)
**Situación:** Algunos datos fuente cambiaron, quieres refrescar sin limpiar todo.

**Pasos:**

1. Actualizar datos en MySQL fuente:
```sql
-- En sales_mysql
UPDATE Producto SET nombre = 'New Product Name' WHERE id = 5
UPDATE Cliente SET correo = 'new@example.com' WHERE id = 10
```

2. Ejecutar ETL (sin truncate):
```bash
python run_etl.py
```

**El ETL:**
- ✅ Detecta productos con mismo SKU y reutiliza ID (nombre no se actualiza en este ETL)
- ✅ Detecta clientes con mismo email y reutiliza ID
- ✅ Nota: El DWH es append-only para hechos, no hace updates

**Implicación:**
- Si necesitas actualizar atributos de dimensión, necesitarías lógica de SCD (Slowly Changing Dimensions)
- Las FactSales son inmutables (auditabilidad)

---

## Monitoreo y Debugging

### Revisar Logs
```bash
# Último log
cat logs/etl_mysql_*.log | tail -50

# O buscar errores
grep -i "error\|failed\|missing" logs/etl_mysql_*.log

# O usar menos para navegación
less logs/etl_mysql_20250101_143022.log
```

### Logs Importantes

**✅ Éxito:**
```
[INFO] Transformed 150 customers - Gender: 150, Dates: 150
[INFO] Transformed 200 products
[INFO] Transformed 500 orders - Dates: 500, CRC: 200
[INFO] Categories: 10 total (5 new)
[INFO] FactSales: 2500 inserted, 0 skipped
```

**⚠️ Advertencias (pero OK):**
```
[WARNING] No exchange rates found - will use default rate (515.0)
[WARNING] Could not insert customer 123 (email): ...
[INFO] FactSales: 2500 inserted, 5 skipped
[WARNING]   - 5 rows missing product references
```

**❌ Errores (Aborta ETL):**
```
[ERROR] Connection FAILED: ...
[ERROR] ETL FAILED: ...
```

---

## Validación Post-Carga

### Verificar Dimensiones
```sql
-- Cuántos registros se cargaron
SELECT 'DimCategory' as tabla, COUNT(*) as registros FROM DimCategory
UNION ALL
SELECT 'DimChannel', COUNT(*) FROM DimChannel
UNION ALL
SELECT 'DimCustomer', COUNT(*) FROM DimCustomer
UNION ALL
SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL
SELECT 'DimTime', COUNT(*) FROM DimTime
UNION ALL
SELECT 'DimOrder', COUNT(*) FROM DimOrder
UNION ALL
SELECT 'FactSales', COUNT(*) FROM FactSales;
```

### Verificar Integridad Referencial
```sql
-- Verificar que FactSales tiene todas sus FK
SELECT COUNT(*) FROM FactSales fs
WHERE NOT EXISTS (SELECT 1 FROM DimProduct WHERE id = fs.productId)
   OR NOT EXISTS (SELECT 1 FROM DimCustomer WHERE id = fs.customerId)
   OR NOT EXISTS (SELECT 1 FROM DimOrder WHERE id = fs.orderId);

-- Resultado debe ser 0 (no hay huérfanos)
```

### Verificar Conversiones (Rule 2)
```sql
-- Verificar que los montos están en USD
SELECT
  COUNT(*) as total_lineas,
  COUNT(CASE WHEN productUnitPriceUSD > 0 THEN 1 END) as con_precio,
  AVG(productUnitPriceUSD) as precio_promedio,
  MIN(productUnitPriceUSD) as precio_min,
  MAX(productUnitPriceUSD) as precio_max
FROM FactSales;
```

### Verificar Mapeos (Rule 1)
```sql
-- Verificar que los mapeos de producto están
SELECT COUNT(*) FROM staging_map_producto
WHERE source_system = 'MYSQL';

-- Verificar códigos únicos
SELECT source_code, COUNT(*) as ocurrencias
FROM staging_map_producto
WHERE source_system = 'MYSQL'
GROUP BY source_code
HAVING COUNT(*) > 1;  -- Si hay algo, hay duplicados
```

---

## Manejo de Errores Comunes

### Error 1: "Could not connect to MySQL"
```
[ERROR] Connection FAILED: No such file or directory
```

**Solución:**
1. Verifica que MySQL está corriendo:
```bash
docker-compose ps  # Si usas Docker
```

2. Verifica credenciales en `.env`:
```
MYSQL_HOST=mysql-transactional
MYSQL_PORT=3306
MYSQL_USER=user
MYSQL_PASSWORD=user123
```

3. Prueba conexión manual:
```bash
mysql -h localhost -u user -puser123 -D sales_mysql -e "SELECT COUNT(*) FROM Cliente;"
```

---

### Error 2: "Could not connect to DWH"
```
[ERROR] Connection FAILED: [ODBC Driver 17 for SQL Server]...
```

**Solución:**
1. Verifica que MSSQL está corriendo:
```bash
docker-compose ps -f docker-compose.yml  # En raíz del proyecto
```

2. Verifica credenciales MSSQL:
```
MSSQL_DW_SERVER=localhost
MSSQL_DW_PORT=1434
MSSQL_DW_USER=admin
MSSQL_DW_PASSWORD=admin123
```

3. Prueba conexión con `sqlcmd`:
```bash
sqlcmd -S localhost,1434 -U admin -P admin123 -d MSSQL_DW -Q "SELECT COUNT(*) FROM DimProduct"
```

---

### Error 3: "Missing product/customer references"
```
[WARNING]   - 50 rows missing product references
[WARNING]   - 10 rows missing customer references
```

**Causa:** OrdenDetalle hace referencia a productos que no existen en la fuente MySQL.

**Solución:**
1. Verificar integridad en MySQL:
```sql
-- Productos referenciados que no existen
SELECT DISTINCT od.producto_id
FROM OrdenDetalle od
WHERE NOT EXISTS (
  SELECT 1 FROM Producto p WHERE p.id = od.producto_id
);
```

2. Opciones:
   - Limpiar datos huérfanos en MySQL (recomendado)
   - Aceptar que se salten estos registros (ETL continúa)
   - Crear productos fantasma en DWH

---

### Error 4: "No exchange rates found"
```
[WARNING] No exchange rates found - will use default rate (515.0)
```

**Causa:** No hay tasas de cambio en DimExchangeRate del DWH.

**Solución:**
1. Verificar tasas disponibles:
```sql
SELECT COUNT(*) FROM DimExchangeRate
WHERE fromCurrency = 'CRC' AND toCurrency = 'USD';
```

2. Si están vacías, inserta tasas por defecto:
```sql
INSERT INTO DimExchangeRate (fromCurrency, toCurrency, exchangeRate, effectiveDate)
VALUES ('CRC', 'USD', 515.0, GETDATE());
```

3. Re-ejecutar ETL para usar tasas reales.

---

## Performance Tips

### Para Grandes Volúmenes (millones de registros)

1. **Aumentar BATCH_SIZE** en `config.py`:
```python
BATCH_SIZE = 5000  # Antes era 1000
```

2. **Usar índices en tablas staging**:
```sql
CREATE INDEX idx_staging_map_producto ON staging_map_producto(source_system, source_code);
```

3. **Deshabilitar constraints temporalmente** (si es append):
```sql
ALTER TABLE FactSales NOCHECK CONSTRAINT ALL;
-- ETL aquí
ALTER TABLE FactSales CHECK CONSTRAINT ALL;
```

4. **Usar bulk insert** en lugar de row-by-row (mejora futura).

---

## Troubleshooting Checklist

- [ ] ¿MySQL está corriendo y accesible?
- [ ] ¿MSSQL DW está corriendo?
- [ ] ¿.env tiene credenciales correctas?
- [ ] ¿Tablas source en MySQL existen?
- [ ] ¿Tablas destino en MSSQL existen?
- [ ] ¿Hay datos en MySQL source?
- [ ] ¿Hay permisos de INSERT en DWH?
- [ ] ¿El log file es escribible?
- [ ] ¿Hay suficiente espacio en disco para logs?

---

## FAQ

**P: ¿Puedo ejecutar ETL con datos parciales?**
A: Sí. El ETL es robusto y saltará registros huérfanos con logging.

**P: ¿Cuánto tiempo toma?**
A: Depende del volumen. ~10-30 seg para 1000 registros. Logs lo indican.

**P: ¿Puedo interrumpir mid-ejecución?**
A: Mejor no. Si lo haces, próxima ejecución retomará desde el principio (idempotente).

**P: ¿Se actualizan datos existentes?**
A: No, el DWH es append-only. Mismo SKU reutiliza ID pero no actualiza atributos.

**P: ¿Cómo auditar qué se cargó?**
A: Revisa `staging_map_producto` y `source_tracking` para trazabilidad.

**P: ¿Puedo ejecutar ETL desde múltiples máquinas?**
A: Sí, pero asegúrate que tienen el mismo origen (MySQL) y destino (MSSQL).
