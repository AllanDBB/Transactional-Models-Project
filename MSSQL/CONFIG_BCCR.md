# GUÍA: Ejecutar ETL MSSQL y Conectarse al DWH Compartido

## 📋 Resumen

Este documento te explica cómo:
1. **Ejecutar tu ETL local** (si eres MSSQL)
2. **Conectarte al DWH compartido** todos
3. **Cargar tus datos** en el DWH (todos)

---

## 🎯 Tu Rol Depende de tu Base de Datos

### Si eres (MSSQL)
```
Tu máquina:
  ├─ MSSQL local (puerto 1433) → SalesDB_MSSQL
  ├─ DWH local (puerto 1434) → MSSQL_DW
  └─ Tu ETL: extrae de MSSQL → carga en DWH

Comando: python run_etl.py
```

### Si eres **Otros (MySQL, MongoDB, Neo4j, Supabase)**
```
Tu máquina:
  └─ Tu BD local (cualquier puerto)

Tu ETL: extrae de tu BD → carga en DWH REMOTO de Santiago

Comando: python run_etl.py (igual, pero conecta remotamente)
```

---

## 🔧 Paso 1: Configurar Conexión

### Opción A: Si estás en la máquina de Santiago (LOCAL)

No hagas nada. Usa `config.py` directamente.

### Opción B: Si estás en otra máquina (REMOTO)

**1. Copia el archivo de configuración:**
```bash
copy MSSQL/etl/.env.example MSSQL/etl/.env
```

**2. Edita `.env`:**
```env
# Cambiar ESTAS líneas:
MSSQL_SERVER=192.168.100.50       # IP de Santiago (pídela)
MSSQL_PORT=1433

MSSQL_DW_SERVER=192.168.100.50    # IP de Santiago (misma)
MSSQL_DW_PORT=1434
```

**3. Guarda el archivo**

---

## ✅ Paso 2: Verificar Conexión

**Prueba 1: Conexión a DWH**
```bash
cd MSSQL/etl
python -c "from config import DatabaseConfig; import pyodbc; conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string()); print('✓ DWH OK'); conn.close()"
```

**Esperado:**
```
✓ DWH OK
```

**Si falla:**
- ❌ `Cannot open server`: IP incorrecta o puertos cerrados
- ❌ `Login failed`: Contraseña incorrecta
- ❌ `Timeout`: Firewall bloqueado

---

## 🚀 Paso 3: Ejecutar tu ETL

**Desde PowerShell:**
```bash
cd MSSQL/etl
python run_etl.py
```

**Salida esperada:**
```
================================================================================
INICIANDO PROCESO ETL: MSSQL → DWH (5 Reglas de Integración)
================================================================================

[FASE 1] EXTRAYENDO DATOS...
✓ Clientes extraídos: 5
✓ Productos extraídos: 5
✓ Órdenes extraídas: 5
✓ Detalles extraídos: 10

[FASE 2] TRANSFORMANDO DATOS (5 REGLAS)...
✓ Clientes transformados: 5
✓ Productos transformados: 5
✓ Órdenes transformadas: 5
✓ Detalles transformados: 10

[FASE 3] CARGANDO DATOS AL DWH...
✓ Dimensiones cargadas correctamente

================================================================================
✅ PROCESO ETL COMPLETADO EXITOSAMENTE
================================================================================
```

---

## 📊 Paso 4: Verificar Datos en DWH

**Desde SSMS (SQL Server Management Studio):**

Conecta a: `192.168.100.50:1434` (o `localhost:1434` si estás en la máquina de Santiago)

**Ver datos cargados:**
```sql
USE MSSQL_DW
GO

-- Ver cuántos registros hay en cada tabla
SELECT 'DimCustomer' as tabla, COUNT(*) as registros FROM DimCustomer
UNION ALL
SELECT 'DimProduct', COUNT(*) FROM DimProduct
UNION ALL
SELECT 'FactSales', COUNT(*) FROM FactSales
GO

-- Ver trazabilidad (cuál ETL cargó qué)
SELECT source_system, COUNT(*) as registros 
FROM staging_source_tracking 
GROUP BY source_system
GO
```

---

## 🔍 Solución de Problemas

### Error: "Cannot open server 192.168.100.50"

**Causas:**
- IP incorrecta
- Puerto 1434 cerrado
- Máquina de Santiago apagada

**Soluciones:**
```powershell
# Verificar que la máquina está activa
ping 192.168.100.50

# Verificar que el puerto está abierto
Test-NetConnection -ComputerName 192.168.100.50 -Port 1434
```

---

### Error: "Login failed for user 'admin'"

**Causa:** Contraseña incorrecta en `.env`

**Verificar:** Que coincida con contenedor DWH
```yaml
# DWH/docker-compose.yml
environment:
  - MSSQL_SA_PASSWORD=admin123
```

---

### Error: "Connection timeout after 15000ms"

**Causa:** Firewall bloqueando conexión

**En máquina de Santiago:** Ejecutar como Admin
```powershell
Get-NetFirewallRule -DisplayName "MSSQL*","DWH*" | Select-Object DisplayName, Enabled
```

**Debe mostrar:**
```
DisplayName Enabled
----------- -------
MSSQL 1433     True
DWH 1434       True
```

---

## 📝 Checklist Final

- [ ] Obtuviste IP de Santiago (ej: 192.168.100.50)
- [ ] Copiaste `.env.example` → `.env`
- [ ] Editaste IP en `.env`
- [ ] Instalaste `python-dotenv`: `pip install python-dotenv`
- [ ] Probaste conexión: `python -c "from config import..."`
- [ ] Ejecutaste: `python run_etl.py`
- [ ] Verificaste datos en DWH con query SQL

---

## 📞 Contacto

**Si algo falla:**

1. Verificar logs: `cat etl_process.log`
2. Pedir IP a Santiago: `ipconfig` → busca "192.168.X.X"
3. Pedir que verifique firewall: `Get-NetFirewallRule -DisplayName "MSSQL*","DWH*"`

---

## 🎯 Próximos Pasos

**Todos (después de cargar datos):**
```sql
-- Verificar integración multi-fuente
SELECT DISTINCT source_system FROM staging_source_tracking
```

**Esperado:**
```
source_system
--------------
MSSQL
MySQL
MongoDB
Neo4j
Supabase
```

