# MS SQL Server - MSSQL ETL

## 👉 COMPAÑEROS: LEER PRIMERO

📄 **Archivo principal:** `COMPAÑEROS_LEER_ESTO.md`

Contiene paso-a-paso para:
- Conectarse al DWH compartido
- Ejecutar tu ETL
- Cargar datos en el DWH

---

##  

### Levantar contenedores

```bash
cd MSSQL
docker-compose up -d
```

### Instalar dependencias Python

```bash
cd MSSQL/etl
pip install -r requirements.txt
```

### Ejecutar ETL

```bash
python run_etl.py
```

---

## 📦 Servicios

- **MSSQL (transaccional)**: Puerto 1433 → SalesDB_MSSQL
- **DWH**: Puerto 1434 → MSSQL_DW (compartido)

**Credenciales:**
- MSSQL: `sa` / `BasesDatos2!`
- DWH: `admin` / `admin123`

---

## 🔧 Configuración Multi-Equipo

**Santiago:** IP `192.168.100.50`

**Compañeros:** Ver `COMPAÑEROS_LEER_ESTO.md`

---

## 📁 Estructura

```
MSSQL/
├── README.md                      (este archivo)
├── COMPAÑEROS_LEER_ESTO.md        (LEER ESTO - compañeros)
├── docker-compose.yml
├── Dockerfile
├── init/                          (scripts SQL iniciales)
├── etl/
│   ├── config.py                  (config + .env support)
│   ├── .env.example               (template para compañeros)
│   ├── requirements.txt
│   ├── run_etl.py                 (orquestación principal)
│   ├── bccr_integration.py        (REGLA 2: tipos de cambio)
│   ├── load_historical_bccr.py    (carga histórico 3 años)
│   ├── update_bccr_rates.py       (actualización diaria)
│   ├── extract/
│   ├── transform/                 (5 REGLAS implementadas)
│   └── load/
└── .env.example                   (template variables entorno)
```

---

## 🎯 5 Reglas de Integración Implementadas

1. **REGLA 1:** Homologación de productos (SKU ↔ codigo_alt ↔ codigo_mongo)
2. **REGLA 2:** Normalización de moneda (CRC → USD con BCCR)
3. **REGLA 3:** Estandarización de género (M/F → Masculino/Femenino)
4. **REGLA 4:** Conversión de fechas (VARCHAR → DATETIME)
5. **REGLA 5:** Transformación de totales (string → DECIMAL)

---

## 📝 Para Compañeros

**1. Copia el archivo:**
```bash
copy etl/.env.example etl/.env
```

**2. Edita con tu IP:**
```env
MSSQL_DW_SERVER=192.168.100.50
MSSQL_DW_PORT=1434
```

**3. Ejecuta:**
```bash
python etl/run_etl.py
```

**Más detalles:** Ver `COMPAÑEROS_LEER_ESTO.md`

---

## ⚙️ Configuración Firewall (Santiago)

Ya abierto en puerto 1433 y 1434. Verificar:

```powershell
Get-NetFirewallRule -DisplayName "MSSQL*","DWH*"
```

---

## 🔗 Integración DWH

Todos los ETLs cargan en: `MSSQL_DW` (compartido)

**Verificar multi-fuente:**
```sql
SELECT source_system, COUNT(*) as registros 
FROM staging_source_tracking 
GROUP BY source_system
```
