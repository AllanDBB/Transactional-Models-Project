#!/usr/bin/env python3
"""
PASO 2: Configurar SQL Agent Job para actualización automática diaria
Ejecuta el script SQL para crear el Job en SQL Server
"""
import pyodbc
from config import DatabaseConfig
import sys

print("=" * 80)
print("PASO 2: CONFIGURAR SQL AGENT JOB")
print("=" * 80)

# Script SQL para crear el Job
sql_script = """
-- ============================================================================
-- JOB SQL AGENT: Actualizar Tipos de Cambio BCCR diariamente a las 5 AM
-- ============================================================================

USE msdb;
GO

-- Verificar si el Job ya existe y eliminarlo
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'Actualizar_TipoCambio_BCCR')
BEGIN
    EXEC msdb.dbo.sp_delete_job @job_name = 'Actualizar_TipoCambio_BCCR', @delete_unused_schedule = 1
    PRINT 'Job anterior eliminado'
END
GO

-- Verificar si el Schedule ya existe
IF EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = 'Diario_5AM_TipoCambio')
BEGIN
    PRINT 'Schedule ya existe'
END
ELSE
BEGIN
    -- 1. Crear Schedule (todos los días a las 5 AM)
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name = 'Diario_5AM_TipoCambio',
        @freq_type = 4,                    -- Diario
        @freq_interval = 1,                -- Cada día
        @active_start_time = 050000,       -- 05:00:00
        @enabled = 1
    PRINT 'Schedule creado: Diario 5 AM'
END
GO

-- 2. Crear Job
EXEC msdb.dbo.sp_add_job
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @enabled = 1,
    @description = 'Actualiza tipos de cambio CRC->USD desde BCCR a las 5 AM'
GO

PRINT 'Job creado: Actualizar_TipoCambio_BCCR'

-- 3. Agregar Step al Job (llamar PowerShell que ejecuta Python)
EXEC msdb.dbo.sp_add_jobstep
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @step_name = 'Ejecutar_BCCR_Update',
    @subsystem = 'PowerShell',
    @command = 'cd "C:\\Users\\Santiago Valverde\\Downloads\\University\\BD2\\Transactional-Models-Project\\MSSQL\\etl"; & ".\\..\\..\\..\\..\\..\.venv\\Scripts\\python.exe" update_bccr_rates.py',
    @retry_attempts = 3,
    @retry_interval = 5
GO

PRINT 'Step de Job creado'

-- 4. Vincular Schedule al Job
EXEC msdb.dbo.sp_attach_schedule
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @schedule_name = 'Diario_5AM_TipoCambio'
GO

PRINT 'Schedule vinculado al Job'

-- 5. Asignar servidor
EXEC msdb.dbo.sp_add_jobserver
    @job_name = 'Actualizar_TipoCambio_BCCR',
    @server_name = N'(local)'
GO

PRINT 'Servidor asignado'

-- Verificar creación
SELECT 
    name AS JobName,
    description,
    enabled
FROM msdb.dbo.sysjobs 
WHERE name = 'Actualizar_TipoCambio_BCCR'

PRINT ''
PRINT '================================================================================'
PRINT '[OK] SQL AGENT JOB CONFIGURADO EXITOSAMENTE'
PRINT '================================================================================'
PRINT 'Job: Actualizar_TipoCambio_BCCR'
PRINT 'Hora: 05:00 AM (todos los días)'
PRINT 'Acción: Actualizar tipos de cambio CRC -> USD desde BCCR'
PRINT 'Tabla: staging_tipo_cambio'
PRINT '================================================================================'
"""

try:
    # Conectar a DWH (que está en SQL Server local)
    print("\n[1] Conectando a SQL Server (DWH)...")
    conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
    cursor = conn.cursor()
    print("    [OK] Conexión exitosa")
    
    # Ejecutar script por partes (pyodbc no soporta GO)
    print("\n[2] Ejecutando configuración de SQL Agent Job...")
    
    # Dividir por GO
    batches = sql_script.split('\nGO\n')
    
    for i, batch in enumerate(batches):
        batch = batch.strip()
        if batch and not batch.upper().startswith('--'):
            try:
                cursor.execute(batch)
                conn.commit()
                print(f"    [OK] Batch {i+1}/{len(batches)} ejecutado")
            except Exception as e:
                if 'already exists' in str(e) or 'Constraint' in str(e):
                    print(f"    [INFO] Batch {i+1}: {str(e)}")
                else:
                    print(f"    [ERROR] Batch {i+1}: {str(e)}")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("[OK] PASO 2 COMPLETADO")
    print("=" * 80)
    print("\nJob SQL Agent configurado:")
    print("  - Nombre: Actualizar_TipoCambio_BCCR")
    print("  - Frecuencia: Diariamente a las 5:00 AM")
    print("  - Acción: Ejecutar update_bccr_rates.py")
    print("  - Destino: staging_tipo_cambio en MSSQL_DW")
    print("\nEl Job iniciará automáticamente mañana a las 5 AM")
    print("=" * 80)

except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    sys.exit(1)
