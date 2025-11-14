#!/usr/bin/env python3
"""Verificar que el Job SQL Agent fue creado correctamente"""

import pyodbc
from config import DatabaseConfig

conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
cursor = conn.cursor()

print("\n" + "=" * 80)
print("VERIFICACION: SQL AGENT JOB")
print("=" * 80)

# Ver detalles del Job
cursor.execute("""
    SELECT 
        j.name AS JobName,
        j.description,
        j.enabled,
        s.name AS ScheduleName,
        s.freq_type AS Frequency,
        s.active_start_time AS StartTime,
        js.step_name AS StepName,
        js.subsystem
    FROM msdb.dbo.sysjobs j
    LEFT JOIN msdb.dbo.sysjobschedules jsc ON j.job_id = jsc.job_id
    LEFT JOIN msdb.dbo.sysschedules s ON jsc.schedule_id = s.schedule_id
    LEFT JOIN msdb.dbo.sysjobsteps js ON j.job_id = js.job_id
    WHERE j.name = 'Actualizar_TipoCambio_BCCR'
""")

rows = cursor.fetchall()

if rows:
    print("\n[OK] Job encontrado:")
    for row in rows:
        print(f"  Job Name: {row[0]}")
        print(f"  Description: {row[1]}")
        print(f"  Enabled: {row[2]}")
        print(f"  Schedule: {row[3]}")
        print(f"  Frequency: {row[4]} (4=Diario)")
        print(f"  Start Time: {row[5]}")
        print(f"  Step Name: {row[6]}")
        print(f"  Subsystem: {row[7]}")
else:
    print("\n[ERROR] Job no encontrado")

# Ver próxima ejecución programada
cursor.execute("""
    SELECT TOP 1
        next_run_date,
        next_run_time
    FROM msdb.dbo.sysjobschedules jsc
    JOIN msdb.dbo.sysjobs j ON jsc.job_id = j.job_id
    WHERE j.name = 'Actualizar_TipoCambio_BCCR'
""")

next_run = cursor.fetchone()
if next_run:
    print(f"\n  Próxima ejecución: {next_run[0]} a las {next_run[1]}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
