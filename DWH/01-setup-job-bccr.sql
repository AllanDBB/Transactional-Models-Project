-- ============================================
-- SQL Server Agent Job: Actualizar Tipos de Cambio BCCR
-- Descripción: Job que ejecuta diariamente el script Python para actualizar 
--              la tasa de cambio CRC->USD desde el BCCR
-- Programado: Diariamente a las 5:00 AM
-- 
-- NOTA: msdb es la BD del SISTEMA donde se guardan los jobs
--       MSSQL_DW es tu DWH donde están los datos
-- ============================================

USE msdb;  -- Base de datos del SISTEMA para jobs y schedules
GO

-- 1. ELIMINAR JOB SI EXISTE (para re-crear limpio)
IF EXISTS (SELECT 1 FROM sysjobs WHERE name = 'ActualizarTipoCambioBCCR')
BEGIN
    EXEC sp_delete_job @job_name = 'ActualizarTipoCambioBCCR', @delete_unused_schedule = 1;
    PRINT 'Job anterior eliminado';
END
GO

-- 2. CREAR EL JOB
EXEC sp_add_job 
    @job_name = 'ActualizarTipoCambioBCCR',
    @enabled = 1,
    @description = 'Actualiza diariamente la tasa de cambio CRC->USD desde BCCR',
    @owner_login_name = 'sa';

PRINT 'Job creado: ActualizarTipoCambioBCCR';
GO

-- 3. CREAR EL PASO DEL JOB (ejecutar el script Python)
EXEC sp_add_jobstep
    @job_name = 'ActualizarTipoCambioBCCR',
    @step_name = 'Ejecutar actualización BCCR',
    @subsystem = 'CmdExec',
    @command = '"C:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\.venv\Scripts\python.exe" "C:\Users\Santiago Valverde\Downloads\University\BD2\Transactional-Models-Project\DWH\bccr\bccr_exchange_rate.py" update-current',
    @retry_attempts = 2,
    @retry_interval = 1,
    @on_success_action = 1,  -- Ir al siguiente paso
    @on_fail_action = 2;     -- Fallar el job

PRINT 'Paso del job creado';
GO

-- 4. CREAR EL SCHEDULE (Diariamente a las 5:00 AM)
EXEC sp_add_schedule
    @schedule_name = 'Diario_5AM_BCCR',
    @freq_type = 4,           -- Diario
    @freq_interval = 1,       -- Cada día
    @active_start_time = 050000,  -- 05:00 (5:00 AM)
    @active_end_time = 235959;

PRINT 'Schedule creado: Diario a las 5:00 AM';
GO

-- 5. ASOCIAR EL SCHEDULE AL JOB
EXEC sp_attach_schedule
    @job_name = 'ActualizarTipoCambioBCCR',
    @schedule_name = 'Diario_5AM_BCCR';

PRINT 'Schedule asociado al job';
GO

-- 6. ASIGNAR EL JOB AL SERVIDOR LOCAL
EXEC sp_add_jobserver
    @job_name = 'ActualizarTipoCambioBCCR',
    @server_name = N'(local)';

PRINT 'Job asignado al servidor local';
GO

-- ============================================
-- VERIFICACIÓN: Mostrar el job creado
-- ============================================
SELECT 
    j.job_id,
    j.name AS [Nombre del Job],
    j.enabled AS [Habilitado],
    j.description AS [Descripción],
    s.schedule_uid,
    s.name AS [Schedule],
    s.freq_type AS [Frecuencia]
FROM sysjobs j
LEFT JOIN sysjobschedules js ON j.job_id = js.job_id
LEFT JOIN sysschedules s ON js.schedule_id = s.schedule_id
WHERE j.name = 'ActualizarTipoCambioBCCR';

PRINT '';
PRINT '✓ Job configurado exitosamente';
PRINT '✓ Se ejecutará diariamente a las 5:00 AM';
PRINT '✓ Comando: python bccr_exchange_rate.py update-current';
GO
