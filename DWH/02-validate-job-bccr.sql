-- ============================================
-- Script de validación del Job BCCR
-- Verificar que todo esté configurado correctamente
--
-- NOTA: Primero validamos en msdb (sistema)
--       Luego en MSSQL_DW (tus datos)
-- ============================================

USE msdb;  -- Base de datos del SISTEMA
GO

PRINT '========================================';
PRINT 'VALIDACIÓN DEL JOB ACTUALIZARTIPOCAMBIO';
PRINT 'BD SISTEMA (msdb)';
PRINT '========================================';
PRINT '';

-- 1. Verificar que el job existe
PRINT '1. ESTADO DEL JOB:';
IF EXISTS (SELECT 1 FROM sysjobs WHERE name = 'ActualizarTipoCambioBCCR')
BEGIN
    SELECT 
        name AS [Nombre del Job],
        enabled AS [Habilitado],
        description AS [Descripción]
    FROM sysjobs 
    WHERE name = 'ActualizarTipoCambioBCCR';
    PRINT '   ✓ Job encontrado';
END
ELSE
BEGIN
    PRINT '   ✗ Job NO encontrado - Ejecuta 01-setup-job-bccr.sql';
END
PRINT '';

-- 2. Verificar el schedule
PRINT '2. HORARIO DE EJECUCIÓN:';
SELECT 
    s.name AS [Schedule],
    CASE s.freq_type 
        WHEN 1 THEN 'Una sola vez'
        WHEN 4 THEN 'Diariamente'
        WHEN 8 THEN 'Semanalmente'
        WHEN 16 THEN 'Mensualmente'
        ELSE 'Otro'
    END AS [Frecuencia],
    CONVERT(CHAR(8), s.active_start_time, 121) AS [Hora de inicio]
FROM sysschedules s
INNER JOIN sysjobschedules js ON s.schedule_id = js.schedule_id
INNER JOIN sysjobs j ON js.job_id = j.job_id
WHERE j.name = 'ActualizarTipoCambioBCCR';
PRINT '   Nota: La hora aparece en formato HHmmss (050000 = 5:00 AM)';
PRINT '';

-- 3. Verificar el paso del job
PRINT '3. PASO DEL JOB:';
SELECT 
    step_name AS [Nombre del Paso],
    subsystem AS [Tipo],
    command AS [Comando]
FROM sysjobsteps
WHERE job_id = (SELECT job_id FROM sysjobs WHERE name = 'ActualizarTipoCambioBCCR');
PRINT '';

-- 4. Historial reciente de ejecuciones (últimas 10)
PRINT '4. HISTORIAL DE ÚLTIMAS EJECUCIONES:';
SELECT TOP 10
    CAST(h.run_date AS CHAR(8)) + ' ' + SUBSTRING(CAST(h.run_time AS NVARCHAR(10)), 1, 2) + ':' + 
    SUBSTRING(CAST(h.run_time AS NVARCHAR(10)), 3, 2) + ':' + 
    SUBSTRING(CAST(h.run_time AS NVARCHAR(10)), 5, 2) AS [Fecha/Hora],
    CASE h.run_status
        WHEN 0 THEN '✗ Failed'
        WHEN 1 THEN '✓ Success'
        WHEN 2 THEN '⟳ Retry'
        WHEN 3 THEN '✗ Cancelled'
        WHEN 4 THEN '◆ Running'
    END AS [Estado],
    h.run_duration AS [Duración (mseg)]
FROM sysjobhistory h
WHERE h.job_id = (SELECT job_id FROM sysjobs WHERE name = 'ActualizarTipoCambioBCCR')
ORDER BY h.run_date DESC, h.run_time DESC;
PRINT '';

-- 5. Próxima ejecución programada
PRINT '5. PRÓXIMA EJECUCIÓN:';
SELECT 
    CASE 
        WHEN CONVERT(INT, FORMAT(GETDATE(), 'HHmmss')) >= 50000
        THEN CONVERT(NVARCHAR(10), DATEADD(DAY, 1, GETDATE()), 121)
        ELSE CONVERT(NVARCHAR(10), GETDATE(), 121)
    END AS [Próxima fecha],
    '05:00 AM' AS [Hora programada];
PRINT '';

-- 6. Verificar tabla DimExchangeRate
USE MSSQL_DW;  -- Tu Data Warehouse
GO

PRINT '';
PRINT '========================================';
PRINT 'VALIDACIÓN DE DATOS EN TU DWH';
PRINT 'BD: MSSQL_DW';
PRINT '========================================';
PRINT '';
SELECT 
    COUNT(*) AS [Total registros],
    MIN(date) AS [Fecha más antigua],
    MAX(date) AS [Fecha más reciente],
    COUNT(DISTINCT date) AS [Fechas únicas]
FROM DimExchangeRate;
PRINT '';

-- 7. Últimos 5 registros
PRINT '7. ÚLTIMOS 5 REGISTROS:';
SELECT TOP 5
    date AS [Fecha],
    fromCurrency AS [De],
    toCurrency AS [A],
    rate AS [Tasa]
FROM DimExchangeRate
ORDER BY date DESC;

PRINT '';
PRINT '========================================';
PRINT 'VALIDACIÓN COMPLETADA';
PRINT '========================================';
GO
