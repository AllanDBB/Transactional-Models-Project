-- ============================================
-- Script para ejecutar manualmente el Job
-- Útil para testing antes de que se active el schedule
--
-- NOTA: Usamos msdb porque es donde están los jobs
-- ============================================

USE msdb;  -- Base de datos del SISTEMA (donde están los jobs)
GO

PRINT '';
PRINT '========================================';
PRINT 'EJECUTANDO JOB MANUALMENTE';
PRINT '========================================';
PRINT '';

DECLARE @job_name NVARCHAR(128) = 'ActualizarTipoCambioBCCR';
DECLARE @return_code INT;

-- Verificar que el job existe
IF EXISTS (SELECT 1 FROM sysjobs WHERE name = @job_name)
BEGIN
    PRINT 'Job encontrado: ' + @job_name;
    PRINT 'Iniciando ejecución...';
    PRINT '';
    
    -- Ejecutar el job
    EXEC @return_code = sp_start_job @job_name = @job_name;
    
    IF @return_code = 0
    BEGIN
        PRINT '✓ Job iniciado correctamente';
        PRINT '';
        PRINT 'Esperando 30 segundos para que se complete...';
        WAITFOR DELAY '00:00:30';
        
        -- Mostrar el estado actual del job
        PRINT '';
        PRINT 'ESTADO DE LA EJECUCIÓN:';
        SELECT 
            j.name AS [Nombre del Job],
            CASE WHEN ja.job_id IS NOT NULL THEN 'Ejecutando' ELSE 'Completado' END AS [Estado],
            h.run_date AS [Fecha ejecución],
            h.run_time AS [Hora ejecución],
            CASE h.run_status
                WHEN 0 THEN '✗ FAILED'
                WHEN 1 THEN '✓ SUCCESS'
                WHEN 2 THEN '⟳ RETRY'
                WHEN 3 THEN '✗ CANCELLED'
                WHEN 4 THEN '◆ RUNNING'
            END AS [Resultado],
            h.run_duration AS [Duración (ms)]
        FROM sysjobs j
        LEFT JOIN sysjobactivity ja ON j.job_id = ja.job_id AND ja.session_id = (
            SELECT MAX(session_id) FROM sysjobactivity WHERE job_id = j.job_id
        )
        LEFT JOIN sysjobhistory h ON j.job_id = h.job_id AND h.instance_id = (
            SELECT MAX(instance_id) FROM sysjobhistory WHERE job_id = j.job_id
        )
        WHERE j.name = @job_name;
        
        PRINT '';
        PRINT 'Para ver más detalles, ejecuta:';
        PRINT '  02-validate-job-bccr.sql';
    END
    ELSE
    BEGIN
        PRINT '✗ Error al iniciar el job. Código de error: ' + CAST(@return_code AS NVARCHAR(10));
    END
END
ELSE
BEGIN
    PRINT '✗ Job NO encontrado: ' + @job_name;
    PRINT '';
    PRINT 'Ejecuta primero:';
    PRINT '  01-setup-job-bccr.sql';
END

PRINT '';
PRINT '========================================';
GO
