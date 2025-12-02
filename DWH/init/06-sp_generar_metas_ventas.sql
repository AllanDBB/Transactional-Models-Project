-- =============================================
-- Stored Procedure: sp_generar_metas_ventas
-- Descripción: Genera metas de ventas realistas con diferentes escenarios
-- Autor: Sistema DWH
-- Fecha: 2025-12-02
-- =============================================

USE MSSQL_DW;
GO

IF OBJECT_ID('dbo.sp_generar_metas_ventas', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_generar_metas_ventas;
GO

CREATE PROCEDURE dbo.sp_generar_metas_ventas
    @Escenario VARCHAR(20) = 'BALANCEADO',  -- CONSERVADOR, BALANCEADO, AGRESIVO
    @CrecimientoBase DECIMAL(5,2) = 5.0,    -- % crecimiento mensual base
    @LimpiarAntes BIT = 0                    -- 1 = limpiar metas existentes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Mensaje NVARCHAR(MAX);
    DECLARE @TotalMetas INT;
    
    BEGIN TRY
        -- Validar escenario
        IF @Escenario NOT IN ('CONSERVADOR', 'BALANCEADO', 'AGRESIVO')
        BEGIN
            RAISERROR('Escenario inválido. Use: CONSERVADOR, BALANCEADO o AGRESIVO', 16, 1);
            RETURN;
        END
        
        -- Limpiar metas anteriores si se solicita
        IF @LimpiarAntes = 1
        BEGIN
            DELETE FROM dwh.MetasVentas;
            PRINT '🗑️  Metas anteriores eliminadas';
        END
        
        -- Configurar multiplicadores según escenario
        DECLARE @MultBase DECIMAL(5,3);
        DECLARE @Variacion DECIMAL(5,3);
        
        SELECT 
            @MultBase = CASE @Escenario
                WHEN 'CONSERVADOR' THEN 1.00  -- Meta = ventas actuales
                WHEN 'BALANCEADO'  THEN 1.05  -- Meta = +5% sobre ventas
                WHEN 'AGRESIVO'    THEN 1.15  -- Meta = +15% sobre ventas
            END,
            @Variacion = CASE @Escenario
                WHEN 'CONSERVADOR' THEN 0.05  -- Variación ±5%
                WHEN 'BALANCEADO'  THEN 0.10  -- Variación ±10%
                WHEN 'AGRESIVO'    THEN 0.15  -- Variación ±15%
            END;
        
        PRINT '📊 Generando metas de ventas...';
        PRINT '   Escenario: ' + @Escenario;
        PRINT '   Multiplicador base: ' + CAST(@MultBase AS VARCHAR);
        PRINT '   Variación: ±' + CAST(@Variacion * 100 AS VARCHAR) + '%';
        PRINT '';
        
        -- Generar metas con crecimiento mensual y variación por producto/cliente
        ;WITH VentasHistoricas AS (
            SELECT
                fs.customerId,
                fs.productId,
                dt.[year] AS Anio,
                dt.[month] AS Mes,
                SUM(fs.lineTotalUSD) AS TotalVendido,
                COUNT(DISTINCT fs.orderId) AS NumOrdenes
            FROM dwh.FactSales fs
            INNER JOIN dwh.DimTime dt ON fs.timeId = dt.id
            GROUP BY
                fs.customerId,
                fs.productId,
                dt.[year],
                dt.[month]
        ),
        MetasCalculadas AS (
            SELECT
                customerId,
                productId,
                Anio,
                Mes,
                TotalVendido,
                -- Aplicar multiplicador base + crecimiento mensual + variación aleatoria
                CAST(
                    TotalVendido 
                    * @MultBase 
                    * (1 + (@CrecimientoBase / 100.0) * Mes)  -- Crecimiento progresivo
                    * (1 + (@Variacion * (ABS(CHECKSUM(NEWID())) % 200 - 100) / 100.0))  -- Variación aleatoria
                    AS DECIMAL(18,2)
                ) AS MetaCalculada,
                -- Calcular meta mínima (80% de la meta)
                CAST(
                    TotalVendido 
                    * @MultBase 
                    * (1 + (@CrecimientoBase / 100.0) * Mes)
                    * 0.80
                    AS DECIMAL(18,2)
                ) AS MetaMinima,
                -- Calcular meta stretch (120% de la meta)
                CAST(
                    TotalVendido 
                    * @MultBase 
                    * (1 + (@CrecimientoBase / 100.0) * Mes)
                    * 1.20
                    AS DECIMAL(18,2)
                ) AS MetaStretch
            FROM VentasHistoricas
        )
        INSERT INTO dwh.MetasVentas (customerId, productId, Anio, Mes, MetaUSD)
        SELECT 
            customerId,
            productId,
            Anio,
            Mes,
            MetaCalculada
        FROM MetasCalculadas;
        
        SELECT @TotalMetas = @@ROWCOUNT;
        
        -- Estadísticas de las metas generadas
        SELECT 
            @Mensaje = '✅ Metas generadas: ' + CAST(@TotalMetas AS VARCHAR) + CHAR(13) + CHAR(10) +
                      '   Total Meta USD: $' + FORMAT(SUM(MetaUSD), 'N2') + CHAR(13) + CHAR(10) +
                      '   Meta promedio: $' + FORMAT(AVG(MetaUSD), 'N2') + CHAR(13) + CHAR(10) +
                      '   Meta mínima: $' + FORMAT(MIN(MetaUSD), 'N2') + CHAR(13) + CHAR(10) +
                      '   Meta máxima: $' + FORMAT(MAX(MetaUSD), 'N2')
        FROM dwh.MetasVentas
        WHERE Anio >= YEAR(GETDATE()) - 1;  -- Solo últimos 2 años
        
        PRINT @Mensaje;
        
        -- Resumen por mes
        PRINT '';
        PRINT '📈 Resumen por mes:';
        SELECT 
            Anio,
            Mes,
            COUNT(*) AS CantidadMetas,
            FORMAT(SUM(MetaUSD), 'N2') AS TotalMetaUSD,
            FORMAT(AVG(MetaUSD), 'N2') AS PromedioMetaUSD
        FROM dwh.MetasVentas
        WHERE Anio >= YEAR(GETDATE()) - 1
        GROUP BY Anio, Mes
        ORDER BY Anio, Mes;
        
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        
        PRINT '❌ ERROR: ' + @ErrorMsg;
        RAISERROR(@ErrorMsg, @ErrorSeverity, @ErrorState);
    END CATCH
END
GO

-- Dar permisos de ejecución
GRANT EXECUTE ON dbo.sp_generar_metas_ventas TO PUBLIC;
GO

PRINT '✅ Stored Procedure sp_generar_metas_ventas creado exitosamente';
PRINT '';
PRINT '📋 Ejemplos de uso:';
PRINT '   -- Escenario balanceado (default):';
PRINT '   EXEC dbo.sp_generar_metas_ventas;';
PRINT '';
PRINT '   -- Escenario conservador:';
PRINT '   EXEC dbo.sp_generar_metas_ventas @Escenario = ''CONSERVADOR'';';
PRINT '';
PRINT '   -- Escenario agresivo con 10% crecimiento:';
PRINT '   EXEC dbo.sp_generar_metas_ventas @Escenario = ''AGRESIVO'', @CrecimientoBase = 10.0;';
PRINT '';
PRINT '   -- Limpiar y regenerar:';
PRINT '   EXEC dbo.sp_generar_metas_ventas @LimpiarAntes = 1;';
GO
