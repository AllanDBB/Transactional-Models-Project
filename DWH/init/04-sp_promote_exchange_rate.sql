-- ============================================================================
-- 04-sp_promote_exchange_rate.sql
-- Promueve staging.tipo_cambio hacia dwh.DimExchangeRate (CRC -> USD)
-- ============================================================================

USE MSSQL_DW;
GO

IF OBJECT_ID('dbo.sp_promote_exchange_rate', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_promote_exchange_rate;
GO

CREATE PROCEDURE dbo.sp_promote_exchange_rate
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE dwh.DimExchangeRate AS target
        USING (
            SELECT fecha, de_moneda, a_moneda, tasa
            FROM staging.tipo_cambio
        ) AS src(fecha, de_moneda, a_moneda, tasa)
        ON target.[date] = src.fecha
           AND target.fromCurrency = src.de_moneda
           AND target.toCurrency = src.a_moneda
        WHEN MATCHED THEN
            UPDATE SET rate = src.tasa
        WHEN NOT MATCHED THEN
            INSERT (toCurrency, fromCurrency, [date], rate)
            VALUES (src.a_moneda, src.de_moneda, src.fecha, src.tasa);

        PRINT '[OK] DimExchangeRate actualizada desde staging.tipo_cambio';
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

GRANT EXECUTE ON dbo.sp_promote_exchange_rate TO public;
GO

-- Procedimiento maestro simple (extensible)
IF OBJECT_ID('dbo.sp_etl_run_all', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_etl_run_all;
GO

CREATE PROCEDURE dbo.sp_etl_run_all
AS
BEGIN
    SET NOCOUNT ON;
    EXEC dbo.sp_promote_exchange_rate;
END;
GO

GRANT EXECUTE ON dbo.sp_etl_run_all TO public;
GO

