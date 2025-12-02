
SET NOCOUNT ON;
DECLARE @json NVARCHAR(MAX);
DECLARE @cmd NVARCHAR(4000);
SET @json = (
    SELECT TOP (5)
      r.RuleID,
      r.ConsequentProductIds,
      r.ConsequentNames,
      r.Support,
      r.Confidence,
      r.Lift,
      r.FechaCalculo
    FROM dwh.ProductAssociationRules r
    WHERE (
      r.AntecedentProductIds = '5689'
      OR r.AntecedentProductIds LIKE '5689,%'
      OR r.AntecedentProductIds LIKE '%,5689,%'
      OR r.AntecedentProductIds LIKE '%,5689'
    )
    AND r.Activo = 1
    ORDER BY r.Lift DESC
   FOR JSON PATH);

-- Guardar en archivo usando bcp
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;

-- Escribir a archivo
DECLARE @sql NVARCHAR(MAX);
SET @sql = 'echo ' + @json + ' > /tmp/json_output_1764701754902.json';
EXEC xp_cmdshell @sql;

EXEC sp_configure 'xp_cmdshell', 0;
RECONFIGURE;
EXEC sp_configure 'show advanced options', 0;
RECONFIGURE;
