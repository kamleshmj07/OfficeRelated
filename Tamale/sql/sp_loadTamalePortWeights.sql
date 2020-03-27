DROP PROCEDURE [dbo].[sp_loadTamalePortWeights]
GO


CREATE PROCEDURE [dbo].[sp_loadTamalePortWeights]
	@PortWeightColumn sysname,
	@PortfolioName varchar(200),
	@asof_dt date
AS
BEGIN
SET NOCOUNT ON;
declare @SQL nvarchar(2048)
--Remove old values
set @SQL = 'UPDATE tamaleFrontSheet
            SET ' + @PortWeightColumn + ' = NULL'
EXEC sp_executeSQL @SQL  -- dynamic SQL

set @SQL = 'UPDATE tamaleFrontSheet 
SET tamaleFrontSheet.' + @PortWeightColumn + ' = weights.sumWeight
FROM (SELECT Tamale_ID, SUM(cast(Percent_Assets as decimal(18,10))) as sumWeight
      FROM uf_tamaleSecurityList(''' + @PortfolioName + ''',''' + CAST(@asof_dt AS VARCHAR) + ''','''')
	  WHERE Tamale_ID IS NOT NULL
	  GROUP BY Tamale_ID) weights
WHERE tamaleFrontSheet.shortName = weights.Tamale_ID'

declare @RowCount INT
EXEC sp_executeSQL @SQL  -- dynamic SQL
SELECT @RowCount = @@ROWCOUNT
PRINT CAST(@RowCount AS VARCHAR(4)) + ' portfolio weights imported for ' + @PortfolioName
END




GO


