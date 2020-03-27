DROP PROCEDURE [dbo].[sp_exportPortfolios]
GO


CREATE PROCEDURE [dbo].[sp_exportPortfolios]
	@PortfolioName varchar(200),
	@ReportDate date,
	@Side varchar(10) = ''
AS

BEGIN
SET NOCOUNT ON;

SELECT distinct Tamale_ID
FROM uf_tamaleSecurityList(@PortfolioName,@ReportDate,@Side)
WHERE Tamale_ID is not NULL
END

GO