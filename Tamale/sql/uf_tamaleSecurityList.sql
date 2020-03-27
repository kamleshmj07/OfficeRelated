use TamaleMarketData;
go

DROP FUNCTION [dbo].[uf_tamaleSecurityList]
GO

create function [dbo].[uf_tamaleSecurityList] (
	@PortfolioName varchar(200),
	@ReportDate date,
	@Side      varchar(10) = ''
) returns @retTable table (
    Tamale_ID varchar(80),
	Security_Symbol varchar(80),
	Axys_Cusip varchar(80),
	Axys_Security_Name varchar(200),
	Portcode varchar(200),
	Market_Value numeric(18,6),
	Percent_Assets numeric(18,6),
	AxysMajorSecType varchar(200),
	Side varchar(200),
	Report_Date varchar(10),
	axysType varchar(4),
	axysSymbol varchar(45),
	BB_ID varchar(40),
	ID_BB_GLOBAL varchar(40),
	client_security_id varchar(80),
	bb_name varchar(125),
	BB_cusip varchar(25),
	BB_Sedol varchar(25),
	gics_sector varchar(125),
	gics_industry varchar(125),
	security_typ varchar(80)
)
AS
BEGIN
declare @ShortSide nvarchar(5);
declare @LongSide nvarchar(5);

set @ShortSide = 'short';
set @LongSide = 'long';

if @Side = 'Short'
    set @LongSide = '';

if @Side = 'Long'
    set @ShortSide = '';
 
with
AXPA as (
	SELECT
		Security_Symbol
		,left(Security_Symbol, 2) as AxysMajorSecType
		,Cusip
		,Security_Name
		,Portcode
		,Market_Value
		,Percent_Assets
		,side
		,asof_dt
	FROM AX_PortfolioAppraisal_Massive
)
INSERT INTO @retTable
SELECT
	(CASE
		WHEN TSOverrides.ShortName IS NOT NULL
		THEN TSOverrides.ShortName
		WHEN exch_code = 'US'
		THEN Ticker
		ELSE Ticker + ' ' + exch_code
	END) Tamale_ID,
	AXPA.Security_Symbol,
	AXPA.Cusip,
	AXPA.Security_Name,
	AXPA.Portcode,
	AXPA.Market_Value,
	AXPA.Percent_Assets,
	AXPA.AxysMajorSecType,
	AXPA.side,
	AXPA.asof_dt as Report_Date,
	ABM.axysType,
	ABM.axysSymbol,
	ABM.BB_ID,
	BBDD.ID_BB_GLOBAL,
	BBDD.client_security_id,
	BBDD.bb_name,
	BBDD.id_cusip,
	BBDD.id_sedol1,
	BBDD.gics_sector,
	BBDD.gics_industry,
	BBDD.security_typ
FROM AXPA
	LEFT OUTER JOIN (
		SELECT DISTINCT axysType,axysSymbol,BB_ID 
		FROM AxysBBMap
	) ABM
		ON (ABM.axysType + ABM.axysSymbol = AXPA.Security_Symbol
			AND AXPA.AxysMajorSecType NOT IN ('ds', 'ca', 'fc', 'tb', 'mc', 'cm', 'cl', 'pt', 'pe','po','co') )
		OR (REPLACE(axysSymbol,'.%','') = RTRIM(SUBSTRING(AXPA.Security_Symbol, 5, 6)) 
			AND AXPA.AxysMajorSecType IN ('cl', 'pt','po','co','op'))
	LEFT OUTER JOIN (
		SELECT DISTINCT ID_BB_GLOBAL,client_security_id,bb_name,id_cusip,id_sedol1,gics_sector,gics_industry,security_typ,exch_code,ticker 
		FROM bbDailyData
	) BBDD
		ON ABM.BB_ID = BBDD.ID_BB_GLOBAL
	LEFT OUTER JOIN tamale_SecurityExclusions TSExclusions
		ON AXPA.Security_Symbol like '%' + TSExclusions.Security_Symbol + '%'
	LEFT OUTER JOIN tamale_SecurityOverrides TSOverrides
		ON AXPA.Security_Symbol like '%' + TSOverrides.Security_Symbol + '%'
WHERE AXPA.Portcode = @PortfolioName
	AND AXPA.asof_dt = @reportDate
	AND TSExclusions.Security_Symbol is NULL
	AND AXPA.AxysMajorSecType NOT IN ('ds', 'ca', 'fc', 'tb', 'mc', 'cm', 'pe')
	AND (security_typ is NULL or security_typ != 'ETP')
	AND ( CASE 
			 WHEN (AXPA.AxysMajorSecType in ('pt', 'po', 'op') and AXPA.side = 'L')-- long put option -> short delta exposure, so flip the side on the position
				THEN 'short'
			 WHEN (AXPA.AxysMajorSecType in ('cl', 'co') and AXPA.side = 'S' ) -- short call option -> long delta exposure, so flip the side on the position
				THEN 'long'
			 WHEN AXPA.side = 'L'
				THEN 'long'
			 WHEN AXPA.side = 'S'
				THEN 'short'
			 END ) in (@ShortSide,@LongSide)
order by
	(CASE
		WHEN TSOverrides.ShortName IS NOT NULL
		THEN TSOverrides.ShortName
		WHEN exch_code = 'US'
		THEN Ticker
		ELSE Ticker + ' ' + exch_code
	END);
RETURN
END

GO
