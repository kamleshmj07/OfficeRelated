Use TamaleMarketData
go

Truncate Table tamaleFrontSheet
go

INSERT INTO tamaleFrontSheet (
	segType, 		 
	shortName 	,	 
	longName  ,		 
	Exchange , 		 
	Market , 		 
	bloombergSymbol , 	 
	PX_LAST  ,
	VOLUME_AVG_3M	,
	VOLUME_AVG_20D,	
	VOLUME_AVG_6M,		
	HIGH_52WEEK  ,		
	LOW_52WEEK	,		
	HIGH_DT_52WEEK  ,	
	LOW_DT_52WEEK  , 	
	CURRENCY,		
	FX_RATE	,			
	PX_LAST_USD	,		
	SHORT_INT	,	   
	Tamale_Symbol,
	PX_YEST_CLOSE,
	CHG_PCT_5D,
	CHG_PCT_1M,
	CHG_PCT_YTD,
	CRNCY,
	CUR_MKT_CAP,
	CURR_ENTP_VAL,
	EQY_FUND_CRNCY,
	FREE_CASH_FLOW_YIELD,
	fcf_yield_with_cur_mkt_cap,
	PE_RATIO,
	BEST_PE_RATIO,
	EQY_SH_OUT_ACTUAL,
	ID_BB_ULTIMATE_PARENT_CO,
	ID_BB_ULTIMATE_PARENT_CO_NAME,
	EQY_SIC_CODE,
	EARNINGS_CONF_CALL_DT,
	EARNINGS_CONF_CALL_TIME,
	EARNINGS_CONF_CALL_PHONE_NUM,
	EARNINGS_CONF_CALL_PIN,
	DOM_EARNINGS_CONF_CALL_PHONE_NUM,
	DOM_EARNINGS_CONF_CALL_PIN,
	gics_sub_industry ,
	gics_sub_industry_name ,
	gics_sector_name  ,
	gics_industry_name  ,
	gics_sector ,
	gics_industry  ,
	gics_industry_group_name  ,
	gics_industry_group,
	si_percent_equity_float,
	eqy_float,
	chg_pct_1yr,
	eqy_beta,
	last_update_dt,
	last_update  
)
SELECT 
	segType =
		'CS',
	shortName =
		CASE exch_code
			WHEN 'US' THEN Ticker
			ELSE Ticker + ' ' + EXCH_CODE
		END,
	longName =
		bb_name,
	NULL,
	NULL,
	bb_code,
	px_last,
	volume_avg_3m ,
	volume_avg_20d,
	volume_avg_6m ,
	high_52week  ,
	low_52week ,
	high_dt_52week ,
	low_dt_52week ,
	CRNCY,
	0,
	0,
	short_int,
	Tamale_Symbol =
		CASE exch_code
			WHEN 'US' THEN Ticker
			ELSE Ticker + ' ' + EXCH_CODE
		END,
	PX_YEST_CLOSE,
	CHG_PCT_5D,
	CHG_PCT_1M,
	CHG_PCT_YTD,
	CRNCY,
	CUR_MKT_CAP,
	CURR_ENTP_VAL,
	EQY_FUND_CRNCY,
	FREE_CASH_FLOW_YIELD,
	fcf_yield_with_cur_mkt_cap,
	PE_RATIO,
	BEST_PE_RATIO,
	eqy_shr_outstanding ,
	id_bb_ultimate_parent_co,
	id_bb_ultimate_parent_co_name,
	eqy_sic_code,
	earnings_conf_call_dt,
	earnings_conf_call_time,
	earnings_conf_call_phone_num,
	earnings_conf_call_pin,
	dom_earnings_conf_call_phone_num,
	dom_earnings_conf_call_pin,
	gics_sub_industry ,
	gics_sub_industry_name ,
	gics_sector_name  ,
	gics_industry_name  ,
	gics_sector ,
	gics_industry  ,
	gics_industry_group_name  ,
	gics_industry_group,
	si_percent_equity_float,
	eqy_float,
	chg_pct_1yr,
	eqy_beta,
	last_update_dt,
	last_update  
FROM bbDailyData
go

Update tamaleFrontSheet
set FX_RATE = 1 
from tamaleFrontSheet
where CRNCY = 'USD'
go

UPDATE t1
SET PX_LAST_USD	= FX_RATE * PX_LAST
FROM tamaleFrontSheet t1
Where FX_RATE > 0
go

Update tamaleFrontSheet
set asOf = getDate()
from tamaleFrontSheet
go
