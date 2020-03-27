/*
 * $Id: $
 * $Source: $
 */
CREATE TABLE dbo.tamaleFrontSheet (
	[segType] [varchar](40) NULL,
	[shortName] [varchar](80) NULL,
	[longName] [varchar](125) NULL,
	[Exchange] [varchar](8) NULL,
	[Market] [varchar](8) NULL,
	[bloombergSymbol] [varchar](80) NULL,
	[PX_LAST] [numeric](19, 6) NULL,
	[VOLUME_AVG_3M] [numeric](19, 0) NULL,
	[VOLUME_AVG_20D] [numeric](19, 0) NULL,
	[VOLUME_AVG_6M] [numeric](19, 0) NULL,
	[HIGH_52WEEK] [numeric](19, 6) NULL,
	[LOW_52WEEK] [numeric](19, 6) NULL,
	[HIGH_DT_52WEEK] [datetime] NULL,
	[LOW_DT_52WEEK] [datetime] NULL,
	[EQY_SH_OUT_ACTUAL] [numeric](19, 0) NULL,
	[CURRENCY] [varchar](20) NULL,
	[FX_RATE] [numeric](19, 6) NULL,
	[PX_LAST_USD] [numeric](19, 6) NULL,
	[SHORT_INT] [numeric](19, 0) NULL,
	[Tamale_Symbol] [varchar](80) NULL,
	[asOf] [datetime] NULL,
	[PX_YEST_CLOSE] [numeric](19, 6) NULL,
	[CHG_PCT_5D] [numeric](19, 6) NULL,
	[CHG_PCT_1M] [numeric](19, 6) NULL,
	[CHG_PCT_YTD] [numeric](19, 6) NULL,
	[CRNCY] [varchar](20) NULL,
	[CUR_MKT_CAP] [numeric](18, 0) NULL,
	[CURR_ENTP_VAL] [numeric](19, 6) NULL,
	[EQY_FUND_CRNCY] [varchar](4) NULL,
	[FREE_CASH_FLOW_YIELD] [numeric](19, 6) NULL,
	[PE_RATIO] [numeric](19, 6) NULL,
	[BEST_PE_RATIO] [numeric](19, 6) NULL,
	[id_bb_ultimate_parent_co] [varchar](80) NULL,
	[id_bb_ultimate_parent_co_name] [varchar](80) NULL,
	[eqy_sic_code] [varchar](80) NULL,
	[eqy_sic_code_name] [varchar](80) NULL,
	[earnings_conf_call_dt] [datetime] NULL,
	[earnings_conf_call_time] [datetime] NULL,
	[earnings_conf_call_phone_num] [varchar](80) NULL,
	[earnings_conf_call_pin] [varchar](80) NULL,
	[dom_earnings_conf_call_phone_num] [varchar](80) NULL,
	[dom_earnings_conf_call_pin] [varchar](80) NULL,
	[gics_sub_industry] [varchar](125) NULL,
	[gics_sub_industry_name] [varchar](125) NULL,
	[gics_sector_name] [varchar](125) NULL,
	[gics_industry_name] [varchar](125) NULL,
	[gics_sector] [varchar](125) NULL,
	[gics_industry] [varchar](125) NULL,
	[gics_industry_group_name] [varchar](125) NULL,
	[gics_industry_group] [varchar](125) NULL,
	[last_update_dt] [datetime] NULL,
	[last_update] [varchar](20) NULL,
	[CooperSq_Port_Weight] [numeric](19, 8) NULL,
	[GreatJones_Port_Weight] [numeric](19, 8) NULL,
	[Lafayette_Port_Weight] [numeric](19, 8) NULL,
	[SMID_Port_Weight] [numeric](19, 8) NULL,
	[SEGPartners_Port_Weight] [numeric](19, 8) NULL,
	[Baxter_Port_Weight] [numeric](19, 8) NULL,
	[Firmwide_Port_Weight] [numeric](19, 8) NULL,
	[fcf_yield_with_cur_mkt_cap] [numeric](18, 6) NULL,
	[SJPGlobal_Port_Weight] [numeric](19, 8) NULL,
	[SJPLarge_Port_Weight] [numeric](19, 8) NULL,
	[Vandam_Port_Weight] [numeric](19, 8) NULL,
	[si_percent_equity_float] [numeric](18, 6) NULL,
	[eqy_float] [numeric](18, 10) NULL,
	[chg_pct_1yr] [numeric](18, 6) NULL,
	[Blackwall_Port_Weight] [numeric](19, 8) NULL
);
GO

alter table dbo.tamaleFrontSheet add
	Chimco_Port_Weight numeric(19, 8),
	UCALSMID_Port_Weight numeric(19, 8)
;
go

alter table [tamaleFrontSheet] add
	EQY_BETA numeric(18,6)
;
go

alter table dbo.tamaleFrontSheet add
	UCITS_Port_Weight numeric(19, 8)
;
go
