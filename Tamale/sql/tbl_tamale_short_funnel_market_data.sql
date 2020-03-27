/*
 * $Id: $
 * $Source: $
 */
create table tamale_short_funnel_market_data (
	short_name varchar(255) not null,
	long_name varchar(1000),
	source_system varchar(100),
	source_system_security_id varchar(100),
	bbg_figi varchar(12),

	rev_growth_next_qtr_vs_pri_yr_pct decimal(20,6),
	rev_growth_ntm_vs_pri_yr_pct decimal(20,6),
	op_margin_next_qtr_pct decimal(20,6),
	op_margin_ntm_vs_pri_yr_pct decimal(20,6),
	eps_next_qtr_pct decimal(20,6),
	eps_growth_ntm_vs_pri_yr_pct decimal(20,6),

	asof_date date,
	last_user varchar(100),
	last_ts datetime,

	constraint pk_tamale_short_funnel_md_sn primary key (short_name)
);
go


alter table tamale_short_funnel_market_data add
	ev_to_ntm_ebit decimal(20,6),
	pe_ntm decimal(20,6),
	pe_ntm_plus1 decimal(20,6),
	rev_growth_ttm_vs_pri_yr_pct decimal(20,6),
	eps_growth_ttm_vs_pri_yr_pct decimal(20,6),
	eps_growth_ntm_plus1_vs_pri_yr_pct decimal(20,6),
	short_interest decimal(20,6),
	days_to_cover decimal(20,6),
	dividend_yield decimal(20,6),
	insider_ownership_pct decimal(20,6),
	stock_perf_ttm decimal(20,6),
	stock_perf_ttm_minus_eps_growth_ttm decimal(20,6),
	sell_side_buy_rating decimal(20,6),
	op_margin_ntm_pct decimal(20,6),
	sales_4wk_pct_chg decimal(20,6),
	eps_4wk_pct_chg decimal(20,6),
	return_to_sell_side_px_target decimal(20,6)
;
go


alter table tamale_short_funnel_market_data add
	min_pe_last_5yrs decimal(20,6),
	max_pe_last_5yrs decimal(20,6),
	avg_pe_last_5yrs decimal(20,6)
;
go

alter table tamale_short_funnel_market_data add
	cur_mkt_cap decimal(38,6),
	cur_entp_val decimal(38,6),
	px_last decimal(20,8),
	crncy varchar(100)
;
go