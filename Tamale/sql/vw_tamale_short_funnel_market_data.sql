/*
 * $Id: $
 * $Source: $
 */

drop view dbo.vw_tamale_short_funnel_market_data;
go

create view vw_tamale_short_funnel_market_data as
select
	short_name,
	long_name,
	cast(rev_growth_next_qtr_vs_pri_yr_pct as decimal(20, 2)) as rev_growth_next_qtr_vs_pri_yr_pct,
	cast(rev_growth_ntm_vs_pri_yr_pct as decimal(20, 2)) as rev_growth_ntm_vs_pri_yr_pct,
	cast(op_margin_next_qtr_pct as decimal(20, 2)) as op_margin_next_qtr_pct,
	cast(op_margin_ntm_vs_pri_yr_pct as decimal(20, 2)) as op_margin_ntm_vs_pri_yr_pct,
	cast(eps_next_qtr_pct as decimal(20, 2)) as eps_next_qtr_pct,
	cast(eps_growth_ntm_vs_pri_yr_pct as decimal(20, 2)) as eps_growth_ntm_vs_pri_yr_pct,
	cast(ev_to_ntm_ebit as decimal(20, 2)) as ev_to_ntm_ebit,
	cast(pe_ntm as decimal(20, 2)) as pe_ntm,
	cast(pe_ntm_plus1 as decimal(20, 2)) as pe_ntm_plus1,
	cast(rev_growth_ttm_vs_pri_yr_pct as decimal(20, 2)) as rev_growth_ttm_vs_pri_yr_pct,
	cast(eps_growth_ttm_vs_pri_yr_pct as decimal(20, 2)) as eps_growth_ttm_vs_pri_yr_pct,
	cast(eps_growth_ntm_plus1_vs_pri_yr_pct as decimal(20, 2)) as eps_growth_ntm_plus1_vs_pri_yr_pct,
	cast(short_interest as decimal(20, 2)) as short_interest,
	cast(days_to_cover as decimal(20, 2)) as days_to_cover,
	cast(dividend_yield as decimal(20, 2)) as dividend_yield,
	cast(insider_ownership_pct as decimal(20, 2)) as insider_ownership_pct,
	cast(stock_perf_ttm as decimal(20, 2)) as stock_perf_ttm,
	cast(stock_perf_ttm_minus_eps_growth_ttm as decimal(20, 2)) as stock_perf_ttm_minus_eps_growth_ttm,
	cast(sell_side_buy_rating as decimal(20, 2)) as sell_side_buy_rating,
	cast(op_margin_ntm_pct as decimal(20, 2)) as op_margin_ntm_pct,
	cast(sales_4wk_pct_chg as decimal(20, 2)) as sales_4wk_pct_chg,
	cast(eps_4wk_pct_chg as decimal(20, 2)) as eps_4wk_pct_chg,
	cast(return_to_sell_side_px_target as decimal(20, 2)) as return_to_sell_side_px_target,
	cast(min_pe_last_5yrs as decimal(20,1)) as min_pe_last_5yrs,
	cast(max_pe_last_5yrs as decimal(20,1)) as max_pe_last_5yrs,
	cast(avg_pe_last_5yrs as decimal(20,1)) as avg_pe_last_5yrs,
	crncy,
	crncy + ' ' + convert(varchar, cast(px_last as money), 1) as share_price, 
	reverse(substring(reverse( -- convert to coma-style number and strip the decimal digits with reverses
		convert(varchar, cast(round(cur_entp_val, 0) as money), 1)
		), 4, 1000)) as cur_entp_val, 
	reverse(substring(reverse(convert(varchar, -- convert to coma-style number and strip the decimal digits with reverses
		cast(
			round(
				cast(cur_mkt_cap as decimal(38,2)) / 1000000.0, 0
			) as money
		), 1
	)), 4, 1000)) as cur_mkt_cap,
	asof_date
from tamale_short_funnel_market_data
;
go