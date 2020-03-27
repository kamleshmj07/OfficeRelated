/*
 * Tamale SEG 500 lists for building the Bloomberg data pull SOI
 */
USE [TamaleMarketData]
GO


CREATE TABLE [dbo].[TamaleSEG500Lists](
	[TamaleListName] [varchar](1000) NOT NULL,
	[TamaleShortName] [varchar](1000) NOT NULL,
	[last_user] [varchar](255),
	[last_ts] [datetime]
);
GO
