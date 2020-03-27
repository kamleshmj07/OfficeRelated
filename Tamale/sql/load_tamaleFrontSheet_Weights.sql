SET IMPLICIT_TRANSACTIONS ON

Use TamaleMarketData
go

print 'Removing old portfolio weights';

UPDATE tamaleFrontSheet 
SET CooperSq_Port_Weight = NULL,
GreatJones_Port_Weight = NULL,
Lafayette_Port_Weight = NULL,
SMID_Port_Weight = NULL,
SEGPartners_Port_Weight = NULL;

print 'Loading Tamale portfolio weights for $(asofdate)';

exec dbo.sp_loadTamalePortWeights 'CooperSq_Port_Weight','coopsq','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

exec dbo.sp_loadTamalePortWeights 'GreatJones_Port_Weight','@gsgrjone','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

exec dbo.sp_loadTamalePortWeights 'Lafayette_Port_Weight','@lafhedge','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

exec dbo.sp_loadTamalePortWeights 'SEGPartners_Port_Weight','@seghedge','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

exec dbo.sp_loadTamalePortWeights 'SMID_Port_Weight','@active','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

exec dbo.sp_loadTamalePortWeights 'Baxter_Port_Weight','baxter','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

exec dbo.sp_loadTamalePortWeights 'Firmwide_Port_Weight','@firmwide','$(asofdate)' 
IF @@ERROR != 0 
BEGIN
    PRINT 'Rolling back changes'
    RETURN
END

COMMIT TRANSACTION

go
