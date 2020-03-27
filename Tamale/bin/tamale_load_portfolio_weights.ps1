Param ([string][alias("d")]$asof_dt,[string][alias("r")]$RUNMODE_PARAM)
 
if(! $RUNMODE) {
    if(! $RUNMODE_PARAM) {
        echo "Please define `$RUNMODE variable with -r flag (ex. -r QA)" 
        exit 1
    }
    else {
        $RUNMODE = $RUNMODE_PARAM
    }
}
else {
    #IF both are set use command line
    if($RUNMODE_PARAM) { $RUNMODE = $RUNMODE_PARAM }
}

$ScriptPath = Split-Path -parent $MyInvocation.MyCommand.Definition
Import-Module $ScriptPath\..\..\Common\powershell\SEGlog
Import-Module $ScriptPath\..\..\Common\powershell\SEGDatabase

. $ScriptPath\tamale_globals_$RUNMODE.ps1


$LogFile = "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).log"

new-item -path $LOGDIR\$currdateiso -type directory -force

LogWrite "[$(get-date)] START $($MyInvocation.MyCommand.Name)" $LogFile
#####################################################################################

#
# Load portfolio weights into Tamale table
#
if(! $asof_dt) {
    $asof_dt = $sqlcurrdate
}

$Portfolios = @{
	'CooperSq_Port_Weight' = 'coopsq';
	'GreatJones_Port_Weight' = '@gsgrjone';
	'SEGPartners_Port_Weight' = '@seghedge';
	'SMID_Port_Weight' = '@active';
	'Baxter_Port_Weight' = '@cooplong';
	'Firmwide_Port_Weight' = '@firmwide';
	'SJPGlobal_Port_Weight' = "sjpall";
	'SJPLarge_Port_Weight' = "sjplarge";
	'Vandam_Port_Weight' = 'vandamlp';
	'Blackwall_Port_Weight' = "@blackwal";
	'UCITS_Port_Weight' = "ucits";
	'Chimco_Port_Weight' = "@chimcog";
}

foreach($port_field in $Portfolios.keys) {
    LogWrite "Updating $port_field" $LogFile

    $SQLCommand = "exec dbo.sp_loadTamalePortWeights '" + $port_field + "','" + $Portfolios[$port_field] + "','$asof_dt'"

    $SQLExp = "`"SQLCMD.exe`" -S $MSSQL_HOSTNAME -U $MSSQL_USERNAME -P $MSSQL_PASSWORD -d $MSSQL_DATABASE -Q `"$SQLCommand`" -b"
    Invoke-Expression "& $SQLExp" | % {LogWrite $_ $LogFile }
    if ($LastExitCode -ne 0) {
        LogWrite "Error running $SQLExp" $LogWrite
        exit 1
    }
}
#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

exit 0
