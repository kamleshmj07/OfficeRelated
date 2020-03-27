Param ([string][alias("r")]$RUNMODE_PARAM)

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

& "SQLCMD.exe" -S $MSSQL_HOSTNAME -U $MSSQL_USERNAME -P $MSSQL_PASSWORD -i $SQLDIR\sp_loadFrontSheetSchemaFromBloomberg.sql -b | % {LogWrite $_ $LogFile }

if ($LastExitCode -ne 0) {
    LogWrite "FATAL ERROR: [$RUNMODE] Tamale: Failure to execute SQL for tamaleFrontSheet load" $LogFile
	exit 1
}

#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

exit 0

