#
# $Id: $
# $Source: $
#
# Generates axys positions report.
#

Param ([string][alias("d")]$asofdate,[string][alias("r")]$RUNMODE_PARAM)

$LastExitCode = 0
 
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

new-item -path $ARCHIVE_FILEDIR -type directory -force
new-item -path $LOGDIR\$currdateiso -type directory -force

if(Test-Path $LogFile) { Remove-Item $LogFile -force }

LogWrite "[$(get-date)] START $($MyInvocation.MyCommand.Name)" $LogFile

Set-Location -Path $WORKDIR

LogWrite "Running $BINDIR\tamale_rec_secdetails.pl" $LogFile

C:\Perl\bin\perl.exe $BINDIR\tamale_rec_secdetails.pl --properties $BINDIR\tamale.properties_$RUNMODE --update | % {LogWrite $_ $LogFile }
if (($LastExitCode -ne 0) -or -not $?){
	LogWrite "FATAL ERROR: Tamale: Failure to run tamale security datareconciliation" $LogFile
	exit 1
}

copy $WORKDIR\SecurityDetailsDifference.csv $ARCHIVE_FILEDIR\SecurityDetailsDifference_$currdateiso.csv


#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"

exit 0


