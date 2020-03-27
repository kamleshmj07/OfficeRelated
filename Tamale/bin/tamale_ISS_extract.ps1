#
# $Id: $
# $Source: $
#
# Generates axys positions report.
#

Param ([string][alias("d")]$asofdate,[string][alias("r")]$RUNMODE_PARAM)
 
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

. $ScriptPath/tamale_globals_$RUNMODE.ps1

$LogFile = "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).log"

new-item -path $ARCHIVE_FILEDIR -type directory -force
new-item -path $LOGDIR\$currdateiso -type directory -force
new-item -path $WORKDIR -type directory -force

if(Test-Path $LogFile) { Remove-Item $LogFile -force }

LogWrite "[$(get-date)] START $($MyInvocation.MyCommand.Name)" $LogFile

if(! $asofdate) {
    $asofdate = $sqlcurrdate
}

LogWrite "Running $BINDIR\tamale_ISS_extract.pl" $LogFile

C:\Perl\bin\perl.exe $BINDIR\tamale_ISS_extract.pl --properties $BINDIR\tamale.properties_$RUNMODE --asofdate $asofdate 2>&1 | % {LogWrite $_ $LogFile }

if ($LastExitCode -ne 0) {
    Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"
	exit 1
}

#Clean up and archive old files
Copy-Item $WORKDIR\*.pdf $ARCHIVE_FILEDIR\
if(Test-Path $WORKDIR\*.pdf) { Remove-Item $WORKDIR\*.pdf -force }

Copy-Item $WORKDIR\holdings$currdateiso.txt $ARCHIVE_FILEDIR\
Remove-Item $WORKDIR\holdings$currdateiso.txt

Copy-Item $WORKDIR\remote_proxy_file.txt $ARCHIVE_FILEDIR\remote_proxy_file_$asofdate.txt 
Remove-Item $WORKDIR\remote_proxy_file.txt


#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"

exit 0


