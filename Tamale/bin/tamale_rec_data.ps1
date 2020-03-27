#
# $Id: $
# $Source: $
#
# Generates axys positions report.

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

if(Test-Path $LogFile) { Remove-Item $LogFile -force }

LogWrite "[$(get-date)] START $($MyInvocation.MyCommand.Name)" $LogFile

if(! $asofdate) {
    $asofdate = $sqlcurrdate
}

             
LogWrite "Running $BINDIR\tamale_rec_data.pl" $LogFile

C:\Perl\bin\perl.exe $BINDIR\tamale_rec_data.pl --properties $BINDIR\tamale.properties_$RUNMODE --asofdate $asofdate | % {LogWrite $_ $LogFile }

if ($LastExitCode -ne 0) {
	$subj = "FATAL ERROR: [$RUNMODE] Tamale: Failure to run tamale data reconciliation"
	$body = (gc $LogFile | out-string)
	$attachments = @($LogFile)

	Send-MailMessage -SmtpServer $SMTPSERVER -To $TO -Cc $CC -From $FROM -Subject $subj -Body $body -Attachments $attachments

    Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"
	exit 1
}

copy $WORKDIR\tamale_recon.xls $ARCHIVE_FILEDIR\tamale_recon_$currdateiso.xls


#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"

exit 0


