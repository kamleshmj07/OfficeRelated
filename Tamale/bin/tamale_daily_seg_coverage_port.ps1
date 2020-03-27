#
# $Id: $
# $Source: $
#
# Driver to generate the Tamale SEG Coverage portfolio file for uploading
# into Tamale.

#####################################################################################
#Init Code
#####################################################################################
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
. $ScriptPath\tamale_globals_$RUNMODE.ps1

$LogFile = "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).log"

new-item -path $ARCHIVE_FILEDIR -type directory -force
new-item -path $LOGDIR\$currdateiso -type directory -force

if(Test-Path $LogFile) { Remove-Item $LogFile -force }

LogWrite "[$(get-date)] START $($MyInvocation.MyCommand.Name)" $LogFile
#####################################################################################


Set-Location -Path $WORKDIR

rm -verbose -erroraction SilentlyContinue $WORKDIR\segCoverage.csv

& C:\Perl\bin\perl.exe $BINDIR\tamale_seg_coverage_portfolio.pl --outfile $WORKDIR\segCoverage.csv 2>&1 | % {LogWrite $_ $LogFile} 

if ($LastExitCode -ne 0) {
	$subj = "FATAL ERROR: [$RUNMODE] Tamale: Portfolio Coverage Load"
	$body = (gc $LogFile | out-string)
	$attachments = @($LogFile)

	Send-MailMessage -SmtpServer $SMTPSERVER -To $TO -Cc $CC -From $FROM -Subject $subj -Body $body -Attachments $attachments

    Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"
	exit 1
}

copy $WORKDIR\segCoverage.csv $ARCHIVE_FILEDIR\segCoverage_$currdateiso.csv

$webclient = New-Object System.Net.WebClient
$webclient.Credentials = New-Object System.Net.NetworkCredential($TAMALE_FTP_USERNAME ,$TAMALE_FTP_PASSWORD ) 

$ftp = "ftp://$TAMALE_FTP_HOSTNAME/public/segCoverage.csv"

#This is try catch because there is not LsatExitCode for the webclient process
try {
   $webclient.UploadFile($ftp, "$WORKDIR\segCoverage.csv") | % {LogWrite $_ $LogFile} 
}
catch {
    LogWrite "Error FTPing" $LogFile
    LogWrite $_ $LogFile

	$subj = "FATAL ERROR: [$RUNMODE] Tamale: Portfolio Coverage FTP downlaod"
	$body = (gc $LogFile | out-string)
	$attachments = @($LogFile)

	Send-MailMessage -SmtpServer $SMTPSERVER -To $TO -Cc $CC -From $FROM -Subject $subj -Body $body -Attachments $attachments

    Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"
	exit 1
}

#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"

exit 0
