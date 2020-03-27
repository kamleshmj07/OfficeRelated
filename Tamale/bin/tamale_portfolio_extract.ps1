#
# $Id: $
# $Source: $
#
# Generates axys positions report.
#

#####################################################################################
#Init Code
#####################################################################################
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
#####################################################################################

#
# Load portfolio weights into Tamale table
#
if(! $asofdate) {
    $asofdate = $sqlcurrdate
}

$PortsName = @{"segCooperSquareLong" = "coopsq";
               "segCooperSquareShort" = "coopsq";
               "segGrJones" = "@gsgrjone";
               "segPartnersLong" = "@seghedge";
               "segPartnersShort" = "@seghedge";
               "segSMID" = "@active";
               "segFirmwide" = "@firmwide";
               "segVandam" = "vandamlp";
               "segSJPGlobal" = "sjpall";
               "segSJPLargeCap" = "sjplarge";
			   "segChimcog" = "@chimcog";
			   "segBlackwal" = "@blackwal";
			   "segBaxter" = "@cooplong";
			   "segUCITS" = "ucits"}

$PortsSide = @{"segCooperSquareLong" = "long";
               "segCooperSquareShort" = "short";
               "segGrJones" = "";
               "segPartnersLong" = "long";
               "segPartnersShort" = "short";
               "segSMID" = "";
               "segFirmwide" = "";
               "segVandam" = "";
               "segSJPGlobal" = "";
               "segSJPLargeCap" = "";
			   "segChimcog" = "";
			   "segBlackwal" = "";
			   "segBaxter" = "";
			   "segUCITS" = ""}

foreach($port in $PortsName.keys) {
    LogWrite "Extracting $port from Axys Database." $LogFile

    rm -erroraction SilentlyContinue $WORKDIR\$port.csv
    
    echo "axys_symbol" | Out-File $WORKDIR\$port.csv -encoding ASCII
    $SQLCommand = "exec dbo.sp_exportPortfolios '" + $PortsName[$port] + "','$asofdate', '" + $PortsSide[$port] + "'"
    echo $SQLCommand
    $SQLExp = "`"SQLCMD.exe`" -S $MSSQL_HOSTNAME -U $MSSQL_USERNAME -P $MSSQL_PASSWORD -Q `"$SQLCommand`" -b -s `",`" -W -h -1"
    Invoke-Expression "& $SQLExp" | out-file -filepath $WORKDIR\$port.csv -append -encoding ASCII

if ($LastExitCode -ne 0) {
	$subj = "FATAL ERROR: [$RUNMODE] Tamale: Axys portfolio extract"
	$body = (gc $WORKDIR\$port.csv | out-string)
	$attachments = @("$WORKDIR\$port.csv")

	Send-MailMessage -SmtpServer $SMTPSERVER -To $TO -Cc $CC -From $FROM -Subject $subj -Body $body -Attachments $attachments
	exit
}

}
LogWrite "Finished extracting portfolios." $LogFile

$webclient = New-Object System.Net.WebClient
$webclient.Credentials = New-Object System.Net.NetworkCredential($TAMALE_FTP_USERNAME ,$TAMALE_FTP_PASSWORD ) 

$HadError = 0

foreach($port in $PortsName.keys) {
    LogWrite "FTPing $port to Tamale." $LogFile
    $ftp = "ftp://$TAMALE_FTP_HOSTNAME/public/$port.csv"

    try {
        $webclient.UploadFile($ftp, "$WORKDIR\$port.csv") | % {LogWrite $_ $LogFile} 
        copy $WORKDIR\$port.csv $ARCHIVE_FILEDIR\$port_$currdateiso.csv
    }
    catch {
        LogWrite "Error FTPing." $LogFile
        LogWrite $_ $LogFile
    
	    $subj = "FATAL ERROR: [$RUNMODE] Tamale: $port Portfolio FTP downlaod"
	    $body = (gc $LogFile | out-string)
	    $attachments = @($LogFile)
    
	    Send-MailMessage -SmtpServer $SMTPSERVER -To $TO -Cc $CC -From $FROM -Subject $subj -Body $body -Attachments $attachments
    
        Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"
 
        $HadError = 1
    }
}

#####################################################################################
LogWrite "[$(get-date)] END $($MyInvocation.MyCommand.Name)" $LogFile

Copy-Item $LogFile "$LOGDIR\$currdateiso\$($MyInvocation.MyCommand.Name).$currtimestamp.log"

exit $HadError
