#
# $Id: $
# $Source: $
#
# Tamale global variables
#


# run mode
$RUNMODE = "PROD"

# today's date
$currdateiso = get-date -UFormat "%Y%m%d"
$sqlcurrdate = get-date -UFormat "%m-%d-%y"
$currtimestamp = get-date -UFormat "%Y%m%d%H%M%S"

#
# dirs
#
$ROOTDRIVE = "\\seg-ny-fsdev\f1\prod"
$ROOTDIR   = "$ROOTDRIVE\Tamale"
$BINDIR    = "$ROOTDIR\bin"
$SQLDIR    = "$ROOTDIR\sql"
$CONFDIR   = "$ROOTDIR\conf"
$WORKDIR   = "$ROOTDIR\work"
$LOGDIR    = "$ROOTDIR\log"
$ARCHIVE_FILEDIR = "$ROOTDIR\archive\$currdateiso"

#
# email server config
#
#$SMTPSERVER = "seg-ny-exchfr.seg.local"
$SMTPSERVER = "seg-ny-exchfr.seg.local"
$FROM = "axysauto@selectequity.com"
$TO = @("LogsTamale@selectequity.com")
$CC = @("LogsTamale@selectequity.com")

#
# mssql database config
#
$MSSQL_HOSTNAME = "seg-ny-sql4"
$MSSQL_USERNAME = "tamale"
$MSSQL_PASSWORD = "t@mal3"
$MSSQL_DATABASE = "tamaleMarketData"

#
# Tamale ftp log int
#
$TAMALE_FTP_HOSTNAME = "seg-ny-tamale1.seg.local"
$TAMALE_FTP_USERNAME = "tsuploads"
$TAMALE_FTP_PASSWORD = "tamale1"
