#!/usr/bin/perl
#
# Reconciles security data with Tamale data 
#

use strict;

use Carp qw(carp cluck confess);
use FileHandle;
use Getopt::Long;
use File::Basename;
use File::Copy;
use FindBin;
use POSIX qw(strftime);
use Log::Log4perl qw(get_logger);
use MIME::Lite;
use Net::SFTP::Foreign;

use Data::Table;
use Data::Reconciliation;
use Data::Reconciliation::Rule;

use Spreadsheet::WriteExcel;

use Data::Dump qw(dump);

# SEG libraries
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../../Common/perl/lib";
use SEG::Util::Properties;
use SEG::Tamale::RestAPI::GetEntities;
use SEG::Tamale::RestAPI::GetRelationships;
use SEG::Tamale::RestAPI::DepositResearch;
use SEG::Tamale::RestAPI::Util;
use SEG::DBI::Util;

#
# globals
#
my ($PROGNAME, $DIR, $SUFFIX) = fileparse($0);
my $BINDIR=$FindBin::Bin;
my $WORKDIR="$FindBin::Bin/../work";

my $ISO_TODAY_DATE = POSIX::strftime('%Y%m%d', localtime);
my $CONFDIR="$BINDIR/../conf";

my $LOG4PERL_CONF_FILENAME = "$CONFDIR/tamale_ISS_extract_log4perl.conf";

my $HELP;
my $DEBUG;
my $PROPERTIES_FILENAME;
my $ASOFDATE;

my %ISS_VOTING_POLICY_REPORTS = (
	1 => {
		'description' => 'ISS Proxy Analysis & Benchmark Policy Voting Recommendations',
		'filename_code' => 'ISSProxyBenchmarkPolicy' 
	},
	166 => {
		'description' => 'Social Advisory Services Policy Voting Recommendations',
		'filename_code' => 'SocialAdvisoryPolicy' 
	}
);


#
#
#
{
	GetOptions(
		'help' => \$HELP,
		'debug' => \$DEBUG,
 		'asofdate=s' => \$ASOFDATE,
		'properties=s' => \$PROPERTIES_FILENAME,
	) || usage();

	process_cmd_line_args();

    my $properties = SEG::Util::Properties::read_properties_file("$PROPERTIES_FILENAME");

    my $log_filename = $properties->getProperty('iss_extract.log_filename');
    $log_filename =~ s/%d/$ISO_TODAY_DATE/g;

    $ENV{ISSEXTRACT_LOGFILE} = $log_filename;

    eval {
        Log::Log4perl->init($LOG4PERL_CONF_FILENAME);
    };
    if ($@) {
        carp "$PROGNAME: Error: could not read log4perl conf file '$LOG4PERL_CONF_FILENAME': $@";
    }
    my $log = get_logger();
    $log->info();
    $log->info("STARTED $PROGNAME pid=$$");

	my $rc = main($properties,$ASOFDATE);

    $log->info("END $PROGNAME");
    $log->info();

	exit ($rc);
}


sub usage {
	print <<EOS;
Usage: $0  --properties file_name [--asofdate date(YYYYMMDD)] [--help] [--debug]
   --properties             properties filename  
   --asofdate               date of the portfolio recon (default: today)
   --debug
   --help                   prints this help screen
EOS

	exit 1;	
}

sub process_cmd_line_args {
	usage() if $HELP;

    if(not defined $PROPERTIES_FILENAME) {
		print "$PROGNAME: Must include a properties file (--properties)\n"; 
		exit 1;
    }
    elsif (!(-e $PROPERTIES_FILENAME)){
        print "$PROGNAME: Error: file $PROPERTIES_FILENAME is does not exist.\n";
		exit 1;
    }

    if(not defined $ASOFDATE) {
        $ASOFDATE = $ISO_TODAY_DATE;
    }
    elsif($ASOFDATE !~ /\d{4}\d{2}\d{2}/) {
		print "$PROGNAME: Date $ASOFDATE must be in the 'YYYYMMDD' format\n";
		exit 1;
    }
}


#
#
#
sub main {
	my $properties = shift;
    my $asofdate = shift;

    my $log = get_logger();
 
    my $ErrorCount = 0;

    my $Host = $properties->getProperty('iss_extract.sftp_hostname');
    my $Username = $properties->getProperty('iss_extract.sftp_username');
    my $Password = $properties->getProperty('iss_extract.sftp_password');

    $log->info("Running for date: $asofdate");

    my %Args = ( 'user' => $Username,
                 'password' => $Password,
                 'ssh_cmd' => 'C:\\PuTTy\\\plink.exe');

    #Using plink will cause warning because it sends plink a plain text password...oops
    my $sftp = Net::SFTP::Foreign->new($Host,%Args);


    $log->info("Pushing holdings file to SFTP: $Host");
    #Push holdings file
	eval {
        my $HoldingsFilename = CreateHoldingsFile($properties, $asofdate);

        PushHoldingsFile($properties, $sftp, $HoldingsFilename);
    };
	if ($@) {
        $log->error("There was an error loading the holdings file:" . $@);
        $ErrorCount++;
	}

    $log->info("Reading proxy report list");

    #Get list of reports
    my ($ReportInformation) = ReadProxyResearchList($properties, $sftp, $asofdate); 

    my $ua = SEG::Tamale::RestAPI::GetRelationships::create_lwp_user_agent_obj($properties);

    my $NoteType = $properties->getProperty('iss_extract.note_type');
   
    #Downlaod the reports from the SFTP site and upload it to Tamale
    foreach my $Report_hash (@$ReportInformation) {
        my $PDFFile = DownloadPDF($properties,$sftp,$Report_hash->{ReportName});

        my $SEDOL = $Report_hash->{SEDOL};
        my $CUSIP = $Report_hash->{CUSIP};
        my $ISIN = $Report_hash->{ISIN};
        my $ReportFilenameCode = $Report_hash->{ReportFilenameCode};
        my $ReportDetails = $Report_hash->{ReportDetails};

        my $TickerList = GetPossibleTickers($properties,$SEDOL,$CUSIP,$ISIN);
        my $LocalPath = $properties->getProperty('iss_extract.localreport_path');

        my $ValidUpload = 0;
        my $PDFFile_withTicker;
        foreach my $Ticker (@$TickerList) {
	        eval {
                $log->info("Trying to upload PDF report to Tamale ticker: $Ticker");

                $PDFFile_withTicker = $LocalPath . "${Ticker}_${ReportFilenameCode}_$asofdate.pdf";
				$PDFFile_withTicker =~ tr(/*)(_); # some Bloomberg tickers will have / or * chars

				my $TamaleNoteSubject = "$Ticker Proxy Vote ($ReportFilenameCode)";

                copy($PDFFile,$PDFFile_withTicker);

                SEG::Tamale::RestAPI::DepositResearch::deposit_research($properties,
                                                                        $ua,
                                                                        $Ticker,
                                                                        $TamaleNoteSubject,
                                                                        $ReportDetails,
                                                                        $PDFFile_withTicker,
                                                                        $NoteType);
	        };
	        if ($@) {
                $log->info("There was an issue uploading the file: $PDFFile_withTicker for $Ticker\n" . $@);
                unlink($PDFFile_withTicker);
	        }
            else {
                $log->info("Successfully uploaded proxy report $PDFFile_withTicker for the Tamale ticker $Ticker\n");
                $ValidUpload = 1;
            }
        }
        if(not $ValidUpload) {
           if(scalar(@$TickerList) > 0) {
               $log->error("Could not upload the proxy report using ticker(s): " . join ',',@$TickerList);
           }
           else {
               $log->error("Could not find any tickers for SEDOL: $SEDOL CUSIP $CUSIP ISIN: $ISIN in bbDailyData");
           }
           $ErrorCount++;
        }
    }
	return $ErrorCount;
}

sub CreateHoldingsFile {
    my $properties = shift;
    my $asofdate = shift;

    my $log = get_logger();

    my $dbh = SEG::DBI::Util::connect($properties) ;

    my $sql = <<HEREDOC;
    SELECT UPPER(Cusip), CONVERT(varchar,CAST(Purchase_Date.Report_Date AS DATETIME),101)
    FROM AX_PortfolioAppraisal_Massive PA
    LEFT OUTER JOIN (SELECT Security_symbol, MIN(asof_dt) Report_Date
                     FROM AX_PortfolioAppraisal_Massive
                     GROUP BY Security_symbol) Purchase_Date
    ON Purchase_Date.Security_Symbol = PA.Security_Symbol
    WHERE Portfolio_Name = '\@firmwide'
    AND PA.asof_dt = (SELECT MAX(asof_dt) 
	                  FROM AX_PortfolioAppraisal_Massive 
					  WHERE asof_dt <= '$asofdate')
    AND LEFT(Security_Type_Code,2) = 'cs'
    AND Cusip IS NOT NULL
HEREDOC

    my $sth = $dbh->prepare($sql);

    $sth->execute();

    my $HoldingsFilename = $properties->getProperty('iss_extract.holdings_filename');
    $HoldingsFilename =~ s/%d/$ISO_TODAY_DATE/g;

    open(my $FH, "> $HoldingsFilename") or
        $log->confess("Could not open $HoldingsFilename for writting");

    my $Count = 0;

    while(my @Row = $sth->fetchrow_array) { 
        my $id = $Row[0];
        my $PurchaseDate = $Row[1];
        print $FH "$id|NONE|$PurchaseDate\n";
        $Count++;
    }

    $log->info("Wrote $Count rows to the holdings file");

    if($Count == 0) {
        $log->confess("Did not get any rows from the AX_PortfolioAppraisal_Massive table for $asofdate.  A new holdings file will not be produced today.");
    }

    close($FH);

    $dbh->rollback();
    $dbh->disconnect();

    return($HoldingsFilename);
}

sub PushHoldingsFile {
    my $properties = shift;
    my $sftp = shift;
    my $HoldingsFilename = shift;

    my $log = get_logger();

    my ($HoldingsFile_NoPath) = fileparse($HoldingsFilename);
   
    $log->info("Removing everyting in remote directory /holdings/.");

	my @files = $sftp->glob('/holdings/*');
	for my $f (@files) {
		my $path = $f->{filename};
		$sftp->remove($path)
			or $log->warn("Could not remove stale holdings file '$path':" . $sftp->error);
		$log->info("Removed stale holdings file '$path'.");
	}
#    $sftp->rremove('/holdings/.')
#        or $log->warn("Could not remove existing contents of /holdings/");

    $log->info("Uploading Holdings file: $HoldingsFile_NoPath");

    $sftp->put($HoldingsFilename,"/holdings/$HoldingsFile_NoPath",(copy_perm => 0,copy_time => 0))
        or $log->confess("Could not put holings file on SFTP server: " . $sftp->error);

}

sub ReadProxyResearchList {
    my $properties = shift;
    my $sftp = shift;
    my $asofdate = shift;

    
    my $log = get_logger();

    my $RemoteFile = $properties->getProperty('iss_extract.remoteproxyfile');
    my $LocalFile = $properties->getProperty('iss_extract.remoteproxyfile_local');

    my $YYYY = substr($asofdate,0,4);
    my $MM = substr($asofdate,4,2);
    my $DD = substr($asofdate,6,2);
    my $RemoteDATEFormat = $MM . $DD . $YYYY;
    $RemoteFile =~ s/%d/$RemoteDATEFormat/g;

    $sftp->get($RemoteFile,$LocalFile)
        or $log->confess("Could not get proxy research mapping fomr SFTP server: " . $sftp->error);

    open(my $FH, $LocalFile)
        or $log->confess("Could not read Proxy Mapping file: " . $?);

    #Map each field to the index of the data
    my $Header = <$FH>;
    chomp $Header;
    my @Header_Array = split '\|', $Header;
    my %Header_Hash = map { $Header_Array[$_] => $_ } 0..$#Header_Array;

    my @ReportInformation;

    while(my $Row = <$FH>) {
        chomp $Row;
        my @Row_Array = split '\|', $Row;
        my %Report;
       
        my $CompanyName = $Row_Array[$Header_Hash{'CompanyName'}];
        my $Ticker = $Row_Array[$Header_Hash{'Ticker'}];
        my $MeetingID = $Row_Array[$Header_Hash{'MeetingID'}];
        my $VotingPolicyID = $Row_Array[$Header_Hash{'VotingPolicyID'}];
        my $MeetingDate = $Row_Array[$Header_Hash{'MeetingDate'}];
        my $SEDOL = $Row_Array[$Header_Hash{'SEDOL'}];
        my $CUSIP = $Row_Array[$Header_Hash{'CUSIP'}];
        my $ISIN = $Row_Array[$Header_Hash{'ISIN'}];

        my $ReportDetails = <<HEREDOC;
Company Name: $CompanyName
Ticker: $Ticker
Meeting ID: $MeetingID
Voting Policy ID: $VotingPolicyID
Voting Policy Description: $ISS_VOTING_POLICY_REPORTS{$VotingPolicyID}->{description}
Meeting Date: $MeetingDate
SEDOL: $SEDOL
CUSIP: $CUSIP
ISIN: $ISIN
HEREDOC

        $Report{SEDOL} = $SEDOL;
        $Report{CUSIP} = $CUSIP;
        $Report{ISIN} = $ISIN;
        $Report{ReportName} = "${MeetingID}_${VotingPolicyID}.pdf";
        $Report{ReportFilenameCode} = $ISS_VOTING_POLICY_REPORTS{$VotingPolicyID}->{'filename_code'};
        $Report{ReportDetails} = $ReportDetails;

        push(@ReportInformation,\%Report);
    }

    return(\@ReportInformation);
}

sub DownloadPDF{
    my $properties = shift;
    my $sftp = shift;
    my $PDFReport = shift;

    my $ReportPath = $properties->getProperty('iss_extract.remotereport_path');
    my $LocalPath = $properties->getProperty('iss_extract.localreport_path');

    $sftp->get($ReportPath . $PDFReport,$LocalPath . $PDFReport);

    return($LocalPath . $PDFReport);
}

sub GetPossibleTickers {
    my $properties = shift;
    my $SEDOL = shift;
    my $CUSIP = shift;
    my $ISIN = shift;

    my $dbh = SEG::DBI::Util::connect($properties) ;

    my $sql = <<HEREDOC;
    SELECT CASE exch_code
               WHEN 'US' THEN Ticker
               ELSE Ticker + ' ' + EXCH_CODE
           END
    FROM bbDailyData bbdd
    WHERE bbdd.id_bb_company IN (SELECT id_bb_company
                                 FROM bbDailyData
                                 WHERE id_sedol1 = '$SEDOL'
                                 OR id_cusip = '$CUSIP'
                                 OR id_isin = '$ISIN')
HEREDOC

    my $sth = $dbh->prepare($sql);

    $sth->execute();

	my $rset = $sth->fetchall_arrayref();
    my @TickerList = map{$_->[0]} @$rset;

    $dbh->rollback();
    $dbh->disconnect();

    return(\@TickerList); 
}
__END__
