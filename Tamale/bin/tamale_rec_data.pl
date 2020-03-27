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
use SEG::Tamale::RestAPI::Util;


#
# globals
#
my ($PROGNAME, $DIR, $SUFFIX) = fileparse($0);
my $BINDIR=$FindBin::Bin;
my $WORKDIR="$FindBin::Bin/../work";

my $DEFAULT_OUTFILE_DELIM = ',';
my $DEFAULT_PORTFOLIO_NAME = 'unspecified_portfolio';

my $ISO_TODAY_DATE = POSIX::strftime('%m-%d-%y', localtime);
my $ISO_TODAY_DATE_LOG = POSIX::strftime('%Y%m%d', localtime);
my $CONFDIR="$BINDIR/../conf";

my $LOG4PERL_CONF_FILENAME = "$CONFDIR/tamale_rec_data_log4perl.conf";

my $HELP;
my $DEBUG;
my $OUT_FILENAME;
my $PROPERTIES_FILENAME;
my $ASOFDATE;



#
#
#
{
	GetOptions(
		'help' => \$HELP,
		'debug' => \$DEBUG,
		'outfile=s' => \$OUT_FILENAME,
 		'asofdate=s' => \$ASOFDATE,
		'properties=s' => \$PROPERTIES_FILENAME,
	) || usage();

	process_cmd_line_args();

    my $properties = SEG::Util::Properties::read_properties_file("$PROPERTIES_FILENAME");

    my $log_filename = $properties->getProperty('port_rec.log_filename');
    $log_filename =~ s/%d/$ISO_TODAY_DATE_LOG/g;

    $ENV{TAMALERECDATA_LOGFILE} = $log_filename;

    eval {
        Log::Log4perl->init($LOG4PERL_CONF_FILENAME);
    };
    if ($@) {
        carp "$PROGNAME: Error: could not read log4perl conf file '$LOG4PERL_CONF_FILENAME': $@";
    }
    my $log = get_logger();
    $log->info();
    $log->info("STARTED $PROGNAME pid=$$");

	my $rc = main($properties,$OUT_FILENAME,$ASOFDATE);

    $log->info("END $PROGNAME");
    $log->info();

	exit $rc;
}


sub usage {
	print <<EOS;
Usage: $0  --properties file_name [--asofdate date] [--outfile file_name] [--help] [--debug]
   --properties             properties filename  
   --outfile                write output into file_name
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

	if (defined $OUT_FILENAME and -e $OUT_FILENAME and ! -w $OUT_FILENAME) {
		print "$PROGNAME: Error: file '$OUT_FILENAME' is not writeable.\n";
		exit 1;
	}

    if(not defined $ASOFDATE) {
        $ASOFDATE = $ISO_TODAY_DATE;
    }
    elsif($ASOFDATE !~ /\d{2}\-\d{2}\-\d{2}/) {
		print "$PROGNAME: Date $ASOFDATE must be in the 'MM-DD-YY' format\n";
		exit 1;
    }
}


#
#
#
sub main {
	my $properties = shift;
	my $out_filename = shift;
    my $asofdate = shift;

	eval {

		if (not defined $out_filename) {
			$out_filename = "$WORKDIR/tamale_recon.xls";
		} 

		reconcile_tamale_data($properties, $out_filename, $asofdate);
	};
	if ($@) {
		cluck "$PROGNAME: $@";
		return 1;
	}

	return 0;
}

sub reconcile_tamale_data {
	my $properties = shift;
	my $out_filename = shift;
    my $asofdate = shift;



	my @tamale_holdings_tab_cols = qw(child-entity-short-name);
	my $tamale_holdings_tab = get_tamale_holdings_table(\@tamale_holdings_tab_cols,$properties);
 
	my @tamale_security_tab_cols = qw(id long-name short-name);
	my $tamale_security_tab = get_tamale_security_data(\@tamale_security_tab_cols,$properties);

    my @axys_holdings_tab_cols = qw(tamale_mapped_symbol Report_Date Security_Symbol Axys_Cusip Axys_Security_Name Missing_In_BB);
	my $axys_holdings_tab = get_axys_holdings_table(\@axys_holdings_tab_cols,$properties,$asofdate);

	my $bb_data_tab = get_bb_data_table($properties);

	my $workbook = Spreadsheet::WriteExcel->new($out_filename) or die "Could not open Excel file for writting $out_filename: $?";

	my ($rec_holdings_summary_msg, $holdingsErrorsFound) = reconcile_holdings($axys_holdings_tab, $tamale_holdings_tab, $workbook);
	my ($rec_data_summary_msg, $dataErrorsFound) = reconcile_security_data($bb_data_tab, $tamale_security_tab, $workbook);

    $workbook->close();

    if($holdingsErrorsFound != 0 or $dataErrorsFound != 0) {
	    mail_xls_file($properties, $out_filename, $rec_holdings_summary_msg . $rec_data_summary_msg);
    }
}


sub reconcile_holdings {
	my $axys_holdings_tab = shift;
	my $tamale_holdings_tab = shift;
	my $workbook = shift;

	my $rule = Data::Reconciliation::Rule->new($axys_holdings_tab, $tamale_holdings_tab);

	$rule->identification(
		['tamale_mapped_symbol','Security_Symbol'], sub { $_[0] eq '' ? $_[1] : uc $_[0] },
		['child-entity-short-name'], sub { uc $_[0] });


	my $r = Data::Reconciliation->new($axys_holdings_tab, $tamale_holdings_tab, -rules => [$rule]);

	$r->build_signatures(0);

	my($widow_signs_1, $widow_signs_2) = $r->delete_wid_signatures;

	my @diffs = $r->reconciliate(0);

	write_portfolio_output_to_spreadsheet(
 		$workbook,
		$axys_holdings_tab, $tamale_holdings_tab,
		$widow_signs_1,
		\@diffs);

    my $axysIssues = scalar(keys %$widow_signs_1);

	my $rec_summary_msg = "in axys only:          $axysIssues\n";
		  

	return ($rec_summary_msg, $axysIssues);
}

sub reconcile_security_data {
	my $bb_data_tab = shift;
	my $tamale_security_tab = shift;
	my $workbook = shift;

	my $rule = Data::Reconciliation::Rule->new($bb_data_tab, $tamale_security_tab);

	$rule->identification(
		['BB_CODE'], sub { uc $_[0] },
		['short-name'], sub { uc $_[0] });

    $rule->add_comparison(['BB_NAME'], sub { $_[0] =~ s/\.|,//g; uc $_[0]},
                          ['long-name'], sub { $_[0] =~ s/\.|,//g; uc $_[0]});

	my $r = Data::Reconciliation->new($bb_data_tab, $tamale_security_tab, -rules => [$rule]);

	$r->build_signatures(0);

	my($widow_signs_1, $widow_signs_2) = $r->delete_wid_signatures;

	my @diffs = $r->reconciliate(0);

	write_data_output_to_spreadsheet(
 		$workbook,
		$bb_data_tab, $tamale_security_tab,
		$widow_signs_1, $widow_signs_2,
		\@diffs);

    my $CompanyIssues = scalar(@diffs);
    my $TickerIssues = scalar(keys %$widow_signs_2);

	my $rec_summary_msg =
		  "Different Company Names:          $CompanyIssues\n" 
		. "Invalid Bloomberg Tickers:        $TickerIssues\n";

	return ($rec_summary_msg,$CompanyIssues+$TickerIssues);
}


sub get_tamale_holdings_table {
	my $tab_cols = shift; # ary ref
	my $properties = shift;

    my $log = get_logger();

    my @portfolios = split(/,/,$properties->getProperty('ports.list')); 

    my $tamale_holdings_tab = Data::Table->new([], [@$tab_cols], 0);	

    foreach my $portfolio (@portfolios) {

        my $portfolio_name = $properties->getProperty($portfolio . '.name');

        my $adv_filter = "((parent-entity short-name equals \"$portfolio_name\") and (relationship-type name equals \"Includes\"))";

	    my $relationships_xml = SEG::Tamale::RestAPI::GetRelationships::_download_relationships_xml($properties, undef, $adv_filter);

	    my $relationships = SEG::Tamale::RestAPI::GetRelationships::get_relationships_from_xml($relationships_xml);
 
	    my $tamale_portfolio_holdings_tab = Data::Table->new([], [@$tab_cols, $portfolio], 0);	
	    foreach my $row (@$relationships) {
		    $tamale_portfolio_holdings_tab->addRow([@$row{@$tab_cols},'X']);
	    }

        $tamale_holdings_tab = $tamale_holdings_tab->join($tamale_portfolio_holdings_tab,Data::Table::FULL_JOIN,[@$tab_cols],[@$tab_cols]);

    }

	$log->info("tamale position rows: ", $tamale_holdings_tab->nofRow(), "\n");

	return $tamale_holdings_tab;
}


sub get_tamale_security_data {
    my $tab_cols = shift;
	my $properties = shift;

    my $log = get_logger();

	my $EntitiesXML = SEG::Tamale::RestAPI::GetEntities::download_entities_xml($properties, 'Corporate');
 
	my $Entities = SEG::Tamale::RestAPI::GetEntities::get_corporate_entities_from_xml($EntitiesXML);

    my $ExceptionTickers = get_exception_tickers($properties);

	my $tamale_security_tab = Data::Table->new([], [@$tab_cols], 0);	
	foreach my $row (@$Entities) {
        if(not defined $ExceptionTickers->{$row->{'short-name'}}) {
		    $tamale_security_tab->addRow([@$row{@$tab_cols}]);
        }
	}

    my $ShortName_Index = $tamale_security_tab->colIndex('short-name');
    my $tamale_security_tab = $tamale_security_tab->match_pattern("\$_->[$ShortName_Index] !~ /_ACQRD|_PRVT|_DLST/");

	$log->info("tamale entity rows: ", $tamale_security_tab->nofRow(), "\n");

	return $tamale_security_tab;
}

sub get_axys_holdings_table {
    my $tab_cols = shift;
    my $properties = shift;
    my $asofdate = shift;

    my @portfolios = split(/,/,$properties->getProperty('ports.list')); 

    my $axys_holdings_tab = Data::Table->new([], [@$tab_cols], 0);	

	my $dbh = SEG::DBIUtil::connect($properties);

    foreach my $portfolio (@portfolios) {

        my $portfolio_name = $properties->getProperty($portfolio . '.name');
        my $side = $properties->getProperty($portfolio . '.side');
        my $dbport = $properties->getProperty($portfolio . '.dbname');

	    my $sql = qq{
            EXEC sp_tamale_portRecs \'$dbport\', \'$asofdate\', \'$side\'
	    };

	    my $axys_portfolio_holdings_tab = Data::Table::fromSQL($dbh, $sql);

        my @default_values = map { 'X' } (1 .. $axys_portfolio_holdings_tab->nofRow());
        $axys_portfolio_holdings_tab->addCol(\@default_values,$portfolio);

        #Join will not match on null so we have to convert the tamale_symbols to empty strings
        $axys_portfolio_holdings_tab->colMap('tamale_mapped_symbol', sub{(not defined $_) ? '' : $_} );

        $axys_holdings_tab = $axys_holdings_tab->join($axys_portfolio_holdings_tab,Data::Table::FULL_JOIN,[@$tab_cols],[@$tab_cols]);
    }

    $dbh->rollback();
    $dbh->disconnect();

	return $axys_holdings_tab;
}

sub get_bb_data_table {
    my $properties = shift;
 
	my $dbh = SEG::DBIUtil::connect($properties);

	my $sql = qq{
SELECT DISTINCT CASE
                WHEN exch_code = 'US'
				THEN ticker
				ELSE ticker + ' ' + exch_code
				END AS BB_CODE, 
				BB_NAME 
FROM bbDailyData 
WHERE px_last is not NULL
	};

	my $axys_holdings_tab = Data::Table::fromSQL($dbh, $sql);
   
    $dbh->rollback();
    $dbh->disconnect();

	return $axys_holdings_tab;
}

sub get_exception_tickers {
    my $properties = shift;

    my $ExclusionFile = $properties->getProperty('port_rec.exclusions_filename');
    open(my $exceptions_FH, $ExclusionFile)
        or die "Could not open exceptions file";

    my %Exception_Tickers;

    while(my $line = <$exceptions_FH>) {
        chomp $line;
        $Exception_Tickers{(uc ($line))} = 1;
    }
    return (\%Exception_Tickers);
}

sub write_portfolio_output_to_spreadsheet {
	my ($workbook,
		$axys_holdings_tab, $tamale_holdings_tab,
		$widow_signs_1,
		$diffs) = @_;
    
    my $log = get_logger(); 

    my @TargetRows = (0) x $axys_holdings_tab->nofRow();
    $TargetRows[$_->[0]] = 1 foreach (values %$widow_signs_1);

    my $MissingAxysHoldings_tab = $axys_holdings_tab->rowMask(\@TargetRows,0);

    my $MissingInTamale_tab = $MissingAxysHoldings_tab->match_pattern('$_->[0] ne "" or $_->[5] == 0');
    $MissingInTamale_tab->delCol("Missing_In_BB");
	write_xl_widow_table_entries($workbook, "Missing In Tamale", $MissingInTamale_tab);

    my $MissingInMapping_tab = $MissingAxysHoldings_tab->match_pattern('$_->[0] eq "" and $_->[5] == 1');
    $MissingInMapping_tab->delCol("Missing_In_BB");
	write_xl_widow_table_entries($workbook, "Missing Mapping(for IT)", $MissingInMapping_tab);

	$log->info("Wrote excel output file\n");
}

sub write_data_output_to_spreadsheet {
	my ($workbook,
		$bb_data_tab, $tamale_security_tab,
		$widow_signs_1, $widow_signs_2,
		$diffs) = @_;

    my $log = get_logger(); 

	write_xl_diff_entries($workbook, "Mismatched Company Names", $bb_data_tab, $tamale_security_tab, $diffs);


    my @TargetRows = (0) x $tamale_security_tab->nofRow();
    $TargetRows[$_->[0]] = 1 foreach (values %$widow_signs_2);

    my $InvalidBBTickers_tab = $tamale_security_tab->rowMask(\@TargetRows,0);
	write_xl_widow_table_entries($workbook, "Invalid Bloomberg Tickers", $InvalidBBTickers_tab);

	$log->info("Wrote excel output file\n");
}

sub write_xl_widow_table_entries {
	my $workbook = shift;
	my $tab_name = shift;
	my $table = shift;

	my $ws = $workbook->add_worksheet($tab_name);

	my $row_idx = 0;
	my $col_idx = 0;

	$ws->write($row_idx++, $col_idx, "$tab_name count: " . $table->nofRow());

	$ws->write_row($row_idx++, $col_idx, [$table->header()]);
    $table->colsMap(sub {$ws->write_row($row_idx++, $col_idx, $_)});
}

sub write_xl_diff_entries {
	my $workbook = shift;
	my $tab_name = shift;
	my $bb_data_table = shift;
	my $tamale_security_table = shift;
	my $diff_array= shift;

    my $log = get_logger(); 

	my $ws = $workbook->add_worksheet($tab_name) or die "Could not open Excell tab: $tab_name: $?";

	my $row_idx = 0;
	my $col_idx = 0;

	$ws->write($row_idx++, $col_idx, "Differences: " . scalar(@$diff_array));

    my $BB_NAME_Col_Index = $bb_data_table->colIndex('BB_NAME');
    my $Tamale_Name_Col_Index = $tamale_security_table->colIndex('long-name');

	if (scalar(@$diff_array)) {
		$ws->write_row($row_idx++, $col_idx, ['Ticker','Bloomberg Name','Tamale Name']);
		foreach my $k (@$diff_array) {
            my $BB_NAME_Row_Index = $k->[1]->[0];
            my $Tamale_Name_Row_Index = $k->[1]->[1];

            my $BB_Name = $bb_data_table->elm($BB_NAME_Row_Index,$BB_NAME_Col_Index);
            my $Tamale_Name = $tamale_security_table->elm($Tamale_Name_Row_Index,$Tamale_Name_Col_Index);
            
		    $ws->write_row($row_idx++, $col_idx, [$k->[0],$BB_Name, $Tamale_Name]);
		}
	}
}



#
#
#
sub mail_xls_file {
	my ($properties, $out_filename, $rec_summary_msg) = @_;

	my ($attachment_filename, $path, $suffix) = fileparse($out_filename);

	my $log = get_logger();

	my $email_subject = "Tamale: Data Reconciliation";
	my $email_body;

	$email_body = "Tamale: Data Reconciliation\n\n";
	$email_body .= $rec_summary_msg;

    my $email_from = $properties->getProperty('port_rec.from_email');
    my @email_to = split(/,/, $properties->getProperty('port_rec.to_email'));
    my @email_cc = split(/,/, $properties->getProperty('port_rec.cc_email'));
    my $mail_ex = $properties->getProperty('port_rec.mail_ex');

    $log->info("Sending recon report to:" . join(',', (@email_to,@email_cc)));
	# create a new multipart message
	my $msg = MIME::Lite->new(
		From    => $email_from,
		To      => join(', ', @email_to),
		Cc      => join(', ', @email_cc),
		Subject => $email_subject,
		Type    => 'multipart/mixed'
	);

	# add parts
	$msg->attach(
		Type     => 'TEXT',
		Data     => $email_body
	);

	$msg->attach(
		Type        => 'application/vnd.ms-excel',
		Path        => $out_filename,
		Filename    => $attachment_filename,
		Disposition => 'attachment'
	);

	eval {$msg->send('smtp', $mail_ex)}; 
    $log->confess("Error sending out email: $@") if $@;

	return 1;
}

package SEG::DBIUtil;
use Log::Log4perl qw(get_logger);
use DBI;

sub connect {
	my $properties = shift;

	my $log = get_logger();

	my $hostname = $properties->getProperty('db.hostname');
	my $mssql_driver = $properties->getProperty('db.driver');
	my $database_name = $properties->getProperty('db.database_name');
	my $username = $properties->getProperty('db.username');
	my $password = $properties->getProperty('db.password');

	my $dbh = DBI->connect("dbi:ODBC:"
		. "DRIVER=$mssql_driver;"
		. "Server=$hostname;"
		. "LogonUser=user;"
		. "LogonAuth=password;"
		. "Trusted_Connection=No;"
		. "MARS_Connection=Yes;",
		$username,
		$password) or $log->logconfess("Could not connect to database '$hostname': $DBI::errstr");

	$dbh->{'RaiseError'} = 1;
	$dbh->{'AutoCommit'} = 0;

	$dbh->do("use $database_name");

	$log->info("connected to mssql db; host=$hostname, database=$database_name, username=$username");

	return $dbh;
}



__END__
