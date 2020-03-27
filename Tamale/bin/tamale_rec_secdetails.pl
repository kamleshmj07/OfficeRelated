#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: tamale_rec_secdetails.pl
#
#        USAGE: ./tamale_rec_secdetails.pl  
#
#  DESCRIPTION: This application checks the security details in Tamale with Bloomberg
#   replacing errant values or adding unpopulated elements.
#
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Jim Blouin (jblouin), jblouin@selectequity.com
# ORGANIZATION: 
#      VERSION: 1.0
#      CREATED: 10/12/2012  1:56:02 PM
#     REVISION: ---
#===============================================================================

use strict;
use warnings;
use utf8;

use FileHandle;
use FindBin;
use Getopt::Long;
use POSIX qw(strftime);
use Log::Log4perl qw(get_logger);
use MIME::Lite;
use File::Basename;
use Carp qw(carp cluck confess);

use Data::Table;
use Data::Reconciliation;
use Data::Reconciliation::Rule;

use Data::Dump qw(dump);

# SEG libraries
use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../../Common/perl/lib";
use SEG::Util::Properties;
use SEG::Tamale::RestAPI::GetEntities;
use SEG::Tamale::RestAPI::GetRelationships;
use SEG::Tamale::RestAPI::DeleteRelationship;
use SEG::Tamale::RestAPI::DepositRelationship;
use SEG::Tamale::RestAPI::Util;

my ($PROGNAME, $DIR, $SUFFIX) = fileparse($0);
my $BINDIR=$FindBin::Bin;
my $WORKDIR="$FindBin::Bin/../work";

my $ISO_TODAY_DATE = POSIX::strftime('%m-%d-%y', localtime);
my $ISO_TODAY_DATE_LOG = POSIX::strftime('%Y%m%d', localtime);
my $CONFDIR="$BINDIR/../conf";

my $LOG4PERL_CONF_FILENAME = "$CONFDIR/tamale_rec_secdetails_log4perl.conf";

my $HELP;
my $DEBUG;
my $PROPERTIES_FILENAME;
my $UPDATE;

{
	GetOptions(
		'help' => \$HELP,
		'debug' => \$DEBUG,
		'properties=s' => \$PROPERTIES_FILENAME,
		'update' => \$UPDATE,
	) || usage();

	process_cmd_line_args();

    my $properties = SEG::Util::Properties::read_properties_file("$PROPERTIES_FILENAME");

    my $log_filename = $properties->getProperty('secdetails.log_filename');
    $log_filename =~ s/%d/$ISO_TODAY_DATE_LOG/g;

    $ENV{TAMALERECSECDETAILS_LOGFILE} = $log_filename;

    eval {
        Log::Log4perl->init($LOG4PERL_CONF_FILENAME);
    };
    if ($@) {
        carp "$PROGNAME: Error: could not read log4perl conf file '$LOG4PERL_CONF_FILENAME': $@";
    }
    my $log = get_logger();
    $log->info();
    $log->info("STARTED $PROGNAME pid=$$");


	my $rc = main($properties);

    $log->info("END $PROGNAME");
    $log->info();

	exit ($rc == 1 ? 0 : $rc);
}

sub usage {
	print <<EOS;
Usage: $0  --properties file_name [--outfile file_name] [--help] [--debug]
   --properties             properties filename  
   --update                 required to make actual updates to tamale
   --outfile                write output into file_name
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
}

sub main {
    my $properties = shift;

    my $log = get_logger();

    #Pull list of Tamale securities
    my $TamaleSecList = GetTamaleSecurities($properties);

    # ['Tamale Relationship Type','bbDailyData Name','Tamale Entity Type']
    my @FieldMappings = ( ['1-GICS Sector','gics_sector_name','GICS Sector'],
                          ['2-GICS Industry','gics_industry_group_name','GICS Industry'],
                          ['3-GICS Sub-Industry','gics_sub_industry_name','GICS Sub-Industry'] );

    #Get security informaion from Tamale pull the relationship type ids which will be used when we
    #add/change gics sectors for securities
    my ($TamaleData_tab, $TamaleRelTypeIDs, $TamaleRelIDs) = getTamaleSecInformation($properties,$TamaleSecList,\@FieldMappings);

    my $TamaleFieldIds = getTamaleFieldIds($properties,\@FieldMappings);

    my $bbSecInfo_tab = getBloombergSecInformation($properties,$TamaleSecList,\@FieldMappings);
 
    my $SecurityDiffs_tab = FindDifferences($properties,$TamaleData_tab,$bbSecInfo_tab,\@FieldMappings);

    my $TamaleMissingFields = RemoveUnUpdateableField($SecurityDiffs_tab, $TamaleFieldIds);

    my $ErrorCount = 0;

    my $MailError = MailMissingFields($properties,$TamaleMissingFields);

    $ErrorCount += $MailError;

    PrintUpdates($properties, $SecurityDiffs_tab);

    if($UPDATE) {
        $ErrorCount += UpdateTamale($properties, $SecurityDiffs_tab, $TamaleSecList, $TamaleRelTypeIDs, $TamaleRelIDs, $TamaleFieldIds);
    }
    else {
        $log->info("***There is no --update flag so no updates were made to Tamale***"); 
    }

    return $ErrorCount;
}

sub GetTamaleSecurities {
    my $properties = shift;

    my $log = get_logger();

    my $entities_xml = SEG::Tamale::RestAPI::GetEntities::download_entities_xml($properties, 'Corporate');
    my $corporate_entities = SEG::Tamale::RestAPI::GetEntities::get_corporate_entities_from_xml($entities_xml);

    my %TamaleSecList = map { uc ( $_->{'short-name'} ) => $_->{id}} @$corporate_entities;

    delete @TamaleSecList{ grep {$_ =~ /_ACQRD|_PRVT|_DLST/} keys %TamaleSecList };

    my $ExceptionTickers = get_exception_tickers($properties);

    delete @TamaleSecList{ grep { defined $ExceptionTickers->{$_} } keys %TamaleSecList };

    #%TamaleSecList = map { $_ => $TamaleSecList{$_} } (sort keys %TamaleSecList)[1 .. 200];

    my $TamaleServer = $properties->getProperty('tamale.hostname');
    $log->info("Found " . (scalar keys %TamaleSecList) . " valid securities in Tamale on $TamaleServer"); 

    return(\%TamaleSecList);
}

sub get_exception_tickers {
    my $properties = shift;

    my $log = get_logger();

    my $ExclusionFile = $properties->getProperty('port_rec.exclusions_filename');
    open(my $exceptions_FH, $ExclusionFile)
        or $log->confess("Could not open exceptions file: $ExclusionFile");

    my %Exception_Tickers;

    while(my $line = <$exceptions_FH>) {
        chomp $line;
        $Exception_Tickers{(uc ($line))} = 1;
    }
    return (\%Exception_Tickers);
}

sub getTamaleSecInformation {
    my $properties = shift;
    my $TamaleSecList = shift;
    my $FieldMappings = shift;

    my $log = get_logger();

    $log->info("Pulling Security informaion from Tamale");

    my $ua = SEG::Tamale::RestAPI::GetRelationships::create_lwp_user_agent_obj($properties);

    my @TamaleFields = map {$_->[0]} @$FieldMappings;
    my $TamaleFields_str = join(';',@TamaleFields); 

    my $TamaleData_tab = Data::Table->new([], ['Ticker',@TamaleFields], 0);	
    my %TamaleRelTypeIDs;
    my %TamaleRelIDs;

    my $adv_filter = "(relationship-type name contains \"GICS\")";
	my $relationships_xml = SEG::Tamale::RestAPI::GetRelationships::_download_relationships_xml($properties, $ua, $adv_filter);

	my $all_relationships = SEG::Tamale::RestAPI::GetRelationships::get_relationships_from_xml($relationships_xml);

    my %RelationshipsBySecurity;
    foreach my $relationship (@$all_relationships) {
        my $SecurityName = $relationship->{"parent-entity-id"};
        push(@{$RelationshipsBySecurity{$SecurityName}},$relationship);
    }

    foreach my $Security (keys %$TamaleSecList) {
        my $SecurityID = $TamaleSecList->{$Security};

        my $relationships = $RelationshipsBySecurity{$SecurityID};

        my %RelHash = map { $_->{"child-entity"} =~ s/\s-\s(Industry|Sector|Sub-Industry)$//g;
                            $_->{"relationship-type"} => $_->{"child-entity"} } @$relationships;
        my @NewTableRow = ($Security);
        foreach my $Field (@TamaleFields) {
            if(defined $RelHash{$Field}) {
                push(@NewTableRow, $RelHash{$Field}); 
            }
            else {
                push(@NewTableRow, ''); 
            }
        }

        foreach (@$relationships) {
            my $RelationshipType = $_->{"relationship-type"};
            my $Field = $_->{"child-entity"};
            $Field =~ s/\s-\s(Industry|Sector|Sub-Industry)$//g;

            if(not defined $TamaleRelTypeIDs{$RelationshipType}) {
                $TamaleRelTypeIDs{$RelationshipType} = $_->{"relationship-type-id"} 
            }

            $TamaleRelIDs{$RelationshipType}{$Security} = $_->{id} 
        }

        $TamaleData_tab->addRow(\@NewTableRow);
    }
    return($TamaleData_tab,\%TamaleRelTypeIDs,\%TamaleRelIDs);
}

sub getBloombergSecInformation {
    my $properties = shift;
    my $TamaleSecList = shift;
    my $FieldMappings = shift;

    my $log = get_logger();

    my @bbFields = map {$_->[1]} @$FieldMappings;

	my $dbh = SEG::DBIUtil::connect($properties);

    my $bbFields_str = join(',',@bbFields);

    my $sql = <<HEREDOC;
    SELECT DISTINCT 
            (CASE
	         WHEN exch_code = 'US'
			 THEN Ticker
			 ELSE Ticker + ' ' + exch_code
			 END) as Ticker, $bbFields_str 
    FROM bbDailyData 
    WHERE LEN(bb_code) > 7 
HEREDOC

	my $bbSecInfo_tab = Data::Table::fromSQL($dbh, $sql);

    my $TamaleSecList_Array = [keys %$TamaleSecList];
    my $TamaleSecList_tab = Data::Table->new([$TamaleSecList_Array], [('Ticker')], 1);	

    $bbSecInfo_tab = $bbSecInfo_tab->join($TamaleSecList_tab,Data::Table::INNER_JOIN,['Ticker'],['Ticker']);

    #This is fix because when the gics_sub_industry_name blank there is sometimes a Q places in the field
    #...Oscar
    if(defined $bbSecInfo_tab->{colHash}->{gics_sub_industry_name}) {
        $bbSecInfo_tab->colMap('gics_sub_industry_name',sub { return ((defined $_ and $_ eq 'Q') ? undef : $_) });
    }

    #Tamale doesn't allow commas in names do we have to remove them
    foreach my $bbField (@bbFields) {
        $bbSecInfo_tab->colMap($bbField,sub { $_ =~ s/,//g if(defined $_); return ($_); });
    }

    $dbh->rollback();
    $dbh->disconnect();

    $log->info("Found " . ($bbSecInfo_tab->nofRow()) . " securities in bbDailyData");

	return $bbSecInfo_tab;
}

sub FindDifferences {
    my $properties = shift;
    my $TamaleData_tab = shift; 
    my $bbSecInfo_tab = shift; 
    my $FieldMappings = shift; 

    my $log = get_logger();

    my $Diff_tab = Data::Table->new([], [('Ticker','Field','Tamale','BB')], 1);	

    foreach my $Fields (@$FieldMappings) {
	    my $rule = Data::Reconciliation::Rule->new($TamaleData_tab, $bbSecInfo_tab);

        my $TamaleField = $Fields->[0];
        my $BBField = $Fields->[1];

    	$rule->identification(
            ['Ticker'], sub { uc $_[0] },
            ['Ticker'], sub { uc $_[0] });

	    $rule->add_comparison([$TamaleField], sub{ uc $_[0] },
                              [$BBField], sub{ uc $_[0] });

	    my $r = Data::Reconciliation->new($TamaleData_tab, $bbSecInfo_tab, -rules => [$rule]);

	    $r->build_signatures(0);

        $r->delete_dup_signatures;
        $r->delete_wid_signatures;

        my @diffs = $r->reconciliate(0);

        #Now we are going to put this in a neat little table
        my $Tamale_Name_Col_Index = $TamaleData_tab->colIndex($TamaleField);
        my $BB_Name_Col_Index = $bbSecInfo_tab->colIndex($BBField);

        foreach my $k (@diffs) {
            my $Ticker = $k->[0];
            my $Tamale_Name_Row_Index = $k->[1]->[0];
            my $BB_Name_Row_Index = $k->[1]->[1];

            my $Tamale_Name = $TamaleData_tab->elm($Tamale_Name_Row_Index,$Tamale_Name_Col_Index);
            my $BB_Name = $bbSecInfo_tab->elm($BB_Name_Row_Index,$BB_Name_Col_Index);
            
            $Diff_tab->addRow([$Ticker,$TamaleField,$Tamale_Name,$BB_Name]);
        }
    }

    $log->info(($Diff_tab->nofRow()) . " differences between Tamale and Bloomberg fields");

    return($Diff_tab);
}

sub getTamaleFieldIds {
    my $properties = shift;
    my $FieldMappings = shift; 

    my @TamaleFieldTypes = map {$_->[2]} @$FieldMappings;

    my %TamaleFieldIds;

    foreach my $Field (@$FieldMappings) {

        my $RelType = $Field->[0];
        my $EntityType = $Field->[2];

        my $entities_xml = SEG::Tamale::RestAPI::GetEntities::download_entities_xml($properties,$EntityType);
        my $entities = SEG::Tamale::RestAPI::GetEntities::get_entities_from_xml($entities_xml);

        my %NameToId = map {$_->{"short-name"} =~ s/\s-\s(Industry|Sector|Sub-Industry)$//g;
                           $_->{"short-name"} => $_->{id}} @$entities;

        $TamaleFieldIds{$RelType} = \%NameToId;
    }

    return(\%TamaleFieldIds);
}

sub PrintUpdates {
    my $properties = shift;
    my $SecurityDiffs_tab = shift;

    my $log = get_logger();

    $SecurityDiffs_tab->sort('Tamale',1,1);
 
    my $OutFile = $properties->getProperty('secdetails.out_filename');
    open(my $FH, "> $OutFile") or 
       $log->confess("Could not open output file: $OutFile");

    print $FH $SecurityDiffs_tab->csv;

    close($FH);

    return 0;
}

sub UpdateTamale { 
    my $properties = shift; 
    my $SecurityDiffs_tab = shift;
    my $TamaleSecList = shift;
    my $TamaleRelTypeIDs = shift;
    my $TamaleRelIDs = shift;
    my $TamaleFieldIds = shift;

    my $log = get_logger();

    my $ua = SEG::Tamale::RestAPI::DepositRelationship::create_lwp_user_agent_obj($properties);

    my $ChangeCount = 0;
    my $ErrorCount = 0;


    foreach my $RowIndex (0 .. $SecurityDiffs_tab->nofRow - 1) {
        my $RowHash = $SecurityDiffs_tab->rowHashRef($RowIndex);
        
        my $Ticker = $RowHash->{Ticker};
        my $Field = $RowHash->{Field};
        my $Tamale = $RowHash->{Tamale};
        my $BB = $RowHash->{BB};

        eval {
            if(defined $Tamale and $Tamale ne '') {
                my $RelID = $TamaleRelIDs->{$Field}->{$Ticker};
                SEG::Tamale::RestAPI::DeleteRelationship::delete_relationship($properties,$ua,$RelID);
            }
    
            my $ParentID = $TamaleSecList->{$Ticker};
            my $ChildID = $TamaleFieldIds->{$Field}->{$BB};
            my $RelTypeID = $TamaleRelTypeIDs->{$Field};
            SEG::Tamale::RestAPI::DepositRelationship::deposit_relationship($properties,$ua,$ParentID,$ChildID,$RelTypeID);
        };
        if($@) {
            $ErrorCount++; 
            $log->error("Could not deposit $Field relationship of $BB for $Ticker:\n $@");
        }
        else {
            $ChangeCount++;
        }
    }

   $log->info("$ChangeCount fields were added or updated");
 
   return($ErrorCount);
}

#This function updates the SecurityDiffs_tab table
sub RemoveUnUpdateableField {
    my $SecurityDiffs_tab = shift;
    my $TamaleFieldIds = shift;

    my $log = get_logger();

    my $BBNameColIndex = $SecurityDiffs_tab->colIndex('BB');
    my $FieldColIndex = $SecurityDiffs_tab->colIndex('Field');

    my %TamaleMissingFields;
    my @RowsToDelete;
    foreach my $RowIndex (0 .. $SecurityDiffs_tab->nofRow - 1) {
        my $Field = $SecurityDiffs_tab->elm($RowIndex, $FieldColIndex);
        my $BBName = $SecurityDiffs_tab->elm($RowIndex, $BBNameColIndex);

        if(defined $Field and defined $BBName and not defined $TamaleFieldIds->{$Field}{$BBName}) {
            $TamaleMissingFields{$Field}{$BBName} = 1;
            push(@RowsToDelete,$RowIndex);
        } 

        #Get rid of the rows where we don't have a BB Fields
        if(not defined $BBName or $BBName eq '') {
            push(@RowsToDelete,$RowIndex);
        }
    }
    $SecurityDiffs_tab->delRows(\@RowsToDelete);

    $log->info(scalar(@RowsToDelete) . " fields will not be updated as the BB field is empty or relationship is not in Tamale");

    return(\%TamaleMissingFields);
}

#
#
#
sub MailMissingFields {
	my ($properties, $TamaleMissingFields) = @_;

	my $log = get_logger();

	my $EmailSubject = "Tamale: Missing Tamale Entities";

	my $EmailBody;
	$EmailBody = "The autoamated process that updates Tamale security information needs the following Entities to be created in Tamale:\n\n";

    my $AtLeastOneEntity = 0;
 
    foreach my $FieldType (keys %$TamaleMissingFields) {
        $EmailBody .= "$FieldType\n";
        foreach my $FieldName (keys %{$TamaleMissingFields->{$FieldType}}) {
            $EmailBody .= "\t$FieldName\n";
            $AtLeastOneEntity = 1;
        } 
    }
    $EmailBody .= "\n------------------------------------------------------------------------------------------\n";

    return 0 unless $AtLeastOneEntity;

    my $EmailFrom = $properties->getProperty('secdetails.from_email');
    my @EmailTo	 = split(/,/, $properties->getProperty('secdetails.to_email'));
    my @EmailCC = split(/,/, $properties->getProperty('secdetails.cc_email'));
    my $MailEx = $properties->getProperty('secdetails.mail_ex');

	# create a new multipart message
	my $msg = MIME::Lite->new(
		From    => $EmailFrom,
		To      => join(', ', @EmailTo),
		Cc      => join(', ', @EmailCC),
		Subject => $EmailSubject,
		Type    => 'multipart/mixed'
	);

	$msg->attach(
		Type     => 'TEXT',
		Data     => $EmailBody
	);

	$msg->send('smtp', $MailEx) || ($log->error("Error sending out email") and return 1);

	return 0;
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
