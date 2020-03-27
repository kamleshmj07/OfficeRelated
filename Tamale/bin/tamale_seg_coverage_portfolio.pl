#!/usr/bin/local/perl
#
# Connects to Tamale via REST API and extracts all the
# entities for input into the bloomberg pricing process
#

use strict;

use Carp qw(carp cluck confess);
use FileHandle;
use Getopt::Long;
use File::Basename;
use FindBin;
use XML::Simple;
use POSIX qw(strftime);
use Data::Dump qw(dump);

# SEG libraries
#use lib "$FindBin::Bin/../lib";
use lib "$FindBin::Bin/../../Common/perl/lib";
use SEG::Util::Properties;
use SEG::Tamale::RestAPI::GetEntities;
use SEG::Tamale::RestAPI::GetRelationships;


#
# globals
#
my ($PROGNAME, $DIR, $SUFFIX) = fileparse($0);
my $BINDIR=$FindBin::Bin;
my $WORKDIR="$BINDIR/../work";

my $DEFAULT_PROPERTIES_FILENAME = 'tamale.properties_PROD';

my $OUTFILE_DELIM = ',';

my $ISO_TODAY_DATE = POSIX::strftime('%Y%m%d', localtime);

my $HELP;
my $DEBUG;
my $IN_FILENAME;
my $OUT_FILENAME;
my $PROPERTIES_FILENAME;



#
#
#
{
	GetOptions(
		'help' => \$HELP,
		'debug' => \$DEBUG,
		'infile=s' => \$IN_FILENAME,
		'outfile=s' => \$OUT_FILENAME,
		'properties=s' => \$PROPERTIES_FILENAME,
	) || usage();

	process_cmd_line_args();

	my $rc = main($IN_FILENAME, $OUT_FILENAME);
	exit ($rc == 1 ? 0 : $rc);
}


sub usage {
	print <<EOS;
Usage: $0 [--infile file_name] [--properties file_name] [--outfile file_name] [--help] [--debug]
   --properties  properties filename (default: $DEFAULT_PROPERTIES_FILENAME)
   --infile      use file_name as the source of entities instead of querying Tamale
   --outfile     write output into file_name
   --debug
   --help        prints this help screen
EOS

	exit 1;	
}

sub process_cmd_line_args {
	usage() if $HELP;

	if (defined $OUT_FILENAME and -e $OUT_FILENAME and ! -w $OUT_FILENAME) {
		print "$PROGNAME: Error: file '$OUT_FILENAME' is not writeable.";
		exit 1;
	} elsif (!defined $OUT_FILENAME) {
		$OUT_FILENAME = '-'; # STDOUT
	}

	if (defined $IN_FILENAME and ! -r $IN_FILENAME) {
		print "$PROGNAME: Error: file '$IN_FILENAME' is not readable.";
		exit 1;
	}

	$PROPERTIES_FILENAME = $DEFAULT_PROPERTIES_FILENAME
		if not defined $PROPERTIES_FILENAME;

	unless (-r "$BINDIR/$PROPERTIES_FILENAME") {
		print "$PROGNAME: Error: properties file '$PROPERTIES_FILENAME' is not readable.";
		exit 1;
	}
}


#
#
#
sub main {
	my $in_filename = shift;
	my $out_filename = shift;

	eval {
		my $properties = SEG::Util::Properties::read_properties_file("$BINDIR/$PROPERTIES_FILENAME");

		my $entities_filename = $properties->getProperty('entities.temp_filename');
		$entities_filename =~ s/%d/$ISO_TODAY_DATE/;

        my $entities_xml = SEG::Tamale::RestAPI::GetEntities::download_entities_xml($properties, 'Corporate');
		if ($DEBUG) {
			my $fh = FileHandle->new("$WORKDIR/entities_debug.xml", 'w')
				or die "Could not open file '$WORKDIR/entities_debug.xml' for writing: $!";
			print $fh $entities_xml;
		}

        my $corporate_entities = SEG::Tamale::RestAPI::GetEntities::get_corporate_entities_from_xml($entities_xml);

        my $adv_filter = "((relationship-type name equals \"Lead Analyst\") or (relationship-type name equals \"Secondary Analyst\"))";

        my $relationships_xml = SEG::Tamale::RestAPI::GetRelationships::_download_relationships_xml($properties, undef, $adv_filter);

        my $relationships = SEG::Tamale::RestAPI::GetRelationships::get_relationships_from_xml($relationships_xml);

        my %Coverage;
        $Coverage{$_->{'child-entity-short-name'}} = 1 foreach (@$relationships);
        $Coverage{$_->{'parent-entity-short-name'}} = 1 foreach (@$relationships);


        my $fh = ($out_filename eq '-' 
			? *STDOUT 
			: new FileHandle($out_filename, 'w'))
				|| die "Could not open filename '$out_filename' for writing: $!";

		$fh->autoflush(1);

		# header
		print $fh 'tamale_symbol', "\n";

		# data
		foreach my $entity (@$corporate_entities) {
			print $fh $entity->{'short-name'}, "\n"
				if ($Coverage{ $entity->{'short-name'} });
		}
		print $fh "TAMALE_DUMMY_SYMBOL", "\n"
	};
	if ($@) {
		cluck "$PROGNAME: $@";
		return 0;
	}

	return 1;
}
