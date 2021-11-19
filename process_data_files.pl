#!/usr/bin/perl

use Redis;
use Search::Elasticsearch;
use Data::Dumper;
use Getopt::Std;
use JSON::XS;
use strict;

my %opts;
getopts('hdiu:p:a:f:e:b:', \%opts);

if($opts{h}){
	printHelp();
	exit;
}

if(!$opts{a} || !$opts{u} || !$opts{p} || $opts{a} !~ ':'){
	print "\nMissing Elasticsearch information.\n";
	print "The script wll run but no ingestion will be done.\n";
	print "ex. process_data_files.pl -i -uesuser -pespass -h127.0.0.1:9200\n";
	print "\n Press Any Key to Continue or CTRL-C to abort\n";
	undef $opts{i};
	<STDIN>
}
if(!$opts{f}){
	print "\n-f option not found. This is required so the script knows\n";
	print "which data file to process.\n";
	exit;
}
if(!$opts{e}){
	print "\n-e option not found. This sets the Elasticsearch index name for ingesting theis data.\n";
	print "Check command and try again\n\n";
	printHelp();
	exit;
}
if(!$opts{i}){
	print "\n\n -i script option not provided.\n";
	print "No Data will Be Ingesetd. Only what would have been done will be shown.\n";
	print "Press and key to continue or CTRL-C to abort script.";
	<STDIN>
}
#If no batch size is provided default to 1000
if(!$opts{b}){
	$opts{b} = 1000;
}
my $redis = Redis->new;
my $es = Search::Elasticsearch->new( nodes => $opts{u}.":".$opts{p}.'@'.$opts{a}) if $opts{i};


#There is no refrences to load from a mapping file for dates.
#ohter info texts for other data sets explained how this is handled.
#This hash is built from that data.
my $monthHash = { M01 => "01", M02 => "02", M03 => "03", M04 => "04", M05 => "05", M06 => "06", M07 => "07", M08 => "08", M09 => "09", M10 => "10", M11 => "11", M12 => "12", M13 => "12", Q1 => "03", Q2 => '06', Q3 => "06", Q4 => "12",
Q01 => "03", Q02 => '06', Q03 => "06", Q04 => "12", Q05 => "12", A01 => "12" };

open(FH, '<', $opts{f}) or die $!;
my $lineCounter = 0;
my @headerArray;
my $headerArray;
my @lineArray;
my $lineArray;
my $finalDocument;
my @bulkArray;
my $bulkCounter;
my $ingestTiming;
my $printCounter;
while(<FH>){
	chomp($_);
	#print $_."\n";
	if($lineCounter == 0){
		@headerArray = split(/\s+/, $_);
		#cleanup headers
		while (my ($i, $el) = each @headerArray) {
			$headerArray[$i] =~ s/^\s+|\s+$//g;
		}
		print Dumper(@headerArray) if $opts{d};
			++$lineCounter;
	}else{
			++$lineCounter;
		@lineArray = split(/\s+/, $_);
		while (my ($i, $el) = each @lineArray) {
			$lineArray[$i] =~ s/^\s+|\s+$//g;
			if($lineArray[$i] eq "-"){
				$lineArray[$i] = "0";
			}
		}
		if($lineArray[3] <= 0){
			#lines that nave no numerical value
			#are not part of this data set as noted
			#in the foot notes.
			#dropping them instead of making Elasticsearch
			#deal with a bunch of 0's.
			next;
		}

		++$printCounter;
		if($printCounter > 100000 ){
		print $lineCounter." lines processed.\n";
		$printCounter = 0;
		}
		#print $lineArray[0]." ";
		my $doc = $redis->get($lineArray[0]);
		#next if !$doc;
		my $sendDocument;
		#print encode_json($doc)." \n";
		$sendDocument = decode_json($doc);
		$sendDocument->{year} = $lineArray[1];
		$sendDocument->{period} = $monthHash->{$lineArray[2]};
		$sendDocument->{value} = $lineArray[3];
		$sendDocument->{calculated_date} = $lineArray[1]."/".$monthHash->{$lineArray[2]}."/01";
		if(!$monthHash->{$lineArray[2]}){
			print Dumper($sendDocument);
			exit;
		}

		my $action = {index =>{_index=>$opts{e}, _id => $lineArray[0]."".$lineArray[1]."".$lineArray[2]}};
        push(@bulkArray, $action);
        push(@bulkArray,$sendDocument);
  		if($bulkCounter >= $opts{b}){
  			$ingestTiming = time;
         	print "Ingest Time\n" if $opts{d};
   			print "Single Document Sample: \n".Dumper($sendDocument)."\n" if $opts{d};

           my $bulkReturn = BulkIngestData(@bulkArray) if $opts{i};
           @bulkArray = ();
           $bulkCounter = 0;
           my $totalIngestTime = time - $ingestTiming;
           print "Time To Ingest:".$totalIngestTime."\n" if $opts{d};
        }
        ++$bulkCounter;
	}

}

if(scalar(@bulkArray) >= 1){
	print "Last push. ".scalar(@bulkArray)." items left.\n" if $opts{d};
	my $bulkReturn = BulkIngestData(@bulkArray);

}

sub BulkIngestData(){
  my @IngestData = @_;
  #print "Ingesting ".scalar(@IngestData)." documents\n" if $opts{d};
  my $res = $es->bulk( body => \@IngestData);
    if ( $res->{errors} ) {
      warn "Bulk index had issues: " . encode_json( $res );
    }

}

sub printHelp(){
	print <<EOF;

	process_data_files.pl Help file

	Options:
	-h This help text
	-d Print debugging output
	-a Address of Elasticsearch host
	-u Elasticsearch user that has write access to the index specified with -e
	-p Password for Elasticsearch user
	-f State data file to process.
	-e Elasticsearch index for writing
	-b Elasticsearch batch size. defaults to 1000
	-i Actually do the ingestion. Without this flag -d should be used to see the debug output.
EOF
}