#!/usr/bin/perl

use Redis;
use Data::Dumper;
use JSON::XS;
use Getopt::Std;
use strict;

my %opts;
getopts('f:', \%opts);

if(!$opts{f}){
	print "Please provide name of state file to process using the -f option.\n";
	exit;
}

if($opts{f} !~ 'output/series'){
	print "\n\n------------------ STOP ------------------\n";
	print "This script is built to work with files that have been split using\n";
	print "the scripts that split the series files by state.\n";
	print "You can continue to run this script on the chosen file\n".$opts{f}." but the results \n";
	print "will be unpredictable.\nThis script nor its author can be held libale for\n";
	print "the outcome.\n\nProcced with caution.\n";
	print "Press any key to continue or CTRL-C to abort this script.\n";
	<STDIN>
}
my $redis = Redis->new;

#Check Redis for thisState key
#Redis should be cleared for every new state

my $keyCheck = $redis->get("thisState");

if($keyCheck){
	print "\n\n------------------ STOP ------------------\n";
	print "Redis Currently contains data for ".$keyCheck."\n";
	print "Press any key to Flush this data and being processing ".$opts{f}.".\n";
	print "Press CTRL-C to abort this script.\n";
	$redis->flushall();
    $redis->set("thisState" => $opts{f});
    <STDIN>
}

##Load mapping data into memory hash
my %mappingHash;
my @mappingFileList = ('en.area','en.footnote','en.industry','en.owner','en.size','en.state','en.type');
my @headerArray;
my $headerArray;
foreach my $mappingFile(@mappingFileList){
	#Load these with cat as they are small
	my @mappingArray = qx(cat ../$mappingFile);
	my $lineCounter = 0;

	my @dataArray;
	my $dataArray;
	my($mappingSet, $mappingName) = split(/\./,$mappingFile);

	foreach my $mappingLine(@mappingArray){
		chomp($mappingLine);
		if($lineCounter == 0){
			@headerArray = split(/\s+/, $mappingLine);
			#Clean @headerArray values
			while (my ($i, $el) = each @headerArray) {
            	$headerArray[$i] =~ s/^\s+|\s+$//g;
            }

			#print Dumper(@headerArray);
		}else{
			@dataArray = split(/\t+/, $mappingLine);
			#clean data array and assign values
			#we will split the file name and use the mapping
			#definition as part of the hash keys 
			while (my ($i, $el) = each @dataArray) {
            	$dataArray[$i] =~ s/^\s+|\s+$//g;
            	$mappingHash{$mappingName."_code"}{$dataArray[0]}{$headerArray[$i]} = $dataArray[$i]; 

            }
		}
		++$lineCounter;
	}
}
#print Dumper(\%mappingHash);
##END Load mapping data into memory hash

#Now we read the series file one line at a time since it is such a large file.
#More Array trickery to get the header columns lined up with the data and the mappings



open(FH, '<', $opts{f}) or die $!;
my $seriesCounter = 0;
my @dataHeader;
my $dataHeader;
my @dataArray;
my $dataArray;
while(<FH>){
	chomp($_);
	if($seriesCounter == 0){
		#Clean @headerArray values
		@dataHeader = split(/\t/, $_);
		while (my ($i, $el) = each @dataHeader) {
        	$dataHeader[$i] =~ s/^\s+|\s+$//g;
        }
	}else{
		@dataArray = split(/\t/, $_);
			#clean data array and assign values
			#we will split the file name and use the mapping
			#definition as part of the hash keys 
			my %sendHash;
			while (my ($i, $el) = each @dataArray) {
            	$dataArray[$i] =~ s/^\s+|\s+$//g if $headerArray[$i] ne "footnote_codes";
            	#$sendHash{$dataHeader[$i]}
            	if($dataHeader[$i] =~ '_code'){
            	$sendHash{$dataHeader[$i]} = $mappingHash{$dataHeader[$i]}{$dataArray[$i]};
            	}else{
            		$sendHash{$dataHeader[$i]} = $dataArray[$i];
            	}
            }
   			#now it is all tied together, we will store the data in Redis with the session_id as the key
            #print Dumper(\%sendHash);
            #delete $sendHash{begin_year};
            #delete $sendHash{begin_period};
            #delete $sendHash{end_year};
            #delete $sendHash{end_period};
            $redis->set($dataArray[0] => encode_json(\%sendHash));
	}
	++$seriesCounter;
}