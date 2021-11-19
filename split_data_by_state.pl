#!/usr/bin/perl

use Data::Dumper;
use strict;

#Load Hash of States
my @stateArray = qx(cat ../en.state);
my %stateHash;
my $workingState;
my $useHeader;

foreach my $state(@stateArray){
	chomp($state);
	next if $state =~ 'state_code';
	print $state."\n";
	my($stateNum, $stateName, undef) = split(/\t+/, $state);

	$stateHash{$stateNum} = $stateName;
}
print Dumper(\%stateHash);

open(FH, '<', "../en.data.1.AllData") or die $!;
while(<FH>){
	chomp($_);
	if($_ =~ 'series_id'){
		$useHeader = $_;
		next;
	}else{
		my @lineArray = split(/\t+/, $_);
		my $lineArray;
		my @idArray = split(//, $lineArray[0]);
		my $idArray;
		my $stateCode = $idArray[3]."".$idArray[4];
		my $useState = $stateHash{$stateCode};
		#There are two other data sets here besides just states. there are
		#national averages and micro averages. Those date lines are labeled 
		#differently so we mark it with US which denotes this kind of data.
		if(!$useState){
			#print $_."\n";
			#print Dumper(@lineArray);
			$useState = "US";
		
		}
		#Replace file name spaces with _ for wider system compatibility
		$useState =~ s/\s+/_/g;
		if($workingState ne $useState){
			#this file extension is .mycopy so we do not confuse it with the
			#original data
			my $filename = "./output/data/en.data.".$useState.".mycopy";
			if (-e $filename) {
				print "!!!!!!!!!! STOP !!!!!!!!!!!!\n".$filename." exists. \nPress enter to overwrite or Ctrl-C to exit script";
				<STDIN>
			}
			print $filename."\n";
			open(SH, '>', $filename) or warn $!;
			print SH $useHeader."\n";
			$workingState = $useState;
		}
		print SH $_."\n";
	}
}
close(SH);