#!/usr/bin/perl
# 
# Rationale: script for  
# - running R script
#
# Author: Sebastien Hitier
# Date: 2010 - 2011
#
# script is using a thread pool to parallel process the retrieval
#
use strict;
my $csvfiledir = "../csv";
my $tickerfile = "shortlist.txt";
my $dbname  = "../sqlitedb/stocks.db";
my $Rsource  = "../Rsource/master_v5.r";
my $outputDir = "../";
my $Rcmd     = '"c:/Program Files/R/R-2.13.1/bin/x64/Rcmd.exe" BATCH ';
# --------------------------------------------------------------------------------------------------------------------------
#
use DBI;
use threads;
use Thread::Queue;
use File::Path;
use Time::localtime; 
my $tm = localtime; 
my @stocklist=qw/SPY XLB IYR MDY EEM EFA IJR XLE XLF XLI/;

my $month = ($tm->mon)+1;
my $outputDir = $outputDir.sprintf("%4d%02d%02d",$tm->year+1900,$tm->mon+1, $tm->mday);
mkpath($outputDir) unless (-d $outputDir);
sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
        my $result;

	my $command = <<"END";
	source('$Rsource')
	regressand <- c('$work')
	outputdir  <- '$outputDir'
	regressand.multiasset.kalman()
END
	open(BATCHFILE, "> $work.r");
	print BATCHFILE $command;
	close BATCHFILE;
	$command = $Rcmd." $work.r";
	#open(BATCHFILE, "> $work.bat");
	#print BATCHFILE $command;
	#close BATCHFILE;
	print STDERR 'cmd.exe /c '.$Rcmd." $work.r"."\n";
	system 'cmd.exe /c '.$Rcmd." $work.r";
        $Qresults->enqueue( $result );
    }
    $Qresults->enqueue( undef ); ## Signal this thread is finished
}

our $THREADS = 1;
my $Qwork = new Thread::Queue;
my $Qresults = new Thread::Queue;

## Create the pool of workers
my @pool = map{
    threads->create( \&worker, $Qwork, $Qresults )
} 1 .. $THREADS;

## Get the work items (from somewhere)
## and queue them up for the workers
for my $workItem  (@stocklist) {
    chomp $workItem;
    $Qwork->enqueue( $workItem );
}

## Tell the workers there are no more work items
$Qwork->enqueue( (undef) x $THREADS );

## Process the results as they become available
## until all the workers say they are finished.
for ( 1 .. $THREADS ) {
    while( my $result = $Qresults->dequeue ) {

        ## Do something with the result ##
        print $result;
    }
}

## Clean up the threads
$_->join for @pool;

