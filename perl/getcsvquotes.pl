#!/usr/bin/perl
# 
# Rationale: script for retrieving stocks data in csv form from yahoo or google
#
# Author: Sebastien Hitier
# Date: 2010 - 2011
#
# script is using a thread pool to parallel process the retrieval
#
use strict;
use threads;
use Thread::Queue;
use File::Path;
use Time::localtime; 

my $csvfiledir = "csv";
my $tickerfile = "tickers.txt";
my $tm = localtime; 

open(DAT, $tickerfile) || die("Could not open file $tickerfile !");
my @stocklist=<DAT>;
close DAT;
mkpath($csvfiledir) unless (-d $csvfiledir);
my $month = ($tm->mon)+1;
my $datestring = sprintf("d=%2d&e=%2d&f=%4d",$tm->mon, $tm->mday,$tm->year+1900);
sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
        my $result;

	my $outfile = "$csvfiledir/$work.csv";
	my $si = -s $outfile;
	if ($si == 0)
	{
		system "wget 'http://ichart.finance.yahoo.com/table.csv?s=$work&a=00&b=29&c=1993&$datestring&g=d&ignore=.csv' -O $outfile" ;
		$si = -s $outfile;
		if ($si == 0)
		{
			system "wget 'http://www.google.com/finance/historical?q=$work&output=csv' -O $outfile";
		}
		$result = "$tid : fetched $work\n";
	}
	else
	{
		$result = "$tid : skipped $work\n";
	}
        ## Process $work to produce $result ##
        $Qresults->enqueue( $result );
    }
    $Qresults->enqueue( undef ); ## Signal this thread is finished
}

our $THREADS = 8;
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

