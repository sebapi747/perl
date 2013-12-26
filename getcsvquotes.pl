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
use LWP::Simple;
my $work="SPY";
my $content = LWP::Simple::get("http://ichart.finance.yahoo.com/table.csv?s=$work");
print $content;
my $csvfiledir ="../../d/quotes-etf";
my $tickerfile = "etf.txt"; #("regressors.txt", "stockfutures.txt", "etf.txt");

# --------------------------------------------------------------------------------------------------------------------------
#

# build list of tickers to take
open(DAT, $tickerfile) || die("Could not open file $tickerfile !");
my @stocklist=<DAT>;
close DAT;

mkpath($csvfiledir) unless (-d $csvfiledir);
sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
        my $result;
	my $outfile = "$csvfiledir/$work.csv";
	if (-e $outfile && -s $outfile>0)
	{
		$result = "$tid : skipped $work\n";
	}
	else
	{
		my $content = LWP::Simple::get("http://ichart.finance.yahoo.com/table.csv?s=$work");
		open(OUT, ">$outfile");
		print OUT $content;
		close(OUT);
		my $si = -s $outfile;
		$result = "$tid : fetched $work. Size=$si\n";
	}
        ## Process $work to produce $result ##
        $Qresults->enqueue( $result );
    }
    $Qresults->enqueue( undef ); ## Signal this thread is finished
}

our $THREADS = 7;
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

