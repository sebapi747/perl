#!/usr/bin/perl
use strict;
use threads;
use Thread::Queue;
my @stocklist=(); #qw/^VIX USO GLD EFA IYR TLT DBA DBB LQD FXE FXY SPY/;
open(DAT, "tickers.txt") || die("Could not open file!");
@stocklist=<DAT>;
close DAT;

sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
        my $result;

	my $fileout = "googlehtml/$work.csv";
	my $si = -s $fileout;
	if ($si == 0)
	{
		system "wget 'http://www.google.com/finance?client=ob&q=$work' -O $fileout" ;
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

