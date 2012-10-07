#!/usr/bin/perl -w
# 
# Rationale: script for populating a sqlite db of stock quotes from csv files produced by yahoo or google
#
# Author: Sebastien Hitier
# Date: 2010 - 2011
#
#
use strict;
use File::Copy;
use threads;
use Thread::Queue;
my $src	= 'y:/Rsource/kalman_2012.r';
my @stocklist = qw/FXI RSX ECH THD TUR EIS EPI EWA EWC EWD EWG EWH EWI EWJ EWK EWL EWM EWN EWO EWP EWP EWQ EWS EWU EWW EWT EWY EWZ EZA IWD IWN MDY EEM EFA IJR XLE XLB XLV XLF XLI XLK XLU XLP
/;
sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
        my $result;

	my $ticker = $work;
	my $dest	= 'y:/Rsource/kalman_2012_'.$ticker.'.r';
	copy($src,$dest) or die "Copy failed: $!";
	print STDOUT $dest;
	open(FILE,'>>'.$dest) || die ("file open error");
	print FILE 'regressand.multiasset.kalman(c("'.$ticker.'"))'."\n";
	close(FILE);
	my $cmd	= ".\\Rcall.bat --vanilla $dest";
	system($cmd );

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


