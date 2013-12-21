#!/usr/bin/perl -w
# 
# Rationale: script for populating a sqlite db of financial data from advfn
#
# Author: Sebastien Hitier
# Date: 2011
#
#
use strict;
my $market  = shift;
my $csvfiledir  = "/cygdrive/d/finqhtml-$market";
my $tsvfiledir  = "/cygdrive/d/finqtsv-$market";
my $tickerfile  = "../../d/tickers-$market.csv";
print STDERR  "bonjour\n";
# --------------------------------------------------------------------------------------------------------------------------
#
use File::Path;
use Text::CSV;
die "run getfin.pl first!" if (!-d $csvfiledir);
mkpath($tsvfiledir) unless (-d $tsvfiledir);

# --------------------------------------------------------------------------------------------------------------------------
#
open my $fh, "<", $tickerfile or die "$tickerfile: $!";
my @stocklist=();
my $csv = Text::CSV->new ({
binary    => 1, # Allow special character. Always set this
auto_diag => 1, # Report irregularities immediately
});
while (my $row = $csv->getline ($fh)) {
	push(@stocklist, $row->[0]);
	print "$row->[0]\n";
}
close $fh;


# --------------------------------------------------------------------------------------------------------------------------
#
use threads;
use Thread::Queue;
use File::Path; 

sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
	my $result;
	my $symbol=$work;
	chomp $symbol;
	system "./popfinqone.pl $market $symbol";
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


