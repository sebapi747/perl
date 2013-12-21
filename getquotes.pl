#!/usr/bin/perl
# let's rock
use strict;
use threads;
use Thread::Queue;
use File::Path;
use Text::CSV;
my $exchange = shift;
my $nbthread = shift;

my $outdir = "../../d/quotes-$exchange";
my $input = "../../d/tickers-$exchange.csv";
mkpath($outdir) unless (-d $outdir);

open my $fh, "<", $input or die "$input: $!";
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
#@stocklist=("ACOR");


sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
	my $result;
	my $ticker=$work;
	chomp $ticker;
	my $f = "$outdir/$ticker.csv";
	next if (-e $f); #skip if we already got a file there
	my $r ="\"http://ichart.finance.yahoo.com/table.csv?s=$ticker\"";
	my $wgopt = ""; #"-t4 -T10"; # -t4 -T10
	my $cmd = "wget $wgopt $r -O $f" ;
	system $cmd;
        $Qresults->enqueue( $result );
    }
    $Qresults->enqueue( undef ); ## Signal this thread is finished
}

our $THREADS = $nbthread;
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




