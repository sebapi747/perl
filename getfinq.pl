#!/usr/bin/perl
my $outdir = "../../d/finqhtml";
my $input = "exchange.txt";

# let's rock
use strict;
use File::Path;
mkpath($outdir) unless (-d $outdir);
open(DAT, $input) || die("Could not open file!");
my @stocklist=<DAT>;
my @years=qw/0 5 10 15 20 25 30 35 40 45 50 55 60 65/;
close DAT;
my %request;
my @filelist;
my $cmd = "";

sub deletebadfile
{
	my $f = shift;
	my $s = -s $f;
	if (-e $f)
	{
		if ($s == 0)
		{
			print "deleting $f size $s\n";
			system "mv $f empty";
			next;
		}
		my $found = -1;
		my $found_lastline = 0;
		my @dates = ();
		open(FILEOUT, $f) || die("Could not open file $f!");
		my @htmlout=<FILEOUT>;
		close FILEOUT;
		foreach (@htmlout)
		{
			if (m/quarter end date/)
			{
				my $x = $_;
				while ($x =~ /(\d\d\d\d\/\d\d)/g)
				{
					push(@dates, $1);
				}
				$found = $#dates;
			}
			$found_lastline=1 if (m/leverage-to-industry/ or m/risk-based capital ratio/);
		}
		if ($found < 0)
		{
			print "deleting $f irrelevant file (size $s)\n";
			system "mv $f irrelevant";
		}
		elsif ($found >= 0 and $found_lastline == 0)
		{
			print "deleting $f corrupt file (size $s)\n";
			system "mv $f corrupt";
		}
		else
		{
			print "$f : ".join(" ", @dates)."\n";
		}
	}
}

use threads;
use Thread::Queue;
use File::Path; 

sub worker {
    my $tid = threads->tid;
    my( $Qwork, $Qresults ) = @_;
    while( my $work = $Qwork->dequeue ) {
	my $result;

	my $fullticker=$work;
	my $ticker=$fullticker;
	$ticker =~ s/.*://;
	for (my $i=0; $i<$#years+1; $i++)
	{
		my $f = "$outdir/$ticker-$i.html";
		deletebadfile($f);
		next if (-e $f); #skip if we already got a file there
		my $r ="\"http://asia.advfn.com/p.php?pid=financials\&btn=s_ok\&mode=quarterly_reports\&symbol=$fullticker\&s_ok=OK\&istart_date=$years[$i]\"";
	
		my $wgopt = ""; #"-t4 -T10"; # -t4 -T10
		my $cmd = "wget $wgopt $r -O $f" ;
		system $cmd;
		 
	}        ## Process $work to produce $result ##
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




