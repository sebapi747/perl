#!/usr/bin/perl
open(DAT, "exchange.txt") || die("Could not open file!");
my @stocklist=<DAT>;
my @years=qw/0 5 10 15/;
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
			if (m/year end date/)
			{
				my $x = $_;
				while ($x =~ /(\d\d\d\d\/\d\d)/g)
				{
					push(@dates, $1);
				}
				$found = $#dates;
				break;
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

foreach (reverse sort @stocklist) #reverse sort
{
	chomp;
	my $fullticker=$_;
	my $ticker=$fullticker;
	$ticker =~ s/.*://;
	for (my $i=0; $i<$#years+1; $i++)
	{
		my $f = "finhtml/$ticker-$i.html";
		deletebadfile($f);

		my $r ="\"http://asia.advfn.com/p.php?pid=financials\&btn=s_ok\&mode=annual_reports\&symbol=$fullticker\&s_ok=OK\&start_date=$years[$i]\"";
		#my $r = "\"http://www.google.com/finance?client=ob\&q=$_\"";
		$request{$f} = $r;
	}
}

sub runcommand
{
	print "retrieving: ".join(" ", @filelist)."\n";
	system $cmd." wait"; 
	foreach (@filelist)
	{
		deletebadfile($_);
	}
	@filelist = ();
	$cmd = "";
}

my $count = 0;
foreach (sort keys %request) #reverse sort
{
	chomp;
	push(@filelist, $_);
	my $r = $request{$_};
	my $wgopt = "-t4 -T10"; # -t4 -T10
	$cmd .= "wget -q $wgopt $r -O $_ &" ;
	$count++;
	next if ($count % 20 != 0);
	runcommand;
}
runcommand;


