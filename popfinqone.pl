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
my $symbol = shift;
my $csvfiledir  = "/cygdrive/d/finqhtml-$market";
my $tsvfiledir  = "/cygdrive/d/finqtsv-$market";
my $tickerfile  = "../../d/tickers-$market.csv";
# --------------------------------------------------------------------------------------------------------------------------
#

# --------------------------------------------------------------------------------------------------------------------------
#
sub extractData
{
	my $depth = shift;
	my $count = shift;
	my $symbol = shift;
	my $html_string = shift;
	my $fileout = shift;
	use HTML::TableExtract;
	my $te = HTML::TableExtract->new( depth => $depth , count => $count);
	$te->parse($html_string);
	# Examine all matching tables
	open(FILEOUT, "> $fileout") || die("Could not open file $fileout!");
	foreach my $ts ($te->tables) {
		foreach my $row ($ts->rows) {
			my @line = @$row;
			next if ( $#line<5);
			for my $el (@line) {
				print FILEOUT "$el\t";
			}
			print FILEOUT "\n";
		}
	}
	close FILEOUT;
}

# --------------------------------------------------------------------------------------------------------------------------
#
foreach my $suffix (0..15) {
	my $htmlin = "$csvfiledir/$symbol-$suffix.html";
	print STDERR "extracting $htmlin...";
	if (!-e $htmlin) { print "file $htmlin  does not exist\n" ; next;}
	if (!-s $htmlin) { print "file $htmlin  is empty\n" ; next;}
	open(MYINFILE, "<$htmlin");
	my $html_string= join("",<MYINFILE>);
	close(MYINFILE);
	my $fileout = $htmlin;
	$fileout =~ s/html/tsv/g;
	extractData(3, 0, $symbol, $html_string, $fileout);
	print STDERR "output in $fileout";
}        ## Process $work to produce $result ##


