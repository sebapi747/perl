#!/usr/bin/perl -w
# 
# Rationale: script for populating a sqlite db of stock quotes from csv files produced by yahoo or google
#
# Author: Sebastien Hitier
# Date: 2010 - 2011
#
#
my $csvfiledir = "csv";
my $tickerfile = "tickers.txt";
my $dbname     = "stocks.db";
my $dbdir       = "sqlitedb";
use File::Path;
use DBI;
use strict;
mkpath($dbdir) unless (-d $dbdir);
my $db = DBI->connect("dbi:SQLite:$dbdir/$dbname", "", "", {RaiseError => 1, AutoCommit => 1});
my $create_table = <<"END";
CREATE TABLE IF NOT EXISTS hist_quotes (
	ticker TEXT(10), 
	date INT, 
	open REAL NULL, 
	high REAL NULL, 
	low REAL NULL, 
	close REAL, 
	volume REAL NULL, 
	adj_close REAL NULL, 
	PRIMARY KEY (ticker, date)
);
END
$db->do($create_table);

open(DAT, $tickerfile) || die("Could not open file $tickerfile !");
my @stocklist=<DAT>;
close DAT;

foreach my $symbol (@stocklist) {
	chomp $symbol;
	print "inserting $symbol... ";
	my $file = "$csvfiledir/$symbol.csv";
	if (-s $file == 0) {
		print "empty or non existant file\n";
		next;
	}
	open(DAT, $file) || die("Could not open $file !");
	my @lines=<DAT>;
	close DAT;
	#$db->do("INSERT OR REPLACE INTO ticker (ticker) VALUES ('$symbol')");
	#$db->do("delete from hist_quotes where ticker='$symbol'");
	$db->do("BEGIN");
	my $lin=0;
	my $colnames;
	foreach (@lines) 
	{
		chomp;
		if ($lin==0) 
		{
			s/^.*Date,/Date,/;
			s/ /_/;
			tr/[A-Z]/[a-z]/;
			$colnames= $_;
			#print $colnames.'\n';
			$lin++;
			next;
		}
		my @row = split(',', $_);
		my $d = shift @row;
		my $row = join(',', @row);
		if ($d =~ m/(\d\d)-\D\D\D-(\d\d)/)
		{
			my $da = $1;
			my $y = 2000+ $2;
			$y -= 100 if ($y >2060);

			$d =~ s/\d\d-Jan-\d\d/-01-/;
			$d =~ s/\d\d-Feb-\d\d/-02-/;
			$d =~ s/\d\d-Mar-\d\d/-03-/;
			$d =~ s/\d\d-Apr-\d\d/-04-/;
			$d =~ s/\d\d-May-\d\d/-05-/;
			$d =~ s/\d\d-Jun-\d\d/-06-/;
			$d =~ s/\d\d-Jul-\d\d/-07-/;
			$d =~ s/\d\d-Aug-\d\d/-08-/;
			$d =~ s/\d\d-Sep-\d\d/-09-/;
			$d =~ s/\d\d-Nov-\d\d/-11-/;
			$d =~ s/\d\d-Dec-\d\d/-12-/;
			$d = $y.$d.$da;
		}
		my $insert_cmd = <<"END";
INSERT OR REPLACE INTO hist_quotes 
	(ticker, $colnames) 
VALUES 
	('$symbol', julianday('$d')-julianday('1899-12-30'), $row);
END
		$db->do($insert_cmd);
		$lin++;
	}
	$db->do("COMMIT");
	print "inserted $lin lines\n";
}


