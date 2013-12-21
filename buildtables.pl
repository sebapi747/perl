#!/usr/bin/perl -w
# 
# Rationale: script for populating a sqlite db of financial data from advfn
#
# Author: Sebastien Hitier
# Date: 2011
#
#
use strict;
my $market  = "nyse";
my $tsvfiledir  = "/cygdrive/d/finqtsv-$market";
my $tickerfile  = "../../d/tickers-$market.csv";
my $dbname   = "financialq.db";
my $dbdir       = "../../d/sqlitedb";
my @symbol = ("A", "C", "DDD", "AHC", "GAS");
# --------------------------------------------------------------------------------------------------------------------------
#
use File::Path;
use Text::CSV;
die "run popfinq.pl nyse first!" if (!-d $tsvfiledir);
mkpath($tsvfiledir) unless (-d $tsvfiledir);

# --------------------------------------------------------------------------------------------------------------------------
#
my @colnames=();
my %sectionname = ();
foreach my $suffix (0..15) {
for my $symbol (@symbol) {
	my $tsvfile = "$tsvfiledir/$symbol-$suffix.tsv";
	if (!-e $tsvfile) { print "file $tsvfile  does not exist\n" ; next;}
	if (!-s $tsvfile) { print "file $tsvfile  is empty\n" ; next;}
	open my $fh, "<", $tsvfile or die "$tsvfile: $!";
	my $csv = Text::CSV->new ({
	binary    => 1, # Allow special character. Always set this
	auto_diag => 1, # Report irregularities immediately
	sep_char => "\t",
	#allow_loose_quotes => 1,
	});

	my $s = "";
	while (my $row = $csv->getline ($fh)) {
		if ($row->[5].$row->[4].$row->[3].$row->[2].$row->[1] eq "" || $row->[0] eq "*" ) {
			$s = $row->[0];
			#print "title $row->[0]\n";
		} else {
			$sectionname{$row->[0]} = $s;
			push(@colnames, $row->[0]);
			#print "$row->[0]\n";
		}
	}
	close $fh;
}
}

use DBI;
mkpath($dbdir) unless (-d $dbdir);
print STDERR "drop table\n";
my $db = DBI->connect("dbi:SQLite:$dbdir/$dbname", "", "", {RaiseError => 1, AutoCommit => 1});
$db->do("DROP TABLE IF EXISTS financialq");
my $create_table = <<"END";
CREATE TABLE IF NOT EXISTS  fin_column_name (
	column_name TEXT PRIMARY KEY, 
	original_name TEXT, 
	section TEXT NULL
)
END
$db->do($create_table);
print STDERR "create table 1\n";

$create_table = "CREATE TABLE  financialq (\n\tticker TEXT(10),\n\tquarter INTEGER";
my %inserted_columns = ();

for my $colname (@colnames) {
	#
	# label column
	next if $colname eq "";
	my $label = $colname;
	$label =~	s/\%/pct/g;
	$label =~	s/\$/usd/g;
	$label =~	s/[,\(\)\.\/\-\*\s\&]/_/g;
	$label = lc $label;
	next if exists $inserted_columns{$label};
	$db->do("INSERT OR REPLACE INTO fin_column_name (column_name, original_name, section) values ('$label', '$colname', '$sectionname{$colname}');");
	my $labeltype = "REAL";
	$labeltype = "TEXT" if ($label =~ m/inventory_valuation_method|auditor|.*date.*|.*indicator|preliminary_full_context_ind/);
	$create_table .= ",\n\t\t$label $labeltype NULL"; 
	#$db->do("ALTER TABLE financialq ADD COLUMN $label $labeltype NULL") if not exists $inserted_columns{$label};
	$inserted_columns{$label} = $labeltype;
}
$create_table .=  ",\n\tPRIMARY KEY (ticker, quarter))";
print $create_table;
$db->do($create_table);

print STDERR "inserted columns\n";
$db->do("VACUUM");