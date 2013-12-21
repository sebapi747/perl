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
my $tsvfiledir  = "/cygdrive/d/finqtsv-$market";
my $tickerfile  = "../../d/tickers-$market.csv";
my $dbname   = "financialq.db";
my $dbdir       = "../../d/sqlitedb";
# --------------------------------------------------------------------------------------------------------------------------
#
use File::Path;
use Text::CSV;
die "run popfinq.pl nyse first!" if (!-d $tsvfiledir);
die "run buildtables.pl first!" if (!-d $dbdir);


# --------------------------------------------------------------------------------------------------------------------------
#
open my $fh, "<", $tickerfile or die "$tickerfile: $!";
my @symbol=();
my $csv = Text::CSV->new ({
binary    => 1, # Allow special character. Always set this
auto_diag => 1, # Report irregularities immediately
});
while (my $row = $csv->getline ($fh)) {
	push(@symbol, $row->[0]);
}
close $fh;
print STDERR "read $#symbol symbols\n";

# --------------------------------------------------------------------------------------------------------------------------
#
my @colnames=();
my %sectionname = ();

use DBI;
mkpath($dbdir) unless (-d $dbdir);
my $db = DBI->connect("dbi:SQLite:$dbdir/$dbname", "", "", {RaiseError => 1, AutoCommit => 1});

for my $symbol (@symbol) {
	foreach my $suffix (0..15) {
		my $tsvfile = "$tsvfiledir/$symbol-$suffix.tsv";
		if (!-e $tsvfile) { print "file $tsvfile  does not exist\n" ; next;}
		if (!-s $tsvfile) { print "file $tsvfile  is empty\n" ; next;}
		open my $fh, "<", $tsvfile or die "$tsvfile: $!";
		#my $csv = Text::CSV->new ({
		#binary    => 1, # Allow special character. Always set this
		#auto_diag => 1, # Report irregularities immediately
		#sep_char => "\t",
		#allow_loose_quotes => 1,
		#});

		my @transposed = ();
		my @rows = ();
		while(<$fh>) {
			chomp;
			s/\t\t/\tNULL\t/g;
			s/\t\t/\tNULL\t/g;
			s/\t\t/\tNULL\t/g;
			push @rows, [split /\t/];
		}
		close $fh;

		for my $column (2 .. 5) { # $#{$row}
			my $d = ${$rows[1]}[$column];
			next if $d eq "NULL";
			$d =~	s/\///;
			$d .= "25";
			my @colnames = ('"ticker"', '"quarter"');
			my @values = ('"'.$symbol.'"', $d);
			for my $row (@rows) {
				next if ($row->[$column] eq "NULL");
				my $label = $row->[0];
				$label =~	s/\%/pct/g;
				$label =~	s/\$/usd/g;
				$label =~	s/[,\(\)\.\/\-\*\s\&]/_/g;
				$label = lc $label;
				push (@colnames, '"'.$label.'"');
				my $labeltype = "REAL";
				$labeltype = "TEXT" if ($label =~ m/inventory_valuation_method|auditor|.*date.*|.*indicator|preliminary_full_context_ind/);
				my $val = $row->[$column];
				if ($labeltype eq "TEXT") {
					$val = '"'.$val.'"';
				} else {
					$val =~ s/,//g;
				}
				push (@values, $val);
				my $colnames = join(",\n", @colnames);
				my $values = join(",\n", @values);
			}
			my $colnames = join(",\n", @colnames);
			my $values = join(",\n", @values);
			my $insert_cmd = "INSERT OR REPLACE INTO financialq($colnames)\nvalues ($values)";
			$db->do($insert_cmd);
			print STDERR "inserted $symbol quarter $d $#colnames cols found in $suffix\n"
		}
	}
}
$db->do("VACUUM");
