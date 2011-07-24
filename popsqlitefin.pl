#!/usr/bin/perl -w
# 
# Rationale: script for populating a sqlite db of financial data from advfn
#
# Author: Sebastien Hitier
# Date: 2011
#
#
use strict;
my $csvfiledir  = "../finhtml";
my $tickerfile  = "tickers.txt";
my $dbname   = "financial.db";
my $dbdir       = "../sqlitedb";

# --------------------------------------------------------------------------------------------------------------------------
#
use File::Path;
use DBI;
die "run getfin.pl first!" if (!-d $csvfiledir);
mkpath($dbdir) unless (-d $dbdir);
my $db = DBI->connect("dbi:SQLite:$dbdir/$dbname", "", "", {RaiseError => 1, AutoCommit => 1});
my $create_table = <<"END";
CREATE TABLE IF NOT EXISTS financial (
	ticker TEXT(10), 
	year INTEGER, 
	PRIMARY KEY (ticker, year)
)
END
$db->do($create_table);
$create_table = <<"END";
CREATE TABLE IF NOT EXISTS fin_column_name (
	column_name TEXT PRIMARY KEY, 
	original_name TEXT, 
	section TEXT NULL
)
END
$db->do($create_table);
my %inserted_columns = ();
sub extractData
{
	my $symbol = shift;
	my $html_string = shift;
	use HTML::TableExtract;
	my $te = HTML::TableExtract->new( depth => 3 , count => 4);
	$te->parse($html_string);
	# Examine all matching tables
	foreach my $ts ($te->tables) {
		my $l = 0;
		my $sectionName = "";
		my @years = ();
		my @label_list = ();
		my %year_data = ();
		foreach my $row ($ts->rows) {
			my @line = @$row;
			my $lineSize = $#line + 1;
			#
			# no error check
			if ($lineSize == 0) {
				print "problem with line for ticker $symbol\n";
				next;
			}
			#
			# section names
			my $orig_label= shift @line;
			if ($lineSize == 1 || $orig_label eq '*' || $orig_label =~ m/[A-Z]*/ && not $orig_label =~ /[a-z]/) {
				print STDERR "found section $orig_label\n";
				$sectionName = $orig_label if ($orig_label =~ m/[A-Z]/);
				next;
			}
			#
			# label column
			my $label = $orig_label;
			$label =~	s/\%/pct/g;
			$label =~	s/\$/usd/g;
			$label =~	s/[,\(\)\.\/\-\*\s\&]/_/g;
			$label = $label."_$l";
			$db->do("INSERT OR REPLACE INTO fin_column_name (column_name, original_name, section) values ('$label', '$orig_label', '$sectionName');");
			my $labeltype = "REAL";
			$labeltype = "TEXT" if ($sectionName eq "INDICATORS" || $label =~ m/inventory_valuation_method|auditor/);
			#$db->do("ALTER TABLE financial ADD COLUMN $label $labeltype NULL") if not exists $inserted_columns{$label};
			$inserted_columns{$label} = $labeltype;
			#
			# year end dates
			if ($orig_label =~ m/year end date/) {
				for (@line) {
					my $y = $_;
					$y =~ s/\/.*//;
					push(@years, $y);
				}
				print "found @years ";
			}

			for (my $i=0; $i<$#line+1; $i++) {
				next if (not defined $line[$i]);
				$line[$i] =~ s/[,']//g;
				$line[$i] = "NULL" if ($line[$i] eq "");
				$year_data{$label.'____'.$years[$i]} = $line[$i]; 
				$year_data{$label.'____'.$years[$i]} = "'$line[$i]'" if ($labeltype eq "TEXT");
			}
			$l ++;
		}
		# aggregate by year the list of column names and values to be inserted
		my %requests_colnames = ();
		my %requests_value = ();
		foreach (sort keys %year_data)
		{
			print STDERR $_;
			m/(.*)____(\d+)/;
			my $y       = $2;
			my $colname = $1;
			my $data    = $year_data{$_};
			$requests_colnames{$y} .= ','.$colname;
			$requests_value{$y}    .= ','.$data;
		}
		# for each year, do one command
		$db->do("BEGIN");
		my $insert_count = 0;
		foreach (sort keys %requests_colnames)
		{
			my $y       = $_;
			my $insert_cmd = "INSERT OR REPLACE INTO financial (ticker, year".$requests_colnames{$y}.")";
			$insert_cmd .= " values ('$symbol', $y".$requests_value{$y}.");";   
			print STDERR "$insert_cmd\n";
			$db->do($insert_cmd);
			$insert_count++;
		}
		$db->do("COMMIT");
		print "commited $insert_count inserts for $symbol\n";
	}
}


open(DAT, $tickerfile) || die("Could not open file $tickerfile !");
my @stocklist=<DAT>;
close DAT;
foreach my $symbol (@stocklist) {
	chomp $symbol;
	my @filesuffix = ("-0", "-1", "-2", "-3");
	foreach my $suffix (@filesuffix) {
		my $htmlin = "$csvfiledir/$symbol$suffix.html";
		print "extracting $htmlin...";
		if (!-e $htmlin) { print "file $htmlin  does not exist\n" ; next;}
		if (!-s $htmlin) { print "file $htmlin  is empty\n" ; next;}
		open(MYINFILE, "<$htmlin");
		my $html_string= join("",<MYINFILE>);
		close(MYINFILE);
		extractData($symbol, $html_string);
	}
}

