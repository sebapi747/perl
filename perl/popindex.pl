#!/usr/bin/perl -w
# 
# Rationale: script for populating a sqlite db of stock quotes from csv files produced by yahoo or google
#
# Author: Sebastien Hitier
# Date: 2010 - 2011
#
# create a table with a join of all the relevant regressors
#
my $dbname     = "stocks.db";
use DBI;
use strict;
my $db = DBI->connect("dbi:SQLite:$dbname", "", "", {RaiseError => 1, AutoCommit => 1});

# XLV XLU XLR XLI XLB XLF GLD EEM FXY FXE 
$db->do("drop table index_adj_close;");
my @indexlist = qw/SPY USO IYR GLD EFA DBB DBA EEM FXY FXE XLV XLU XLI XLF XLB TLT/;
my $create_table = "CREATE TABLE index_adj_close ( date INT PRIMARY KEY ";
foreach (@indexlist) 
{
	$create_table .= ", $_ REAL NULL\n";
}
$create_table .= "); delete from index_adj_close;";
$db->do($create_table);
my $il = join(',', @indexlist);
my $insert_stmt = "INSERT OR REPLACE INTO index_adj_close (date, $il) select $indexlist[0].date";
foreach (@indexlist) 
{
	$insert_stmt .= ", $_.adj_close ";
}
$insert_stmt .= "from ";
foreach (@indexlist) 
{
	$insert_stmt .= "hist_quotes $_, ";
}
$insert_stmt =~ s/, $//;
$insert_stmt .= " where ";
foreach (@indexlist) 
{
	$insert_stmt .= "$_.ticker = '$_' and ";
}
foreach (@indexlist) 
{
	$insert_stmt .= "$_.date = $indexlist[4].date and " if ($_ ne $indexlist[4]);
}
$insert_stmt =~ s/and $//;
print STDERR "$insert_stmt\n";
$db->do($insert_stmt);

