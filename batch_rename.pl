open(DAT, "tickers.txt") || die("Could not open file!");
my @stocklist=<DAT>;
close DAT;
foreach (@stocklist) {
	chomp;
	system "mv opinion/$_.csv opinion/$_.html";
}
