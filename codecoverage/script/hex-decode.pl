#!/opt/local/bin/perl

while (<STDIN>)
{
	my $line = $_;
	$line =~ s/([0-9,a-f][0-9,a-f])/\\x$1/g;
	eval("printf \"$line\"\n");
}

