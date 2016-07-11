#!/usr/bin/perl

use Digest::MurmurHash3 qw( murmur128_x64 );


my $data_to_hash = shift; 

# Create two 64 bit pieces (your perl must be built to use 64bit ints)
my (@array) = murmur128_x64( $data_to_hash );

foreach(@array) {

  print $_ . "<p>\n";

}

