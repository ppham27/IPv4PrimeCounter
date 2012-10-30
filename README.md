IPv4PrimeCounter
================

A map-reduce program to generate IPv4 addresses and filter out prime numbers using the Miller-Rabin primality test.

Installation:
git clone https://github.com/ppham27/IPv4PrimeCounter.git && cd IPv4PrimeCounter
mvn install && cd target

Usage:
hadoop jar IPv4PrimeCounter-1.1.jar com.phillypham.mapreduce.Main [strict] <IPv4 CIDR(s) list...> <output path>

Examples:
Normal mode (uses probabilistic Miller-Rabin):
hadoop jar IPv4PrimeCounter-1.1.jar com.phillypham.mapreduce.Main 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 /tmp/private_ipv4_primes
Strict mode (assumes Riemann hypothesis and uses deterministic Miller-Rabin):
hadoop jar IPv4PrimeCounter-1.1.jar com.phillypham.mapreduce.Main strict 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 /tmp/private_ipv4_primes_strict
