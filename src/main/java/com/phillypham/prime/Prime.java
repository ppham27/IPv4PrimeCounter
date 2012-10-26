package com.phillypham.prime;

import java.math.BigInteger;
import java.util.Random;
import java.util.ArrayList;

public class Prime {
    public static boolean isPrime(long p) {
        if (!isProbablePrime(p)) return false;
        // assume riemann hypothesis and do deterministic miller-rabin test
        long d = p - 1;
        int s = 0;
        while (d % 2 == 0) {
            d >>= 1;
            s += 1;
        }
        long upperLimit = (long) Math.min(p-1, Math.floor(2*Math.pow(Math.log(p),2)));
        for (long i = 2; i <= upperLimit; i++) {
            if(!witnessesPrime(i, p, s, d)) return false;
        }
        return true;
    }

    public static boolean isProbablePrime(long p) {
        return isProbablePrime(p, 20);
    }

    public static boolean isProbablePrime(long p, int n) {
        if (p < 2) return false;        
        if (p == 2) return true; // 2 is prime
        // check to make sure p is odd
        if (p % 2 == 0) return false;
        // suppose p is prime by default
        // try to prove that it is composite
        // factor p-1 into 2^r*d
        long d = p - 1L;
        int s = 0;
        while (d % 2 == 0) {
            d >>= 1;
            s += 1;
        }
        long witness;
        Random rand = new Random();
        for (int i=0; i < n; i++) {
            witness = (long) (2+rand.nextDouble()*(p-3));
            if(!witnessesPrime(witness, p, s, d)) return false;
        }
        return true;
    }
    
    public static ArrayList<Long> getPrimes(long n) {
        return getPrimes(n, true);
    }

    public static ArrayList<Long> getPrimes(long n, boolean strict) {
        ArrayList<Long> primes = new ArrayList<Long>();
        for (long p = 2; p < n; p++) {
            if(strict) {
                if (isPrime(p)) primes.add(p);
            } else {
                if (isProbablePrime(p)) primes.add(p);
            }
        }
        return primes;
    }      

    public static long modExp(long a, long e, long m) {
        if (m*m >= 0) {         // m is small
            long res = 1;
            while (e > 0) {
                if (e % 2 == 1) res = (res * a) % m;
                e >>= 1;
                a = (a*a) % m;
            }
            return res;
        } else {                // m is too big, use big integer classes
            BigInteger bigRes = new BigInteger(String.valueOf(1));
            BigInteger bigA = new BigInteger(String.valueOf(a));
            BigInteger bigM = new BigInteger(String.valueOf(m));
            BigInteger bigTwo = new BigInteger(String.valueOf(2));
            while (e > 0) {
                if (e % 2 == 1) bigRes = bigRes.multiply(bigA).mod(bigM);
                e >>= 1;
                bigA = bigA.modPow(bigTwo,bigM);
            }
            return bigRes.longValue();
        }
    }

    private static boolean witnessesPrime(long witness, long p, long s, long d) {
        long tmp = modExp(witness, d, p);
        if(tmp == 1 || tmp == p - 1) return true;
        long powOfTwo = 2;
        for (int j=1; j < s; j++) {
            if (modExp(witness, powOfTwo*d, p) == p - 1) return true;
            powOfTwo <<= 1;
        }
        return false;
    }
}
