package com.phillypham.prime;

public class NativeMillerRabin
    extends ProbabilisticPrimalityTester {

    static {
        System.loadLibrary("MillerRabin");
    }

    public native boolean isPrime(long p);
    
    public native boolean isProbablePrime(long p);
    
    public native boolean isProbablePrime(long p, int n);
}