package com.phillypham.prime;

public abstract class ProbabilisticPrimalityTester
    extends PrimalityTester {
    
    public abstract boolean isProbablePrime(long p);
    
    public abstract boolean isProbablePrime(long p, int certainty);
    
}