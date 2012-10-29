package com.phillypham.prime;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Before;

public class TestMillerRabin{

    ProbabilisticPrimalityTester primalityTester;
    
    @Before
    public void setUp() {
        primalityTester = new MillerRabin();
    }

    @Test
    public void testIsPrime() {
        assertFalse(primalityTester.isPrime(0));
        assertFalse(primalityTester.isPrime(1));
        assertFalse(primalityTester.isPrime(-1));
        assertFalse(primalityTester.isPrime(9));
        assertFalse(primalityTester.isPrime(88573));
        assertFalse(primalityTester.isPrime(79381));
        assertFalse(primalityTester.isPrime(60351));
        assertFalse(primalityTester.isPrime(137149));
        assertTrue(primalityTester.isPrime(2));
        assertTrue(primalityTester.isPrime(6829));
        assertTrue(primalityTester.isPrime(906349));
        assertTrue(primalityTester.isPrime(736823));
        assertTrue(primalityTester.isPrime(4294967291L));
    }

    @Test
    public void testIsProbablePrimeLongInt() {
        assertFalse(primalityTester.isProbablePrime(0,20));
        assertFalse(primalityTester.isProbablePrime(1,20));
        assertFalse(primalityTester.isProbablePrime(-1,20));
        assertFalse(primalityTester.isProbablePrime(9,20));
        assertFalse(primalityTester.isProbablePrime(88573,20));
        assertFalse(primalityTester.isProbablePrime(79381,20));
        assertFalse(primalityTester.isProbablePrime(60351,20));
        assertFalse(primalityTester.isProbablePrime(137149,20));
        assertTrue(primalityTester.isProbablePrime(2,20));
        assertTrue(primalityTester.isProbablePrime(6829,20));
        assertTrue(primalityTester.isProbablePrime(906349,20));
        assertTrue(primalityTester.isProbablePrime(736823,20));
        assertTrue(primalityTester.isProbablePrime(3232235569L,20));

        int primeCount = 0;
        for (int p = 0; p < 1000000; p++) {
            if (primalityTester.isProbablePrime(p)) primeCount++;
        }
        assertEquals(78498, primeCount);
    }
}