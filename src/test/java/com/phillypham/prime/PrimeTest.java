package com.phillypham.prime;

import static org.junit.Assert.*;
import org.junit.Test;


public class PrimeTest {

    @Test
    public void testIsPrime() {
        assertFalse(Prime.isPrime(0));
        assertFalse(Prime.isPrime(1));
        assertFalse(Prime.isPrime(-1));
        assertFalse(Prime.isPrime(9));
        assertFalse(Prime.isPrime(88573));
        assertFalse(Prime.isPrime(79381));
        assertFalse(Prime.isPrime(60351));
        assertFalse(Prime.isPrime(137149));
        assertTrue(Prime.isPrime(2));
        assertTrue(Prime.isPrime(6829));
        assertTrue(Prime.isPrime(906349));
        assertTrue(Prime.isPrime(736823));
        assertTrue(Prime.isPrime(4294967291L));
    }

    @Test
    public void testIsProbablePrimeLongInt() {
        assertFalse(Prime.isProbablePrime(0,20));
        assertFalse(Prime.isProbablePrime(1,20));
        assertFalse(Prime.isProbablePrime(-1,20));
        assertFalse(Prime.isProbablePrime(9,20));
        assertFalse(Prime.isProbablePrime(88573,20));
        assertFalse(Prime.isProbablePrime(79381,20));
        assertFalse(Prime.isProbablePrime(60351,20));
        assertFalse(Prime.isProbablePrime(137149,20));
        assertTrue(Prime.isProbablePrime(2,20));
        assertTrue(Prime.isProbablePrime(6829,20));
        assertTrue(Prime.isProbablePrime(906349,20));
        assertTrue(Prime.isProbablePrime(736823,20));
        assertTrue(Prime.isProbablePrime(3232235569L,20));
    }
    
    @Test
    public void testGetPrimesLongBoolean() {
        assertEquals(Prime.getPrimes(3, true).size(),1);
        assertEquals(Prime.getPrimes(1000, true).size(), 168);
        assertEquals(Prime.getPrimes(1000000, false).size(), 78498);
    }

    @Test
    public void testModExp() {
        assertEquals(Prime.modExp(5,6,7),1);
        assertEquals(Prime.modExp(11,13,53),52);
        assertEquals(Prime.modExp(23421,2234124,64),49);
    }    
}
