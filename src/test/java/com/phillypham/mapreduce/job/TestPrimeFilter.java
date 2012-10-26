package com.phillypham.mapreduce.job;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

public class TestPrimeFilter {

    Map<LongWritable, Text> primes;
    Map<LongWritable, Text> nonPrimes;

    @Before
    public void setUp() {
        primes = new HashMap<LongWritable, Text>();
        primes.put(new LongWritable(3232235537L), new Text("192.168.0.17"));
        primes.put(new LongWritable(3232235557L), new Text("192.168.0.37"));
        primes.put(new LongWritable(3232235569L), new Text("192.168.0.49"));
        primes.put(new LongWritable(3232235599L), new Text("192.168.0.79"));
        primes.put(new LongWritable(3232235623L), new Text("192.168.0.103"));
        primes.put(new LongWritable(2L), new Text("0.0.0.2"));
        
        nonPrimes = new HashMap<LongWritable, Text>();
        nonPrimes.put(new LongWritable(3232235520L), new Text("192.168.0.0"));
        nonPrimes.put(new LongWritable(3232235521L), new Text("192.168.0.1"));
        nonPrimes.put(new LongWritable(9L), new Text("0.0.0.9"));
    }
    
    @Test
    public void testPrimeFilterStrictMapper() {
        for (Entry<LongWritable, Text> e : primes.entrySet()) {
            new MapDriver<LongWritable, Text, LongWritable, Text>().
                withMapper(new PrimeFilter.PrimeFilterStrictMapper()).
                withInput(e.getKey(), e.getValue()).
                withOutput(e.getKey(), e.getValue()).
                runTest();
        }
        
        for (Entry<LongWritable, Text> e : nonPrimes.entrySet()) {
            new MapDriver<LongWritable, Text, LongWritable, Text>().
                withMapper(new PrimeFilter.PrimeFilterStrictMapper()).
                withInput(e.getKey(), e.getValue()).
                runTest();
        }                              
    }

    @Test
    public void testPrimeFilterLenientMapper() {
        for (Entry<LongWritable, Text> e : primes.entrySet()) {
            new MapDriver<LongWritable, Text, LongWritable, Text>().
                withMapper(new PrimeFilter.PrimeFilterLenientMapper()).
                withInput(e.getKey(), e.getValue()).
                withOutput(e.getKey(), e.getValue()).
                runTest();
        }
        
        for (Entry<LongWritable, Text> e : nonPrimes.entrySet()) {
            new MapDriver<LongWritable, Text, LongWritable, Text>().
                withMapper(new PrimeFilter.PrimeFilterLenientMapper()).
                withInput(e.getKey(), e.getValue()).
                runTest();
        }        
    }    
}