package com.phillypham.mapreduce.job;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;



public class TestIPv4AddressGenerator {
    
    @Test
    public void testParseCIDR() {
        Long[] tmp;
        tmp = IPv4AddressGenerator.parseCIDR("192.168.0.0/16");
        assertEquals(tmp[0].longValue(), 3232235520L);
        assertEquals(tmp[1].longValue(), 65536L);
        
        tmp = IPv4AddressGenerator.parseCIDR("176.16.0.0/12");
        assertEquals(tmp[0].longValue(), 2953838592L);
        assertEquals(tmp[1].longValue(), 1048576L);

        tmp = IPv4AddressGenerator.parseCIDR("10.0.0.0/8");
        assertEquals(tmp[0].longValue(), 167772160L);
        assertEquals(tmp[1].longValue(), 16777216L);

        tmp = IPv4AddressGenerator.parseCIDR("0.0.0.0/0");
        assertEquals(tmp[0].longValue(), 0L);
        assertEquals(tmp[1].longValue(), 4294967296L);

        tmp = IPv4AddressGenerator.parseCIDR("255.255.255.255/32");
        assertEquals(tmp[0].longValue(), 4294967295L);
        assertEquals(tmp[1].longValue(), 1L);
    }

    @Test
    public void testIsValidCIDR() {
        assertTrue(IPv4AddressGenerator.isValidCIDR("0.0.0.0/0"));
        assertTrue(IPv4AddressGenerator.isValidCIDR("198.168.0.0/16"));
        assertTrue(IPv4AddressGenerator.isValidCIDR("10.0.0.0/8"));
        assertTrue(IPv4AddressGenerator.isValidCIDR("176.16.0.0/12"));        
        assertTrue(IPv4AddressGenerator.isValidCIDR("255.255.255.255/32"));
        assertFalse(IPv4AddressGenerator.isValidCIDR("255.255.255.255/31"));
        assertFalse(IPv4AddressGenerator.isValidCIDR("198.168.0.0/1"));
        assertFalse(IPv4AddressGenerator.isValidCIDR("198.168.0.0/33"));
        assertFalse(IPv4AddressGenerator.isValidCIDR("256.168.0.0/32"));
        assertFalse(IPv4AddressGenerator.isValidCIDR("198.168.0.256/32"));
    }

    @Test
    public void testIPv4AddressGeneratorMapper() {
        MapDriver<LongWritable, LongWritable, LongWritable, Text> mapOut =
            new MapDriver<LongWritable, LongWritable, LongWritable, Text>().
            withMapper(new IPv4AddressGenerator.IPv4AddressGeneratorMapper()).
            withInput(new LongWritable(3232235520L), new LongWritable(4L));
        mapOut.addOutput(new LongWritable(3232235520L), new Text("192.168.0.0"));
        mapOut.addOutput(new LongWritable(3232235521L), new Text("192.168.0.1"));
        mapOut.addOutput(new LongWritable(3232235522L), new Text("192.168.0.2"));
        mapOut.addOutput(new LongWritable(3232235523L), new Text("192.168.0.3"));
        mapOut.runTest();
    }
}