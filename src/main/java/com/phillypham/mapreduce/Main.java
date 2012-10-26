package com.phillypham.mapreduce;

import java.util.Date;

import org.apache.hadoop.util.ToolRunner;

import com.phillypham.mapreduce.job.IPv4AddressGenerator;
import com.phillypham.mapreduce.job.PrimeFilter;

public class Main {
    public static void main(String[] argv) throws Exception {
        Date startTime = new Date();
        System.out.println("Jobs started: " + startTime);
        
        String[] ipv4AddressGeneratorInputs;
        if (argv[0].equals("strict")) {
            ipv4AddressGeneratorInputs = new String[argv.length-1];
            System.arraycopy(argv, 1, ipv4AddressGeneratorInputs, 0, argv.length-1);
        } else {
            ipv4AddressGeneratorInputs = new String[argv.length];
            System.arraycopy(argv, 0, ipv4AddressGeneratorInputs, 0, argv.length);
        }        
        String[] primeFilterInputs = new String[3];
        primeFilterInputs[0] = (argv[0].equals("strict")) ? "strict" : "lenient";
        primeFilterInputs[1] = argv[argv.length-1] + "/addresses";
        primeFilterInputs[2] = argv[argv.length-1];
        
        ToolRunner.run(null, new IPv4AddressGenerator(), ipv4AddressGeneratorInputs);
        int numPrimes = ToolRunner.run(null, new PrimeFilter(), primeFilterInputs);
        System.out.println(numPrimes + " primes were found in the following IPv4 address range(s):");
        for (int i = 0; i < ipv4AddressGeneratorInputs.length-1; i++) {
            System.out.println(ipv4AddressGeneratorInputs[i]);
        }
        
        Date endTime = new Date();
        System.out.println("Jobs ended: " + endTime);
        System.out.println("These jobs took " + 
                           (endTime.getTime() - startTime.getTime())/1000 + 
                           " seconds.");
        System.exit(0);
    }
}