package com.phillypham.mapreduce.job;

import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.phillypham.prime.Prime;


public class PrimeFilter
    extends Configured implements Tool {
        
    public static class PrimeFilterStrictMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {            
        public void map(LongWritable number,
                        Text address,
                        Context context)
            throws IOException, InterruptedException {
            if(Prime.isPrime(number.get())) {
                context.write(number, address);
            }
        }
    }

    public static class PrimeFilterLenientMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {                
        public void map(LongWritable number,
                        Text address,
                        Context context)
            throws IOException, InterruptedException {
            if(Prime.isProbablePrime(number.get())) {
                context.write(number, address);
            }
        }
    }


    public int run(String[] args) throws Exception {
        // make sure there are enough inputs
        if (args.length != 2 && args.length != 3) {
            System.err.println("Usage: "+getClass().getName()+" [strict] <input path> <output path>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // get file system, create new job, and name it
        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Job job = new Job(configuration);
        job.setJobName("prime-filter");

        // configure io
        String inDir;
        String outDir;
        if(args.length == 2) {
            inDir = args[0];
            outDir = args[1];
        } else {
            inDir = args[1];
            outDir = args[2];            
        }
        FileInputFormat.addInputPath(job, new Path(inDir));
        FileOutputFormat.setOutputPath(job, new Path(outDir, "primes"));

        // set input format
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // set mapper
        if (args[0].equals("strict")) {
            System.out.println("Using strict mode...");
            job.setMapperClass(PrimeFilterStrictMapper.class);
        } else {
            job.setMapperClass(PrimeFilterLenientMapper.class);
        }

        // set reducer, there are none
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);

        // run job
        job.waitForCompletion(false);
        
        // get number of primes from counters
        int numPrimes = (int) job.getCounters().
            findCounter("org.apache.hadoop.mapred.Task$Counter","MAP_OUTPUT_RECORDS").
            getValue();        
        PrintWriter primeCountOut = new PrintWriter(new File(outDir, "primeCount.txt"));
        try {
            primeCountOut.println("primes\t" + numPrimes);
        } finally {
            primeCountOut.close();
        }
        
        return numPrimes;
    }    
}