package com.phillypham.mapreduce.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import com.phillypham.prime.*;


public class PrimeFilter
    extends Configured implements Tool {

    private static final String PRIMALITY_TESTING_TYPE_KEY = "primality.testing.type";

    public static class PrimeFilterMapper 
        extends Mapper<LongWritable, Text, LongWritable, Text> {

        ProbabilisticPrimalityTester primalityTester;
        boolean strict;

        public void setup(Context context)
            throws IOException, InterruptedException {
            String primalityTestingType = context.getConfiguration().get(PRIMALITY_TESTING_TYPE_KEY);
            strict = (primalityTestingType.equals("strict")) ? true : false;
            primalityTester = new MillerRabin();
        }
        
        public void map(LongWritable number,
                        Text address,
                        Context context)
            throws IOException, InterruptedException {
            if(strict) {
                if(primalityTester.isPrime(number.get())) context.write(number, address);
            } else {
                if(primalityTester.isProbablePrime(number.get())) context.write(number, address);
            }
        }
    }
    
    public int run(String[] args) throws Exception {
        // make sure there are enough inputs
        if (args.length != 2 && args.length != 3) {
            System.err.println("Usage: "+getClass().getName()+" [strict] <sequence file input path> <output path>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // configure and create new job
        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        if (args.length == 3) configuration.set(PRIMALITY_TESTING_TYPE_KEY, args[0]);
        Job job = new Job(configuration);
        job.setJobName("prime-filter");
        job.setJarByClass(PrimeFilter.class);

        // configure io
        String inDir = args[args.length - 2];
        String outDir = args[args.length - 1];

        FileInputFormat.addInputPath(job, new Path(inDir));
        FileOutputFormat.setOutputPath(job, new Path(outDir, "primes"));

        // set input format
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // set mapper
        job.setMapperClass(PrimeFilterMapper.class);

        // set reducer, there are none
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);

        // run job
        job.waitForCompletion(true);
        
        // get number of primes from counters
        int numPrimes = (int) job.getCounters().
            findCounter("org.apache.hadoop.mapred.Task$Counter","MAP_OUTPUT_RECORDS").
            getValue();
        FSDataOutputStream primeCountOut = fs.create(new Path(outDir, "primeCount.txt"));
        try {
            primeCountOut.writeChars("primes\t" + numPrimes + "\n");
        } finally {
            primeCountOut.close();
        }
        
        return numPrimes;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(null, new PrimeFilter(), args));
    }
}