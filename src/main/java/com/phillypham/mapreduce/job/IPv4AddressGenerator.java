package com.phillypham.mapreduce.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IPv4AddressGenerator
    extends Configured implements Tool {

    private static final int IPV4_BITS = 32;
    private static final int BITS_IN_BYTE = 8;
    private static final long ADDRESSES_PER_MAP = 0x80000;
    private static final Pattern IPV4_PATTERN = Pattern.compile("^(?:(?:2[0-4][0-9]|25[0-5]|[01][0-9]{2}|[0-9]{1,2})\\.){3}(?:2[0-4][0-9]|25[0-5]|[01][0-9]{2}|[0-9]{1,2})/(?:[0-9]|[12][0-9]|3[0-2])$");
    private static final Path TMP_DIR = new Path(IPv4AddressGenerator.class.getSimpleName() + "_TMP");

    private static String longToIPv4(long num) {
        return ((num & 0xff000000) >> 24) + "." + 
            ((num & 0x00ff0000) >> 16) + "." + 
            ((num & 0x0000ff00) >> 8) + "." + 
            (num & 0x000000ff);
    }

    public static Long[] parseCIDR(String cidr) {
        Long[] parsedCIDR = new Long[2];
        String[] ipAndPrefix = cidr.split("/");
        String[] ipSplit = ipAndPrefix[0].split("\\.");
        parsedCIDR[0] = 0L;
        for (int j=0; j < ipSplit.length; j++) {
            parsedCIDR[0] += (long) (Long.parseLong(ipSplit[ipSplit.length-1-j])*
                                     Math.pow(2,j*BITS_IN_BYTE));
        }
        parsedCIDR[1] = (long) Math.pow(2, IPV4_BITS-Long.parseLong(ipAndPrefix[1]));
        return parsedCIDR;
    }

    public static boolean isValidCIDR(String cidr) {
        if(!IPV4_PATTERN.matcher(cidr).matches()) { return false; }
        Long[] parsedCIDR = parseCIDR(cidr);
        if (parsedCIDR[0] + parsedCIDR[1] - 1 > 0xffffffffL) { return false; }
        return true;
    }

    public static class IPv4AddressGeneratorMapper
        extends Mapper<LongWritable, LongWritable, LongWritable, Text> {

        public void map(LongWritable base,
                        LongWritable size,
                        Context context)
            throws IOException, InterruptedException {
            for (long i = 0; i < size.get(); i++) {                
                long key = base.get()+i;
                context.write(new LongWritable(key),
                              new Text(IPv4AddressGenerator.longToIPv4(key)));
            }
        }
    }

    public int run(String[] argv) throws Exception {
        // make sure there are enough inputs
        if (argv.length < 2) {
            System.err.println("Usage: "+getClass().getName()+" <IPv4 CIDR(s) list...> <output path>");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // get file system, create new job, and name it
        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Job job = new Job(configuration);
        job.setJobName("ipv4-generator");
        job.setJarByClass(IPv4AddressGenerator.class);

        // regurgitate input and make sure it is valid to give user feedback
        // first parse IPv4 ranges in CIDR format
        Map<Long, Long> ipToPrefix = new HashMap<Long, Long>(argv.length-1);
        System.out.println("IPv4 range(s) to expand:");
        for ( int i = 0; i < argv.length - 1; i++ ) {
            System.out.println(argv[i]);
            if(isValidCIDR(argv[i])) {
                // parse cidr and put in map
                Long[] parsedCIDR = parseCIDR(argv[i]);
                ipToPrefix.put(parsedCIDR[0], parsedCIDR[1]);
            } else {
                System.err.println(argv[i] + " is not a valid IPv4 address range. IPv4 addresses must be specified using CIDR. For example, 192.168.0.0/16.");
                return -1;
            }
        }
        Path inDir = new Path(argv[argv.length-1],TMP_DIR);
        Path outDir = new Path(argv[argv.length-1],"addresses");
        System.out.println("Output to be written to " + outDir.toUri());

        // write input
        int numMaps = 0;
        try {
            for ( Entry<Long, Long> e : ipToPrefix.entrySet() ) {
                int splits = (int) Math.ceil((1.0*e.getValue())/ADDRESSES_PER_MAP);
                for ( int i = 0; i < splits; i++) {
                    long base = e.getKey() + i*ADDRESSES_PER_MAP;                    
                    long size = (i < splits - 1 || e.getValue() % ADDRESSES_PER_MAP == 0) ?
                        ADDRESSES_PER_MAP : e.getValue() % ADDRESSES_PER_MAP;
                    Path file = new Path(inDir,"part" + numMaps);
                    SequenceFile.Writer writer = SequenceFile.
                        createWriter(fs,
                                     configuration,
                                     file,
                                     LongWritable.class,
                                     LongWritable.class,
                                     CompressionType.NONE);
                    try {                        
                        writer.append(new LongWritable(base),
                                      new LongWritable(size));
                    } finally {
                        writer.close();
                    }
                    numMaps++;
                }
            }
            // configure job
            job.setMapperClass(IPv4AddressGeneratorMapper.class);
            job.setReducerClass(Reducer.class);
            // set input
            FileInputFormat.addInputPath(job, inDir);
            job.setInputFormatClass(SequenceFileInputFormat.class);
        
            // set output
            FileOutputFormat.setOutputPath(job, outDir);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // no reducer 
            job.setNumReduceTasks(0);

            // run job
            job.waitForCompletion(true);
            
        } finally {
            fs.delete(inDir, true);
        }
        return 0;
    }

    public static void main(String[] argv) throws Exception {
        System.exit(ToolRunner.run(null, new IPv4AddressGenerator(), argv));
    }
}