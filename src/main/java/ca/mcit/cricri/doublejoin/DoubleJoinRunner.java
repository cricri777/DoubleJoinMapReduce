package ca.mcit.cricri.doublejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DoubleJoinRunner {

    public static void main(String[] args) throws Exception {
        // Configuration
        Configuration configuration = new Configuration();

        // Paths to input and output
        Path input = new Path(args[0]);
        Path intermediateResult = new Path("/user/cricri777/intermediate");
        Path output = new Path(args[1]);

        // Job
        Job job1 = Job.getInstance(configuration, "join-R-and-S-cricri");

        // Job classes to run
        job1.setJarByClass(DoubleJoinRunner.class);
        job1.setMapperClass(DoubleJoinR_SMapper.class);
        job1.setReducerClass(DoubleJoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediateResult);

//        System.exit(job.waitForCompletion(true)?0:1);
        job1.waitForCompletion(true);


        Job job2 = Job.getInstance(configuration, "join-S-and-t-cricri");
        FileInputFormat.addInputPath(job2, intermediateResult);
        FileOutputFormat.setOutputPath(job2, output);
        job2.waitForCompletion(true);
    }

}