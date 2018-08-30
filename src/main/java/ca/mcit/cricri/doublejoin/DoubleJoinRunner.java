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
        Path intermediateResult = new Path("/user/cricri/intermediate");
        Path output = new Path(args[1]);

        // Job
        Job job1 = Job.getInstance(configuration, "join-R-and-S-cricri");

        // Job classes to run
        job1.setJarByClass(DoubleJoinRunner.class);
        job1.setMapperClass(DoubleJoinMap_FirstIteration.class);
        job1.setReducerClass(DoubleJoinReducer_FirstIteration.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediateResult);

        job1.waitForCompletion(true);


        Job job2 = Job.getInstance(configuration, "join-S-and-All-cricri");
        job2.setJarByClass(DoubleJoinRunner.class);
        job2.setMapperClass(DoubleJoinMap_SecondIteration.class);
        job2.setReducerClass(DoubleJoinReducer_SecondIteration .class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, input, intermediateResult);
        FileOutputFormat.setOutputPath(job2, output);

        System.exit(job2.waitForCompletion(true)?0:1);
    }

}