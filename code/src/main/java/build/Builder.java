package build;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Builder {

    public static Job getJob(String in, String out) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BuildGraph");

        job.setJarByClass(Builder.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(BuildMapper.class);
        job.setReducerClass(BuildReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static void main(String[] args) {
        try{
            if(args.length < 2) {
                System.out.println("Usage: xxx <in> <out> <nameList>");
                System.exit(-1);
            }

            Job job = Builder.getJob(args[0], args[1]);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
