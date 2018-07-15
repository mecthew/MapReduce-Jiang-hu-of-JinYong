package lpa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class LPAReorganize {
    public static class LPAReorganizeMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text outFirst = new Text(), outSecond = new Text();
        @Override
        protected  void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            String[] line = value.toString().split("\t");
            String personName = line[0];
            String label = line[1].split("#")[0];
            String restList = line[1].split("#")[1];
            outFirst.set(label);
            outSecond.set(personName+"#"+restList);
            context.write(outFirst, outSecond);
        }
    }

    public static class LPAReorganizeReducer extends Reducer<Text, Text, Text, Text> {
        public static Integer labelCount;
        @Override
        protected void setup(Context context){
            labelCount = 0;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException{
            ++labelCount;
            for(Text element: value){
                StringTokenizer itr = new StringTokenizer(element.toString(), "#");
                String personName = itr.nextToken(), PR = itr.nextToken(), restList = itr.nextToken();
                String outFirst = labelCount.toString()+"#"+personName+"#"+PR, outSecond = restList;
                context.write(new Text(outFirst), new Text(outSecond));
            }
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try {
            Job job3 = Job.getInstance(conf, "LPAReorganize");
            job3.setJarByClass(LPAReorganize.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            job3.setMapperClass(LPAReorganizeMapper.class);
            job3.setReducerClass(LPAReorganizeReducer.class);
            FileInputFormat.addInputPath(job3, new Path(args[0]));
            FileOutputFormat.setOutputPath(job3, new Path(args[1]));
            job3.waitForCompletion(true);
        }
        catch (Exception e){e.printStackTrace();}
    }
}
