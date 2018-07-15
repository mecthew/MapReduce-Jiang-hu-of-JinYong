package lpa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class LPAInit {
    public static class LPAInitMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static int labelCount;
        @Override
        protected void setup(Context context){
            labelCount = 0;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer first = new StringTokenizer(value.toString(), "\t"); //根据文件格式决定" "或"\t"
            String personName = first.nextToken(), graphList = first.nextToken();

            StringTokenizer itr = new StringTokenizer(graphList, "#");
            String PR = itr.nextToken(), list = itr.nextToken();

            labelCount++;
            String outList = String.valueOf(labelCount) + "#" + list;
//            System.out.println(outList);
            context.write(new Text(personName+"#"+PR), new Text(outList));
        }

    }

    public static void main(String[] args){
        Configuration conf = new Configuration();
        try {
            Job job2 = Job.getInstance(conf, "LPAInit");
            job2.setJarByClass(LPAInit.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setMapperClass(LPAInitMapper.class);
//            job2.setReducerClass(PRlterReducer.class);

            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);
        }
        catch (Exception e){e.printStackTrace();}
    }

}
