package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankViewer {

    public static class PageRankViewerMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
        private Text outName = new Text();
        private FloatWritable outPR = new FloatWritable();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String personName = line[0];
            Float PR = Float.parseFloat(line[1].split("#")[0]);
            String restList = line[1].split("#")[1];
            outPR.set(PR);
            outName.set(personName);
            context.write(outPR, outName);
        }
    }


    /**
     * 重载key的比较函数，使其经过shuffle和sort后反序（从大到小）输出
     **/
    public static class DescFloatComparator extends FloatWritable.Comparator {
        //@Override
        public float compare(WritableComparator a,
                             WritableComparable<FloatWritable> b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    /**
    *可能存在PR值相同的键对应的值包含多个人名的情况，因此需要reduce过程
    **/
    public static class PageRankViewerReducer extends Reducer<FloatWritable,Text,FloatWritable,Text> {
        protected void reduce(FloatWritable key, Iterable<Text> value, Context context)throws IOException, InterruptedException{
            for(Text element: value){
                context.write(key, element);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        try {
            Job job3 = Job.getInstance(conf, "PageRankViewer");
            job3.setJarByClass(PageRankViewer.class);
            job3.setOutputKeyClass(FloatWritable.class);
            job3.setSortComparatorClass(DescFloatComparator.class);
            job3.setOutputValueClass(Text.class);
            job3.setMapperClass(PageRankViewerMapper.class);
            job3.setReducerClass(PageRankViewerReducer.class);
            FileInputFormat.addInputPath(job3, new Path(args[0]));
            FileOutputFormat.setOutputPath(job3, new Path(args[1]));
            job3.waitForCompletion(true);
        }
        catch (Exception e){e.printStackTrace();}
    }
}