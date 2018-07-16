package pagerank;

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
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

public class PageRanklter {
//    private static final double damping = 0.85;
//    private static final double number = 0.001;

    public static class PRlterMapper extends Mapper<LongWritable, Text, Text, Text> {
        /*
        输入格式：
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
            String personName = itr.nextToken(), list = itr.nextToken();
            StringTokenizer listToken = new StringTokenizer(list, "#");
            Double PR = Double.parseDouble(listToken.nextToken());
            String nameList = listToken.nextToken();

            StringTokenizer nameItr = new StringTokenizer(nameList, ";");
            while(nameItr.hasMoreTokens()){
                String[] element = nameItr.nextToken().split(":"); //第一个元素为人名，第二个元素为归一化的权值
                Double PRValue = Double.parseDouble(element[1])*PR;
                BigDecimal bigDecimal = BigDecimal.valueOf(PRValue);
                DecimalFormat decimalFormat = new DecimalFormat("0.00000");
                context.write(new Text(element[0]), new Text(decimalFormat.format(bigDecimal)));
            }


            nameList = "#" + nameList;
            context.write(new Text(personName),new Text(nameList));
        }
    }


    public static class PRlterReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String list = new String();
            Double sum = new Double(0);
            for(Text element: values){
                String str = element.toString();
                if(str.length()>0 && str.charAt(0) == '#'){
                    list = new String(str);
                }
                else if(str.length()> 0){
                    sum += Double.parseDouble(str);
                }
            }
            BigDecimal bigDecimal = BigDecimal.valueOf(sum);
            DecimalFormat decimalFormat = new DecimalFormat("0.00000");
            String relationList = decimalFormat.format(bigDecimal)+list;
            context.write(key, new Text(relationList));
        }
    }

    public static void main(String[] args){
        Configuration conf = new Configuration();
        try {
            Job job2 = Job.getInstance(conf, "PageRanklter");
            job2.setJarByClass(PageRanklter.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setMapperClass(PRlterMapper.class);
            job2.setReducerClass(PRlterReducer.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);
        }
        catch (Exception e){e.printStackTrace();}
    }

}
