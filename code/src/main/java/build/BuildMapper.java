package build;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BuildMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        /*
         *INPUT:<key, (name1_name2\tfreq)>
         *OUTPUT:<name1, name2:freq>
        */
        String[] strings = value.toString().split("\t");
        String[] names = strings[0].split("_");
        context.write(new Text(names[0]), new Text(names[1] + ":" + strings[1]));
    }
}
