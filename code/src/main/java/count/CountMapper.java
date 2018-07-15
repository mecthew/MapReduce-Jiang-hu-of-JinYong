package count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class CountMapper extends Mapper<Object, Text, Text, LongWritable> {

    private static LongWritable valueOut = new LongWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] names = value.toString().split(" ");
        HashSet<String> set = new HashSet<>();
        for(String name: names) {
            set.add(name);
        }

        for(String name1 : set) {
            for(String name2: set) {
                if(!name1.equals(name2)) {
                    context.write(new Text(name1 + "_" + name2), valueOut);
                }
            }
        }
    }
}
