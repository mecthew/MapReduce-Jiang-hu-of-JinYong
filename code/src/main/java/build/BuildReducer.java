package build;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class BuildReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*
        * INPUT: <name1, [name2:freq;...]>
        * OUTPUT: <name1, [name2:freq/sum;...]>
        * */
        long sum = 0;
        LinkedList<String> nameList = new LinkedList<>();
        LinkedList<Long> freqList = new LinkedList<>();
        for(Text value : values) {
            String[] strs = value.toString().split(":");
            nameList.add(strs[0]);
            long freq = Long.valueOf(strs[1]);
            freqList.add(freq);
            sum += freq;
        }

        StringBuilder stringBuilder = new StringBuilder();
        for(int i=0; i < nameList.size(); i++) {
			if(i==0)
				stringBuilder.append("0.1"+"#");
            float freq = freqList.get(i) / (float)sum;
            stringBuilder.append(nameList.get(i) + ":" + freq);
            if(i != nameList.size()-1) {
                stringBuilder.append(";");
            }
        }

        context.write(key, new Text(stringBuilder.toString()));
    }
}
