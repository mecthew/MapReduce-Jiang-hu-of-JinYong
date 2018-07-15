package lpa;


import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.jcodings.util.Hash;
//import edu.umd.cs.findbugs.annotations.CleanupObligation;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.IOException;
import java.util.*;

public class LPAIteration {
    public static class LPAIterationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
            String personName_PR = itr.nextToken(), list = itr.nextToken();
            String personName = personName_PR.split("#")[0], PR = personName_PR.split("#")[1];
            String[] subList = list.split("#");
            String label = subList[0];
            String outList = subList[1];
            StringTokenizer nameList = new StringTokenizer(subList[1], ";");

            while(nameList.hasMoreTokens()){
                String[] element = nameList.nextToken().split(":");
                String labelTurple = label+"#"+personName;  //格式为 label+personName
                context.write(new Text(element[0]),new Text(labelTurple));
            }

            outList = "#" + outList;
            context.write(new Text(personName), new Text(outList));
            context.write(new Text(personName), new Text("$"+label));  //保存上一次的label
            context.write(new Text(personName), new Text("@"+PR));
        }

    }

    public static class LPAIterationReducer extends Reducer<Text, Text, Text, Text> {
        private static Double changeCount, changeRate;
        private static Integer labelCount;
        private static HashMap<String, String> name_label_Map;
        @Override
        protected void setup(Context context){
            name_label_Map = new HashMap<String,String>();
            changeCount = changeRate = 0.0;
            labelCount = 0;
        }
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
            ++labelCount;
            String list = null, prevLabel = null, key_pr = null;
            ArrayList<String> lableList = new ArrayList<>();

            for(Text element: value){
                String str = element.toString();
                if(str.length() > 0 && str.charAt(0)== '#'){
                    list = new String(str);
                }
                else if(str.length() > 0 && str.charAt(0) == '$'){
                    prevLabel = str.replace("$","");
                }
                else if(str.length() > 0 && str.charAt(0) == '@'){
                    key_pr = str.replace("@", "");
                }
                else if(str.length()>0){
                    lableList.add(str);
                }
            }

            if(list != null){
                String subList = list.replace("#","");
                StringTokenizer itr = new StringTokenizer(subList, ";");
                HashMap<String, Double> prMap = new HashMap<String, Double>();
                while(itr.hasMoreTokens()){
                    String[] element = itr.nextToken().split(":");
                    prMap.put(element[0], Double.parseDouble(element[1]));
                }

                HashMap<String, Double> labelMap = new HashMap<>();
                for(String element: lableList){     //每个element为所传入的"label#FromNode"对
                    String[] tokens = element.split("#");
                    String label = tokens[0], personName = tokens[1];
                    if(name_label_Map.containsKey(personName)) label = name_label_Map.get(personName);  //得到异步更新结果
                    if(prMap.containsKey(personName) == false) continue;
                    Double pr = prMap.get(personName);  //节点间的关联度

                    if(labelMap.containsKey(label) == false) labelMap.put(label, pr);
                    else labelMap.put(label, labelMap.get(label)+pr);
                }

                ArrayList<String> maxLabel = new ArrayList<>();
                Double maxPr = 0.0;
                for(Map.Entry<String, Double> entry: labelMap.entrySet()){
                    if(entry.getValue() > maxPr) {
                        maxLabel.clear();
                        maxLabel.add(entry.getKey());
                        maxPr = entry.getValue();
                    }
                    else if(entry.getValue() == maxPr){
                        maxLabel.add(entry.getKey());
                    }
                }
                int index = new Random().nextInt(maxLabel.size());  //多个最大值相同时，随机选择一个
                String newLabel = maxLabel.get(index);
                if(newLabel.equals(prevLabel) == false) ++changeCount;
                String outList = maxLabel.get(index)+list;
                context.write(new Text(key.toString()+"#"+key_pr), new Text(outList));
                name_label_Map.put(key.toString(), newLabel);  //保存已更新的label，用于异步迭代
            }
        }

        @Override
        protected void cleanup(Context context){
            name_label_Map.clear();
//            System.out.println("---------------------LabelCount:"+labelCount.toString()+"-------------------");
            changeRate = changeCount/labelCount;
            System.out.println("-------------------------------"+changeRate.toString()+"--------------------------");
        }
    }

    public static void main(String[] args){
        Configuration conf = new Configuration();
        try {
            Job job2 = Job.getInstance(conf, "LPAIteration");

            job2.setJarByClass(LPAIteration.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setMapperClass(LPAIterationMapper.class);
            job2.setReducerClass(LPAIterationReducer.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);
        }
        catch (Exception e){e.printStackTrace();}
    }
}
