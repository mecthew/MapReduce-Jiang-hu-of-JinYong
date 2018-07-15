package extract;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;

public class ExtractMapper extends Mapper<Object, Text, Text, Text> {

    private static HashSet<String> nameSet = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException{
        Path nameListPath = new Path(context.getConfiguration().get("nameList", "/data/task2/people_name_list.txt"));
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        fileSystem.open(nameListPath)
                ));
        String name;
        while((name = reader.readLine()) != null) {
            nameSet.add(name);
            UserDefineLibrary.insertWord(name, "nr", 1000);
        }
        reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        StringBuilder stringBuilder = new StringBuilder();
        Result result = ToAnalysis.parse(value.toString());
        List<Term> termList = result.getTerms();
        for(int i=0; i < ((List) termList).size(); i++) {
           // String natureStr = termList.get(i).getNatureStr();
            String name = termList.get(i).getName();
            if(nameSet.contains(name)) {
                if(stringBuilder.length() != 0) {
                    stringBuilder.append(" ");
                }
                stringBuilder.append(termList.get(i).getName());
            }
        }
        /*后面可以考虑优化，只有一个人名时，不传送*/
        if(stringBuilder.length() != 0) {
            Text keyOut = new Text(fileName);
            Text valueOut = new Text(stringBuilder.toString());
            context.write(keyOut, valueOut);
        }
    }
}
