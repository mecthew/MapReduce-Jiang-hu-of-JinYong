package pagerank;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRankDriver {
    private static int times; // 设置迭代次数


    public static void main(String[] args) throws Exception {
        if(args.length != 3){
            System.err.println("Usage: PageRanker.jar <inPath> <outPath> <cycleNumber>");
            System.exit(2);

        }
        times = Integer.parseInt(args[2]);
       

        String[] forItr = { "", "" };
        for (int i = 0; i < times; i++) {
			if(i==0)
				forItr[0] = args[0];
			else
				forItr[0] = args[1] + "/Data" + i;
            forItr[1] = args[1] + "/Data" + String.valueOf(i + 1);
            System.out.println("------------------------------The "+String.valueOf(i+1)+"th Interval-------------------------");
            PageRanklter.main(forItr);
        }


        String[] forRV = { args[1] + "/Data" + times, args[1] + "/FinalRank" };
        PageRankViewer.main(forRV);
    }
}
