import build.Builder;
import count.Counter;
import extract.Extracter;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author lw
 */

public class Main {

    private static Integer prInterval = 11;
    private static Integer lpaInterval = 6;

    public static void main(String[] args) {
        //-phase1 or -phase2
        if (args[0].equals("-p1")) {
            if(args.length < 4) {
                System.out.println("Usage: xxx -p1 <in> <out> <name_list>");
                System.exit(-1);
            }

            try {
                Job extractJob = Extracter.getJob(args[1], args[2]+"/name", args[3]);
                extractJob.waitForCompletion(true);

                Job countJob = Counter.getJob(args[2]+"/name", args[2]+"/count");
                countJob.waitForCompletion(true);

                Job buildJob = Builder.getJob(args[2]+"/count", args[2]+"/build");
                buildJob.waitForCompletion(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        else if (args[0].equals("-p2")) {
            String arg2 = args[2];
            if (arg2.charAt(arg2.length() - 1) != '/') arg2 = arg2 + "/";

            try {
                String[] forPR = {args[1], arg2 + "PR", prInterval.toString()};
                pagerank.PageRankDriver.main(forPR);
                //Usage: PageRanker.jar <inPath> <outPath> <cycleNumber>
                //inPath is readNovelOutput，格式name 0.1#n1:w1;n2:w2;n3:w3

                String[] forLPA = {arg2 + "PR/Data" + prInterval.toString(), arg2 + "LPA", lpaInterval.toString()};
                lpa.LPADriver.main(forLPA);
                //Usage: LabelPropagation.jar <inPath> <outPath> <cycleNumber>

            } catch (Exception e) { e.printStackTrace(); }

        }
        else if (args[0].equals("-h")) {
            System.out.println("1. run All phases: bin/hadoop jar xxx.jar <inpath> <outpath_phase1> <name_list> <outpath_phase2>");
            System.out.println("2. run only phase1: bin/hadoop jar xxx.jar -p1 <inpath> <outpath_phase1> <name_list>");
            System.out.println("3. run only phase2: bin/hadoop jar xxx.jar -p2 <outpath_phase1> <outpath_phase2>");
            System.exit(-1);
        }
        else{
            if(args.length < 4) {
                System.out.println("Usage: xxx <inpath> <outpath_phase1> <name_list> <outpath_phase2>");
                System.exit(-1);
            }
            try {
                long time0 = System.currentTimeMillis();
                Job extractJob = Extracter.getJob(args[0], args[1]+"/name", args[2]);
                extractJob.waitForCompletion(true);
                long time1 = System.currentTimeMillis();

                Job countJob = Counter.getJob(args[1]+"/name", args[1]+"/count");
                countJob.waitForCompletion(true);
                long time2 = System.currentTimeMillis();

                Job buildJob = Builder.getJob(args[1]+"/count", args[1]+"/build");
                buildJob.waitForCompletion(true);
                long time3 = System.currentTimeMillis();

                String arg2 = args[3];
                if (arg2.charAt(arg2.length() - 1) != '/') arg2 = arg2 + "/";

                String[] forPR = {args[1]+"/build", arg2 + "PR", prInterval.toString()};
                pagerank.PageRankDriver.main(forPR);
                long time4 = System.currentTimeMillis();
                //Usage: PageRanker.jar <inPath> <outPath> <cycleNumber>
                //inPath is readNovelOutput，格式name 0.1#n1:w1;n2:w2;n3:w3

                String[] forLPA = {arg2 + "PR/Data" + prInterval.toString(), arg2 + "LPA", lpaInterval.toString()};
                lpa.LPADriver.main(forLPA);
                long time5 = System.currentTimeMillis();
                //Usage: LabelPropagation.jar <inPath> <outPath> <cycleNumber>


                long TotalDiff = time5-time0;
                System.out.println(time1-time0);
                System.out.println(time2-time1);
                System.out.println(time3-time2);
                System.out.println(time4-time3);
                System.out.println(time5-time4);
                System.out.println(time5-time0);
            } catch (Exception e) { e.printStackTrace(); }

        }

    }





}
