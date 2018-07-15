package lpa;

public class LPADriver {
    private static int times; // 设置迭代次数
    private static final double threshold = 0.01;


    public static void main(String[] args) throws Exception {
        if(args.length != 3){
            System.err.println("Usage: LabelPropagation.jar <inPath> <outPath> <cycleNumber>");
            System.exit(2);

        }
        times = Integer.parseInt(args[2]);
        String[] forGB = { "", args[1] + "/Data0" };
        forGB[0] = args[0];
        LPAInit.main(forGB);

        String[] forItr = { "", ""};  //把总的label数,即节点数传给迭代过程
        int i;
        for (i = 0; i < times; i++) {
            forItr[0] = args[1] + "/Data" + i;
            forItr[1] = args[1] + "/Data" + String.valueOf(i + 1);
            System.out.println("------------------------------The "+String.valueOf(i+1)+"th Interval--------------------------");
            LPAIteration.main(forItr);
        }


        String[] forRV = { args[1] + "/Data" + i, args[1] + "/FinalRank" };
        LPAReorganize.main(forRV);
    }
}
