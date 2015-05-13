package com.iojin.heads.mr;

import com.iojin.heads.common.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;


public class QuickJoin {
    public static void main(String[] args) throws Exception {

        int parseStatus = parseArgs(args);
        if (parseStatus < 0) {
            System.exit(parseStatus);
        }
        Configuration conf = new Configuration();
        saveArgs(conf, args);
        pivots = sample(conf, numPivot);
        conf.set(PIVOTS, FormatUtils.formatPivots(pivots));
        readSharedData(conf);
        buildDuals(conf, numWrk);
        saveDuals(conf);
        buildReductions();


        QuickJoin join = new QuickJoin();
        int result = ToolRunner.run(conf, join.new QJoin(), null);
        if (result < 0) {
            System.exit(result);
        }
    }

    private static int parseArgs(String[] args) {
        if (args.length < 12) {
            return -1;
        }
        // skip 0
        joinType = args[0];
        joinPred = Double.valueOf(args[1]);
        if (joinType.equalsIgnoreCase("rank")) {
            n = Integer.valueOf(args[1]);
        }
        inputPath = args[2];
        dimension = Integer.valueOf(args[3]);
        binPath = args[4];
        numBin = Integer.valueOf(args[5]);
        projPath = args[6];
        numProj = Integer.valueOf(args[7]);
        splRate = Double.valueOf(args[8]);
        numWrk = Integer.valueOf(args[9]);
        numPivot = Integer.valueOf(args[10]);
        numReduction = numWrk / 4 > 4 ? numWrk / 4 : 4;
        outputPath = args[11];
        return 0;
    }

    private static void saveArgs(Configuration conf, String[] args) {
        StringBuilder builder = new StringBuilder();
        for (String each: args) {
            builder.append(each).append(" ");
        }
        conf.set(ARGS, builder.toString().trim());
    }

    private static void restoreArgs(Configuration conf) {
        String[] args = conf.get(ARGS).split(" ");
        parseArgs(args);
    }

    private static void readSharedData(Configuration conf) throws IOException, InterruptedException {
        bin = FileUtils.readDoubleArray(conf, binPath);
        double[] proj = FileUtils.readDoubleArray(conf, projPath);
        projBin = HistUtils.projectBins(bin, dimension, proj, numProj);
        pivots = FormatUtils.getPivots(conf.get(PIVOTS), numBin);
    }

    private static void buildDuals(Configuration conf, int numSample)
            throws IOException, InterruptedException {
        numDual = (numSample - 1 ) * numSample / 2;
        double[][] samples = sample(conf, numSample);
        int count = 0;
        duals = new DualBound[numDual];
        for (int i = 0; i  < numSample; i++) {
            for (int j = i + 1; j< numSample; j++) {
                duals[count] = new DualBound(samples[i], samples[j], bin, dimension);
                count ++;
            }
        }
        // rank join, compute a join predicate
        if (joinType.equalsIgnoreCase("rank")) {
            double[][] emdSamples = samples;
            if (numDual <= n) {
                emdSamples = sample(conf, n);
            }
            int numEmdSample = numDual <= n ? n : numSample;
            double[] emds = new double[numEmdSample * (numEmdSample - 1) / 2];
            count = 0;

            for (int i = 0; i < numEmdSample; i++) {
                for (int j = i + 1; j < numEmdSample;  j++) {
                    emds[count] = DistanceUtils.getEmdLTwo(
                            emdSamples[i], emdSamples[j], dimension, bin);
                    count ++;
                }
            }
            Arrays.sort(emds);
            joinPred = emds[n - 1];
            conf.set(RANK, joinPred.toString());
        }
    }

    public static void saveDuals(Configuration conf) {
        StringBuilder builder = new StringBuilder();
        for (DualBound bound : duals) {
            builder.append(bound.toString()).append(";");
        }
        builder.deleteCharAt(builder.length() - 1);
        conf.set(DUALS, builder.toString());
    }

    public static void restoreDuals(Configuration conf) {
        String[] dualString = conf.get(DUALS).split(";");
        duals = new DualBound[dualString.length];
        numDual = dualString.length;
        for (int i = 0; i < dualString.length; i++) {
            duals[i] = new DualBound(dualString[i]);
        }
    }

    public static void restoreAll(Configuration conf) throws IOException, InterruptedException {
        restoreArgs(conf);
        readSharedData(conf);
        restoreDuals(conf);
        buildReductions();
        if (joinType.equalsIgnoreCase("rank")) {
            joinPred = Double.valueOf(conf.get(RANK));
        }
    }

    private static void buildReductions() {
        reductions = new ReductionBound[numReduction];
        for (int i = 0; i < numReduction; i++) {
            reductions[i] = new ReductionBound(dimension, REDUCED_DIMENSION, bin);
        }
    }

    private static double[][] sample(Configuration conf, int num)
            throws IOException, InterruptedException {
        Job sampleJob = Job.getInstance(conf, "Sampling");
        sampleJob.setInputFormatClass(KeyValueTextInputFormat.class);
        sampleJob.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(sampleJob, new Path(inputPath));
        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler.RandomSampler<Text, Text>(1.0, num, 100);
        Object[] samples = sampler.getSample(
                new KeyValueTextInputFormat(), sampleJob);
        assert(samples.length == num);
        double[][] result = new double[num][numBin];
        for(int i = 0; i < num; i++) {
            String each = samples[i].toString();
            String[] array = each.split(" ");
            for (int j = 1; j <= numBin; j++) { // the first one is id
                result[i][j-1] = Double.valueOf(array[j]);
            }
        }
        return result;
    }

    private static final String ARGS = "com.iojin.heads.args";
    private static final String DUALS = "com.iojin.heads.duals";
    private static final String RANK = "com.iojin.heads.rank";
    private static final String PIVOTS = "com.iojin.heads.pivots";

    private static String joinType = null;
    private static Double joinPred = null;
    private static int n = 0;
    private static String inputPath = null;
    private static int dimension = 0;
    private static String binPath = null;
    private static int numBin = 0;
    private static double[] bin = null;
    private static String projPath = null;
    private static int numProj = 0;
    private static double splRate = 0.0d;
    private static int numWrk = 0;
    private static int numPivot = 0;
    private static int numDual = 0;
    private static int numReduction = 0;
    private static String outputPath = null;
    private static double[] projBin = null;
    private static DualBound[] duals = null;
    private static ReductionBound[] reductions = null;
    private static double[][] pivots = null;

    private static final int REDUCED_DIMENSION = 8;

    private class QJoin extends Configured implements Tool {

        @Override
        public int run(String[] strings) throws Exception {
            Configuration conf = this.getConf();

            FileUtils.deleteIfExistOnHDFS(conf, outputPath);
            FileUtils.deleteIfExistOnHDFS(conf, outputPath + "_ranked");

            TimerUtils.start();
            Job quickJob = Job.getInstance(conf, "QuickJoin MR1 " + joinType);
            quickJob.setJarByClass(QuickJoin.class);
            quickJob.setMapperClass(QuickMapper.class);
            quickJob.setMapOutputKeyClass(Text.class);
            quickJob.setMapOutputValueClass(Text.class);
            quickJob.setReducerClass(QuickReducer.class);
            quickJob.setOutputKeyClass(LongWritable.class);
//            quickJob.setReducerClass(Reducer.class);
//            quickJob.setOutputKeyClass(Text.class);
            quickJob.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(quickJob, new Path(inputPath));
            FileOutputFormat.setOutputPath(quickJob, new Path(outputPath));
            quickJob.setNumReduceTasks(numWrk);
            quickJob.waitForCompletion(true);

            if (joinType.equalsIgnoreCase("distance"))
                TimerUtils.end();

            if (joinType.equalsIgnoreCase("rank")) {
                Job rankJob = Job.getInstance(conf, "QuickJoin MR2 " + joinType);
                rankJob.setJarByClass(QuickJoin.class);
                rankJob.setMapperClass(Mapper.class);
                rankJob.setReducerClass(RankReducer.class);
                rankJob.setOutputKeyClass(LongWritable.class);
                rankJob.setOutputValueClass(Text.class);
                FileInputFormat.setInputPaths(rankJob, new Path(outputPath));
                FileOutputFormat.setOutputPath(rankJob, new Path(outputPath + "_ranked"));
                rankJob.setNumReduceTasks(1);
                rankJob.waitForCompletion(true);
                TimerUtils.end();
            }

            TimerUtils.print();
            return 0;
        }
    }

    public static class QuickMapper extends Mapper<Object, Text, Text, Text> {

        public QuickMapper(){}

        private double[] rankEmd = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            restoreAll(context.getConfiguration());
            if (joinType.equalsIgnoreCase("rank")) {
                rankEmd = new double[n];
                for (int i = 0; i < n; i++) {
                    rankEmd[i] = Double.MAX_VALUE;
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            long recordId = (long)(FormatUtils.toDoubleArray(value.toString())[0]);
            double[] weights = FormatUtils
                    .getNormalizedDoubleArray(value, 1, numBin);
            double[] pEmds = new double[numPivot];
            int i = 0;
            int min = 0;
            double minEmd = Double.MAX_VALUE;
            for(double[] pivot : pivots) {
                long pivotId = (long) pivot[0];
                double[] pWeights = HistUtils.normalizeArray(pivot);
                pEmds[i] = DistanceUtils.getEmdLTwo(weights, pWeights, dimension, bin);
                if (joinType.equalsIgnoreCase("rank") && pEmds[i] < rankEmd[0]) {
                    rankEmd[n-1] = pEmds[i];
                    Arrays.sort(rankEmd);
                    if (rankEmd[n - 1] < Double.MAX_VALUE) {
                        joinPred = rankEmd[n - 1];
                    }
                }
                if (minEmd > pEmds[i]) {
                    min = i;
                    minEmd = pEmds[i];
                }
                i++;
            }
            List<Text> windows = new ArrayList<Text>();
            for (i =0; i < numPivot; i++) {
                if (i != min && (pEmds[i] - minEmd) < joinPred ) {
                    String part = i < min ? i + " " + min : min + " " + i;
                    windows.add(new Text(part));
                }
            }
            context.write(new Text(String.valueOf(min)), value);
            for (Text each : windows) {
                context.write(each, value);
            }
        }

    }

    public static class QuickReducer extends Reducer<Text, Text, LongWritable, Text> {

        public QuickReducer(){}

        private double[] projection = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            restoreAll(context.getConfiguration());
            projection = new double[numBin];
            if (joinType.equalsIgnoreCase("rank")) {
                joinPred = Double.valueOf(context.getConfiguration().get(RANK));
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Double[]> records = new ArrayList<Double[]>();
            Utils.JoinPairComparator comparator = new Utils.JoinPairComparator();
            for (Text each : values) {
                Double[] record = FormatUtils.getDoubleObjectArray(each.toString(), 0, numBin);
                records.add(record);
            }
            int numRecord = records.size();
            System.out.println("HEADS DEBUG: to block join " + numRecord + " records");
            JoinedPair[] pairs = new JoinedPair[n];
            for (int i = 0; i < numRecord; i++) {
                Double[] recordA = records.get(i);
                for (int j = i + 1; j < numRecord; j++) {
                    Double[] recordB= records.get(j);
                    long ridA = (long) recordA[0].doubleValue();
                    long ridB = (long) recordB[0].doubleValue();
                    if (ridA < ridB) {
                        double emd = Utils.joinRecords(recordA, recordB,
                                numProj, projBin, projection,
                                numBin, bin,
                                dimension, joinPred,
                                numDual, duals, numReduction, reductions);
                        if (emd > 0) {
                            if (joinType.equalsIgnoreCase("rank")) {
                                boolean toSort = false;
                                for (int k = 0; k < n; k++) {
                                    if (pairs[k] == null) {
                                        pairs[k] = new JoinedPair(ridA, ridB, emd);
                                        break;
                                    }
                                }
                                if (pairs[n - 1] != null && emd < pairs[n - 1].getDist()) {
                                    pairs[n - 1] = new JoinedPair(ridA, ridB, emd);
                                    toSort = true;
                                }
                                if (toSort) {
                                    Arrays.sort(pairs, comparator);
                                    joinPred = pairs[n - 1].getDist();
                                }
                            } else {
                                context.write(new LongWritable(ridA), new Text(ridB + " " + emd));
                            }
                        }
                    }
                    context.progress();
                }
            }
            if (joinType.equalsIgnoreCase("rank")) {
                for (int i = 0; i < n; i++) {
                    if (pairs[i] != null) {
                        long ridA = pairs[i].getRid();
                        long ridB = pairs[i].getSid();
                        double emd = pairs[i].getDist();
                        context.write(new LongWritable(ridA), new Text(ridB + " " + emd));
                    }
                }
            }
        }

    }

    public static class RankReducer extends Reducer<Object, Text, LongWritable, Text> {

        public RankReducer(){}

        private List<String> unranked = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            restoreAll(context.getConfiguration());
        }

        @Override
        protected void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text each : values) {
                unranked.add(each.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            JoinedPair[] pairs = new JoinedPair[unranked.size()];
            Set<JoinedPair> set = new HashSet<JoinedPair>();
            Utils.JoinPairComparator comparator = new Utils.JoinPairComparator();
            for (String each: unranked) {
                String[] array = each.trim().split("\\s");
                JoinedPair pair = new JoinedPair(Long.valueOf(array[0]), Long.valueOf(array[1]), Double.valueOf(array[2]));
                set.add(pair);
//                pairs[count] = pair;
            }
            JoinedPair[] pairs = new JoinedPair[set.size()];
            int count = 0;
            for (JoinedPair each : set) {
                pairs[count] = each;
                count++;
            }
            Arrays.sort(pairs, comparator);
            for (int i = 0; i < n; i++) {
                context.write(new LongWritable(pairs[i].getRid()),
                        new Text(pairs[i].getSid() + " " + pairs[i].getDist()));
            }
        }

    }
}
