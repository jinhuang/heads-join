package com.iojin.heads.mr;

import com.iojin.heads.common.*;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class HeadsJoin extends Configured implements Tool  {

    public static void main(String[] args) throws Exception {

        int parseStatus = parseArgs(args);
        if (parseStatus < 0) {
            System.exit(parseStatus);
        }
        Configuration conf = new Configuration();
        readSharedData(conf);
        buildDuals(conf, numWrk);
        saveDuals(conf);
        saveArgs(conf, args);
        buildReductions();

        HeadsJoin join = new HeadsJoin();
        int result = ToolRunner.run(conf, new HeadsJoin(), null);
        if (result < 0) {
            System.exit(result);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();

        FileUtils.deleteIfExistOnHDFS(conf, FIRST_OUTPUT);
        FileUtils.deleteIfExistOnHDFS(conf, SECOND_OUTPUT);
        FileUtils.deleteIfExistOnHDFS(conf, outputPath);
        FileUtils.deleteIfExistOnHDFS(conf, outputPath + "_ranked");

        TimerUtils.start();
        Job firstJob = Job.getInstance(conf, "HeadsJoin MR1 " + joinType);
        // add mat
        firstJob.setJarByClass(HeadsJoin.class);
        firstJob.setMapperClass(FirstMapper.class);
        firstJob.setReducerClass(FirstReducer.class);
        firstJob.setMapOutputKeyClass(LongWritable.class);
        firstJob.setMapOutputValueClass(Text.class);
        firstJob.setOutputKeyClass(LongWritable.class);
        firstJob.setOutputValueClass(Text.class);
        firstJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(firstJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(firstJob, new Path(FIRST_OUTPUT));
        firstJob.waitForCompletion(true);

        Job secondJob = Job.getInstance(conf, "HeadsJoin MR2 " + joinType);
        FileUtils.addToCache(secondJob, FIRST_OUTPUT, QUANTILE);
        secondJob.setJarByClass(HeadsJoin.class);
        secondJob.setMapperClass(SecondMapper.class);
        secondJob.setReducerClass(SecondReducer.class);
        secondJob.setMapOutputKeyClass(Text.class);
        secondJob.setMapOutputValueClass(Text.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(Text.class);
        secondJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(secondJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(secondJob, new Path(SECOND_OUTPUT));
        secondJob.waitForCompletion(true);

        Job thirdJob = Job.getInstance(conf, "HeadsJoin MR3 " + joinType);
        FileUtils.addToCache(thirdJob, FIRST_OUTPUT, QUANTILE);
        FileUtils.addToCache(thirdJob, SECOND_OUTPUT, AGGREGATE);
        thirdJob.setJarByClass(HeadsJoin.class);
        thirdJob.setMapperClass(ThirdMapper.class);
        thirdJob.setReducerClass(ThirdReducer.class);
        thirdJob.setMapOutputKeyClass(LongWritable.class);
        thirdJob.setMapOutputValueClass(Text.class);
        thirdJob.setOutputKeyClass(LongWritable.class);
        thirdJob.setOutputValueClass(Text.class);
        thirdJob.setNumReduceTasks(numWrk);
        FileInputFormat.addInputPath(thirdJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(thirdJob, new Path(outputPath));
        thirdJob.waitForCompletion(true);
        if (joinType.equalsIgnoreCase("distance"))
            TimerUtils.end();

        if (joinType.equalsIgnoreCase("rank")) {
            Job fourthJob = Job.getInstance(conf, "HeadsJoin MR4 " + joinType);
            fourthJob.setJarByClass(HeadsJoin.class);
            fourthJob.setMapperClass(Mapper.class);
            fourthJob.setMapOutputKeyClass(LongWritable.class);
            fourthJob.setMapOutputValueClass(Text.class);
            fourthJob.setReducerClass(FourthReducer.class);
            fourthJob.setOutputKeyClass(LongWritable.class);
            fourthJob.setOutputValueClass(Text.class);
            fourthJob.setNumReduceTasks(1);
            FileInputFormat.addInputPath(fourthJob, new Path(outputPath));
            FileOutputFormat.setOutputPath(fourthJob, new Path(outputPath + "_ranked"));
            fourthJob.waitForCompletion(true);
            TimerUtils.end();
        }

        TimerUtils.print();
        return 0;
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
        numErr = Integer.valueOf(args[8]);
        grnu = Integer.valueOf(args[9]);
        numWrk = Integer.valueOf(args[10]);
        numReduction = numWrk / 4 > 4 ? numWrk / 4 : 4;
        outputPath = args[11];
        return 0;
    }

    private static final String ARGS = "com.iojin.heads.args";
    private static final String DUALS = "com.iojin.heads.duals";
    private static final String RANK = "com.iojin.heads.rank";

    private static void saveArgs(Configuration conf, String[] args) {
        StringBuilder builder = new StringBuilder();
        for (String each: args) {
            builder.append(each).append(" ");
        }
        conf.set(ARGS, builder.toString().trim());
        conf.set(RANK, joinPred.toString());
    }

    private static void restoreArgs(Configuration conf) {
        String[] args = conf.get(ARGS).split(" ");
        parseArgs(args);
    }

    private static void readSharedData(Configuration conf) {
        bin = FileUtils.readDoubleArray(conf, binPath);
        double[] proj = FileUtils.readDoubleArray(conf, projPath);
        projBin = HistUtils.projectBins(bin, dimension, proj, numProj);
    }

    private static void buildDuals(Configuration conf, int numWorker)
            throws IOException, InterruptedException {
        int numSample = 10;
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
            references = new ArrayList<Double[]>();
            int numEmdSample = numDual <= n ? n : numSample;
            double[] emds = new double[numEmdSample * (numEmdSample - 1) / 2];
            count = 0;

            for (int i = 0; i < numEmdSample; i++) {
                for (int j = i + 1; j < numEmdSample;  j++) {
                    emds[count] = DistanceUtils.getEmdLTwo(
                            emdSamples[i], emdSamples[j], dimension, bin);
                    count ++;
                }
                references.add(FormatUtils.toObjectDoubleArray(emdSamples[i]));
            }
            Arrays.sort(emds);
            joinPred = emds[n];
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

    public static void restoreAll(Configuration conf) {
        restoreArgs(conf);
        readSharedData(conf);
        restoreDuals(conf);
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
    private static int numErr = 0;
    private static int grnu = 0;
    private static int numWrk = 0;
    private static int numDual = 0;
    private static int numReduction = 0;
    private static String outputPath = null;
    private static double[] projBin = null;
    private static DualBound[] duals = null;
    private static ReductionBound[] reductions = null;
    private static List<Double[]> references = null;

    private static final String FIRST_OUTPUT = "/apps/heads/mr/temp/first";
    private static final String SECOND_OUTPUT =  "/apps/heads/mr/temp/second";
    private static final String QUANTILE = "quantile";
    private static final String AGGREGATE = "aggregate";
    private static final int REDUCED_DIMENSION = 8;


    public static class FirstMapper extends Mapper<Object, Text, LongWritable, Text> {

        //dummy constructor for the reflection
        public FirstMapper(){}

        @Override
        protected void setup(Context context) {
            restoreAll(context.getConfiguration());
        }

        @Override
        protected void map(Object key, Text values, Context context)
                throws IOException, InterruptedException {
            double[] weights = FormatUtils
                    .getNormalizedDoubleArray(values, 1, numBin);

            NormalDistributionImpl[] normals = getNormals(weights);
            double[] ms = getM(normals);
            double[] bs = getB(normals);

            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < numProj; i++) {
                builder.append(i).append(" ");
                builder.append(ms[i]).append(" ");
                builder.append(bs[i]).append(" ");
            }
            context.write(new LongWritable(0),
                    new Text(builder.toString().trim()));
        }
    }

    public static class FirstReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        //dummy constructor for the reflection
        public FirstReducer() {}

        private double[] minM = null;
        private double[] maxM = null;
        private double[] minB = null;
        private double[] maxB = null;
        private List<List<Double>> collectionM = new ArrayList<List<Double>>();
        private List<List<Double>> collectionB = new ArrayList<List<Double>>();
        private List<List<Double>> collectionSW = new ArrayList<List<Double>>();
        private List<List<Double>> collectionSE = new ArrayList<List<Double>>();
        private List<Double> percentile = new ArrayList<Double>();
        private List<Double[]> quantileSW = new ArrayList<Double[]>();
        private List<Double[]> quantileSE = new ArrayList<Double[]>();
        private Percentile statistics = new Percentile();
        private Grid[] grids = null;
        private double[] t;

        @Override
        protected void setup(Context context) throws IOException {
            restoreAll(context.getConfiguration());
            minM = new double[numProj];
            maxM = new double[numProj];
            minB = new double[numProj];
            maxB = new double[numProj];
            grids = new Grid[numProj];
            for(int i = 0; i < numProj; i++) {
                minM[i] = Double.MAX_VALUE;
                maxM[i] = -Double.MAX_VALUE;
                minB[i] = Double.MAX_VALUE;
                maxM[i] = -Double.MAX_VALUE;
                collectionM.add(new ArrayList<Double>());
                collectionB.add(new ArrayList<Double>());
                collectionSW.add(new ArrayList<Double>());
                collectionSE.add(new ArrayList<Double>());
                quantileSE.add(new Double[grnu + 1]);
                quantileSW.add(new Double[grnu + 1]);
            }
            double eachPercentile = 100.0 / grnu;
            for (int i = 0; i < grnu; i++) {
                percentile.add(i * eachPercentile + eachPercentile);
            }
            t = getT();
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                double[] domains = FormatUtils.getDoubleArray(val, 0, numProj * 3 - 1);
                for (int i = 0; i < numProj; i++) {
                    double m = domains[i * 3 + 1];
                    double b = domains[i * 3 + 2];
                    minM[i] = m < minM[i] ? m : minM[i];
                    maxM[i] = m > maxM[i] ? m : maxM[i];
                    minB[i] = b < minB[i] ? b : minB[i];
                    maxB[i] = b > maxB[i] ? b : maxB[i];

                    // for quantile information
                    collectionM.get(i).add(m);
                    collectionB.get(i).add(b);
                }
            }
            // project points
            double [] slopes = new double[2 * numProj];
            for (int i = 0; i < numProj; i++) {
                slopes[i*2] = (-1) * t[i*2 + 1];
                slopes[i*2+1] = (-1) * t[i*2];
            }
            double[] eachDomain = new double[4];
            double[] eachPoint = new double[2];
            for (int i = 0; i < numProj; i++) {
                eachDomain[0] = minM[i];
                eachDomain[1] = maxM[i];
                eachDomain[2] = minB[i];
                eachDomain[3] = maxB[i];
                double[] eachSlopes = FormatUtils.getNthSubArray(slopes, 2, i);
                grids[i] = new Grid(eachDomain, eachSlopes, grnu);

                for (int j = 0; j < collectionM.get(i).size(); j++) {
                    eachPoint[0] = collectionM.get(i).get(j);
                    eachPoint[1] = collectionB.get(i).get(j);
                    double[] eachDistance = grids[i].getProjectionDistanceInGrid(eachPoint);
                    collectionSW.get(i).add(eachDistance[0]);
                    collectionSE.get(i).add(eachDistance[1]);
                }
            }
            // compute the quantile
            for (int i = 0; i < numProj; i++) {

                statistics.setData(FormatUtils.getArrayFromList(collectionSW.get(i)));
                quantileSW.get(i)[0] = statistics.evaluate(0.000001);
                for (int j = 1; j <= grnu; j++) {
                    if (j == grnu) {
                        quantileSW.get(i)[j] = statistics.evaluate(100);
                    }
                    else {
                        quantileSW.get(i)[j] = statistics.evaluate(percentile.get(j-1));
                    }
                }

                statistics.setData(FormatUtils.getArrayFromList(collectionSE.get(i)));
                quantileSE.get(i)[0] = statistics.evaluate(0.000001);
                for (int j = 1; j <= grnu; j++) {
                    if (j == grnu) {
                        quantileSE.get(i)[j] = statistics.evaluate(100);
                    }
                    else {
                        quantileSE.get(i)[j] = statistics.evaluate(percentile.get(j-1));
                    }
                }
            }

            for (int i = 0; i < numProj; i++) {
                String eachRecord = minM[i] + " " + maxM[i] + " " +
                        minB[i] + " " + maxB[i];
                for (int j = 0; j < grnu + 1; j++) {
                    eachRecord += " " + quantileSW.get(i)[j];
                }
                for (int j = 0; j < grnu + 1; j++) {
                    eachRecord += " " + quantileSE.get(i)[j];
                }
                context.write(new LongWritable(i), new Text(eachRecord));
            }
        }
    }

    public static class SecondMapper extends Mapper<Object, Text, Text, Text> {

        //dummy constructor for the reflection
        public SecondMapper(){}

        private Grid[] grids;

        @Override
        protected void setup(Context context) throws IOException {
            restoreAll(context.getConfiguration());
            double[] quantileArray = FileUtils.getMultiFromCache(
                    context, QUANTILE, 1 + (grnu + 3) * 2);
            double[] quantile = FormatUtils.omitVectorId(quantileArray, 1 + (grnu + 3) * 2);
            grids = quantileArrayToGrid(quantile);
        }

        @Override
        protected void map(Object key, Text values, Context context)
            throws IOException, InterruptedException {
            NormalStructure r = processNormal(values, grids);
            double[] keys = new double[duals.length];
            for (int i = 0; i < duals.length; i++) {
                keys[i] = duals[i].getKey(r.weights);
            }
            double[] rubnerKeys = DistanceUtils.getRubnerValue(r.weights,
                    dimension, bin);
            String outVal = FormatUtils.formatDoubleArray(r.error) + " " +
                    FormatUtils.formatDoubleArray(keys) + " " +
                    FormatUtils.formatDoubleArray(rubnerKeys);
            context.write(new Text(r.combination), new Text(outVal));
        }
    }

    public static class SecondReducer extends Reducer<Text, Text, Text, Text> {

        //dummy constructor for the reflection
        public SecondReducer(){}

        private double[] minErrors = null;
        private double[] maxErrors = null;
        private double[] minFullError = null;
        private double[] maxFullError = null;
        private double[] minRubnerKeys = null;
        private double[] maxRubnerKeys = null;
        private double[] minKeys = null;
        private double[] maxKeys = null;

        @Override
        protected void setup(Context context) {
            restoreAll(context.getConfiguration());
            minErrors = new double[numErr * numProj];
            maxErrors = new double[numErr * numProj];
            minFullError = new double[numProj];
            maxFullError = new double[numProj];
            minRubnerKeys = new double[dimension];
            maxRubnerKeys = new double[dimension];
            minKeys = new double[numDual];
            maxKeys = new double[numDual];
        }

        @Override
        protected void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            for (int i = 0; i < numProj; i++) {
                minFullError[i] = Double.MAX_VALUE;
                maxFullError[i] = -Double.MAX_VALUE;

                for (int j = 0; j < numErr; j++) {
                    minErrors[i * numErr + j] = Double.MAX_VALUE;
                    maxErrors[i * numErr + j] = -Double.MAX_VALUE;
                }
            }
            int eachErrorLength = 2 * numErr + 1;

            for (int i = 0; i < dimension; i++) {
                minRubnerKeys[i] = Double.MAX_VALUE;
                maxRubnerKeys[i] = -Double.MAX_VALUE;
            }

            for (int i = 0; i < numDual; i++) {
                minKeys[i] = Double.MAX_VALUE;
                maxKeys[i] = -Double.MAX_VALUE;
            }

            int counter = 0;
            for (Text val : values) {
                String first = val.toString();
                double[] all = FormatUtils
                        .getDoubleArray(first, 0, first.split("\\s").length - 1);
                double[] allErrors = FormatUtils
                        .getSubArray(all, 0, numProj * eachErrorLength - 1);
                double[] keys = FormatUtils
                        .getSubArray(all, numProj * eachErrorLength, numProj * eachErrorLength + numDual - 1);
                double[] rubnerKeys = FormatUtils
                        .getSubArray(all, numProj * eachErrorLength + numDual, all.length - 1);
                for (int i = 0; i < numProj; i++) {
                    double[] errors = FormatUtils
                            .getNthSubArray(allErrors, eachErrorLength, i);
                    for (int j = 0; j < numErr; j++) {
                        int it = i * numErr + j;
                        minErrors[it] = errors[j * 2] < minErrors[it] ? errors[j * 2]
                                : minErrors[it];
                        maxErrors[it] = errors[j * 2 + 1] > maxErrors[it] ? errors[j * 2 + 1]
                                : maxErrors[it];
                    }
                    minFullError[i] = errors[2 * numErr] < minFullError[i] ? errors[2 * numErr]
                            : minFullError[i];
                    maxFullError[i] = errors[2 * numErr] > maxFullError[i] ? errors[2 * numErr]
                            : maxFullError[i];
                }
                counter++;
                for (int i = 0; i < numDual; i++) {
                    minKeys[i] = keys[i] > minKeys[i] ? minKeys[i] : keys[i];
                    maxKeys[i] = keys[i] > maxKeys[i] ? keys[i] : maxKeys[i];
                }

                for (int i = 0; i < dimension; i++) {
                    minRubnerKeys[i] = minRubnerKeys[i] < rubnerKeys[i] ? minRubnerKeys[i] : rubnerKeys[i];
                    maxRubnerKeys[i] = maxRubnerKeys[i] > rubnerKeys[i] ? maxRubnerKeys[i] : rubnerKeys[i];
                }
            }

            StringBuilder builder = new StringBuilder();
            builder.append(counter).append(" ");
            // error length numProj * (numErr * 2 + 2)
            for (int i = 0; i < numProj; i++) {
                double[] eachMinErrors = FormatUtils.getNthSubArray(minErrors, numErr, i);
                double[] eachMaxErrors = FormatUtils.getNthSubArray(maxErrors, numErr, i);
                for (int j = 0; j < numErr; j++) {
                    builder.append(eachMinErrors[j]).append(" ");
                    builder.append(eachMaxErrors[j]).append(" ");
                }
                builder.append(minFullError[i]).append(" ");
                builder.append(maxFullError[i]).append(" ");
            }
            // dual length numDual * 2
            for (int i = 0; i < numDual; i++) {
                builder.append(minKeys[i]).append(" ");
            }
            for (int i = 0; i < numDual; i++) {
                builder.append(maxKeys[i]).append(" ");
            }
            // rubner length dimension * 2
            for (int i = 0; i < dimension; i++) {
                builder.append(minRubnerKeys[i]).append(" ");
            }
            for (int i = 0; i < dimension; i++) {
                builder.append(maxRubnerKeys[i]).append(" ");
            }
            context.write(key, new Text(builder.toString().trim()));
        }
    }

    public static class ThirdMapper extends Mapper<Object, Text, LongWritable, Text> {

        //dummy constructor for the reflection
        public ThirdMapper(){}

        private Grid[] grids;
        private double[] errorArray;
        private HashMap<String, Long> count = new HashMap<String, Long>();
        private HashMap<String, Double[]> dualKeys = new HashMap<String, Double[]>();
        private HashMap<String, Double[]> rubnerKeys = new HashMap<String, Double[]>();
        private HashMap<String, Long> assignment = new HashMap<String, Long>();
        private TreeSet<Candidate> rankTmpCandidates = new TreeSet<Candidate>();
        private TreeSet<Candidate> rankCandidates = new TreeSet<Candidate>();
        private String[] workerCombinations = null;
        private static LongWritable outKey = new LongWritable(0);
        private static Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            restoreAll(context.getConfiguration());
            double[] quantileArray = FileUtils.getMultiFromCache(
                    context, QUANTILE, 1 + (grnu + 3) * 2);
            double[] quantile = FormatUtils.omitVectorId(
                    quantileArray, 1 + (grnu + 3) * 2);
            grids = quantileArrayToGrid(quantile);

            int errorLength = numProj * (numErr * 2 + 2);
            int combLength = numProj + 1 + errorLength + numDual * 2 + dimension * 2;
            int dualStart = numProj + 1 + errorLength;
            int rubnerStart = numProj + 1 + errorLength + numDual * 2;
            double[] aggregateArray = FileUtils.getMultiFromCache(
                    context, AGGREGATE, combLength);
            int numComb = aggregateArray.length / combLength;
            int combErrorLength = numProj + 1 + errorLength;
            errorArray = new double[numComb * combErrorLength];
            for(int i = 0; i < numComb; i++) {
                double[] combArray = FormatUtils.getNthSubArray(aggregateArray, combLength, i);
                String combination = FormatUtils.formatCombination(
                        FormatUtils.getSubArray(combArray, 0, numProj - 1));
                count.put(combination, Math.round(combArray[numProj]));
                System.arraycopy(combArray, 0, errorArray, i * combErrorLength, combErrorLength);
                dualKeys.put(combination, FormatUtils.toObjectDoubleArray(
                        FormatUtils.getSubArray(combArray, dualStart,
                                dualStart + numDual * 2 - 1)));
                rubnerKeys.put(combination, FormatUtils.toObjectDoubleArray(
                        FormatUtils.getSubArray(combArray, rubnerStart,
                                rubnerStart + dimension * 2 - 1)));
            }

            assignment = Grid.assignGrid(count, numWrk);
            workerCombinations = new String[numWrk];
            for (int i = 0; i < numWrk; i++) {
                workerCombinations[i] = "";
            }
        }

        @Override
        protected void map(Object key, Text values, Context context)
                throws IOException, InterruptedException {
            NormalStructure r = processNormal(values, grids);

            if (joinType.equalsIgnoreCase("rank")) {
                TreeSet<Candidate> guest = Grid.getGuestForRank(r.location, r.error, r.combination,
                        joinPred, errorArray, numErr, numProj, grids, r.recordId,
                        duals, dualKeys, r.weights, n, 0, references, bin, dimension, joinPred, rubnerKeys);
                for (Candidate each : guest) {
                    each.setRid(r.recordId);
                    each.setWeights(r.weights);
                    each.setNativeGrids(r.combination);
                }
//                rankTmpCandidates.clear();
//                rankTmpCandidates.addAll(rankCandidates);
//                rankTmpCandidates.addAll(guest);
//                joinPred = Grid.pruneCandidates(rankTmpCandidates, rankCandidates, n, joinPred);
                rankCandidates.addAll(guest);
            } else {
                List<String> guest = Grid.getGuestWithDual(r.location, r.error, r.combination,
                        joinPred, errorArray, numErr, numProj, grids, r.recordId, duals,
                        dualKeys, r.weights, bin, dimension, rubnerKeys);

                String recordString =  r.recordId + " " + FormatUtils.formatDoubleArray(r.weights);
                for (String eachGuest : guest) {
                    if (Utils.shouldDistribute(r.combination, eachGuest)) {
                        int worker = (int) assignment.get(eachGuest).longValue();;
                        if (workerCombinations[worker].length() == 0) {
                            workerCombinations[worker] = recordString + " " + 1 + " " + eachGuest;
                        }
                        else {
                            workerCombinations[worker] += " " + eachGuest;
                        }
                    }
                }

                for (int i = 0; i < numWrk; i++) {
                    if (workerCombinations[i].length() > 0) {
                        context.write(new LongWritable(i), new Text(workerCombinations[i]));
                    }
                }
            }
            // native
            StringBuilder builder = new StringBuilder();
            builder.append(r.recordId).append(" ");
            builder.append(FormatUtils.formatDoubleArray(r.weights)).append(" ");
            builder.append(0).append(" ").append(r.combination);
            context.write(new LongWritable(assignment.get(r.combination)), new Text(builder.toString()));
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("rank")) {
                for (Candidate candidate : rankCandidates) {
                    String recordString = candidate.getRid() + " " + FormatUtils.formatDoubleArray(candidate.getWeights());
                    String out = "";
                    out = recordString + " " + 1 + " " + candidate.getCombination();
                    outVal.set(out);
                    outKey.set(assignment.get(candidate.getCombination()));
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static class ThirdReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        //dummy constructor for the reflection
        public ThirdReducer(){
        }

        private double[] histA = null;
        private double[] histB = null;
        private double[] projection = null;

        @Override
        protected void setup(Context context) {
            restoreAll(context.getConfiguration());
            histA = new double[numBin];
            histB = new double[numBin];
            projection = new double[numBin];
            buildReductions();
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            HashMap<String, Set<Double[]>> nativeRecords = new HashMap<String, Set<Double[]>>();
            HashMap<String, Set<Double[]>> guestRecords = new HashMap<String, Set<Double[]>>();
            int offset = numBin + 2;
            double[] eachCombination = new double[numProj];
            JoinedPair[] pairs = new JoinedPair[n];
            Utils.JoinPairComparator comparator = new Utils.JoinPairComparator();
            int reduceInput = 0;
            for (Text val : values) {
                reduceInput++;
                Double[] record = new Double[numBin + 1];
                double[] array = FormatUtils.toDoubleArray(val.toString());
                for (int i = 0; i < numBin + 1; i++) {
                    record[i] = array[i];
                }
                if (array[numBin + 1] == 0) {
                    // native
                    System.arraycopy(array, offset, eachCombination, 0, numProj);
                    String combination = FormatUtils.formatCombination(eachCombination);
                    if (!nativeRecords.containsKey(combination)) {
                        nativeRecords.put(combination, new HashSet<Double[]>());
                    }
                    HashSet<Double[]> set = (HashSet<Double[]>) nativeRecords.get(combination);
                    set.add(record);
                    nativeRecords.put(combination, set);
                }

                else {
                    // guest
                    int numCombination = (array.length - numBin - 1) / numProj;

                    for (int i = 0; i < numCombination; i++) {
                        for (int j = 0; j < numProj; j++) {
                            eachCombination[j] = array[offset + i * numProj + j];
                        }
                        String combination = FormatUtils.formatCombination(eachCombination);
                        if (!guestRecords.containsKey(combination)) {
                            guestRecords.put(combination, new HashSet<Double[]>());
                        }
                        HashSet<Double[]> set = (HashSet<Double[]>) guestRecords.get(combination);
                        set.add(record);
                        guestRecords.put(combination, set);
                    }

                }
            }
            for (Map.Entry<String, Set<Double[]>> entry : nativeRecords.entrySet()) {
                if (guestRecords.containsKey(entry.getKey())) {
                    for (Double[] nativeRecord : entry.getValue()) {
                        long ridA = (long) nativeRecord[0].doubleValue();

                        for (Double[] guestRecord : guestRecords
                                .get(entry.getKey())) {
                            long ridB = (long) guestRecord[0].doubleValue();
                            if (ridA < ridB) {
                                double emd = Utils.joinRecords(nativeRecord, guestRecord,
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
                                context.progress();
                            }
                        }
                    }
                }
            }
            for (Map.Entry<String, Set<Double[]>> entry : nativeRecords.entrySet()) {
                for (Double[] recordA : entry.getValue()) {
                    long ridA = (long) recordA[0].doubleValue();
                    for (Double[] recordB : entry.getValue()) {
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
                            context.progress();
                        }
                    }
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

    public static class FourthReducer extends Reducer<Object, Text, LongWritable, Text> {

        //dummy constructor for the reflection
        public FourthReducer(){}

        private List<String> unranked = new ArrayList<String>();

        @Override
        protected void setup(Context context) {
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
            JoinedPair[] pairs = new JoinedPair[unranked.size()];
            int count = 0;
            for (String each: unranked) {
                String[] array = each.trim().split("\\s");
                JoinedPair pair = new JoinedPair(Long.valueOf(array[0]), Long.valueOf(array[1]), Double.valueOf(array[2]));
                pairs[count] = pair;
                count ++;
            }
            Arrays.sort(pairs, new Comparator<JoinedPair>() {
                @Override
                public int compare(JoinedPair o1, JoinedPair o2) {
                    double diff = o1.getDist() - o2.getDist();
                    if (diff < 0) return -1;
                    else if (diff > 0) return 1;
                    else return 0;
                }
            });
            for (int i = 0; i < n; i++) {
                context.write(new LongWritable(pairs[i].getRid()),
                        new Text(pairs[i].getSid() + " " + pairs[i].getDist()));
            }
        }

    }

    public static class NormalStructure{

        public NormalStructure(){}

        public double[] error = new double[(numErr * 2 + 1) * numProj];
        public double[] location = new double[2 * numProj];
        public String combination = null;
        public long recordId = 0L;
        public double[] weights = new double[numBin];
    }

    private static double[] getProjection(int i) {
        double[] proj = FormatUtils.getNthSubArray(projBin, numBin, i);
        proj = FormatUtils.subtractAvg(proj);
        return proj;
    }

    private static NormalDistributionImpl[] getNormals(double[] weights) {
        NormalDistributionImpl[] normals = new NormalDistributionImpl[numProj];
        for (int i = 0; i < numProj; i++) {
            double[] proj = getProjection(i);
            normals[i] = HistUtils.getNormal(weights, proj);
        }
        return normals;
    }

    private static double[] getM(NormalDistributionImpl[] normals) {
        double[] ms = new double[numProj];
        for (int i = 0; i < numProj; i++) {
            ms[i] = 1 / normals[i].getStandardDeviation();
        }
        return ms;
    }

    private static double[] getB(NormalDistributionImpl[] normals) {
        double[] bs = new double[numProj];
        for (int i = 0; i < numProj; i++) {
            bs[i] = (-1) * normals[i].getMean() / normals[i].getStandardDeviation();
        }
        return bs;
    }

    private static double[] getT() {
        double[] t = new double[2 * numProj];
        for (int i = 0; i < numProj; i++) {
            double[] eachProjection = FormatUtils.getNthSubArray(projBin, numBin, i);
            t[i * 2] += HistUtils.getMinIn(eachProjection) - FormatUtils.avg(eachProjection);
            t[i * 2 + 1] += HistUtils.getMaxIn(eachProjection) - FormatUtils.avg(eachProjection);
        }
        return t;
    }

    private static double[] getSlopes() {
        double[] slopes = new double[2 * numProj];
        double[] t = getT();
        for (int i = 0; i < numProj; i++) {
            slopes[i * 2] = (-1) * t[i * 2 + 1];
            slopes[i * 2 + 1] = (-1) * t[i * 2];
        }
        return slopes;
    }

    private static QuantileGrid[] quantileArrayToGrid(double[] quantile) {
        QuantileGrid[] grids = new QuantileGrid[numProj];
        double[] slopes = getSlopes();
        for (int i = 0; i < numProj; i++) {
            double[] each = FormatUtils.getNthSubArray(quantile, 4 + 2*(grnu+1), i);
            double[] eachDomain = FormatUtils.getSubArray(each, 0, 3);
            double[] eachSlopes = FormatUtils.getNthSubArray(slopes, 2, i);
            double[] eachSW = FormatUtils.getSubArray(each, 4, 3 + (grnu+1));
            double[] eachSE = FormatUtils.getSubArray(each, 4 + grnu + 1, 3 + 2 * (grnu+1));
            grids[i] = new QuantileGrid(eachDomain, eachSlopes, grnu, eachSW, eachSE);
        }
        return grids;
    }

    private static NormalStructure processNormal(Text values, Grid[] grids) {
        NormalStructure result = new NormalStructure();
        result.recordId = (long)(FormatUtils.toDoubleArray(values.toString())[0]);
        result.weights = FormatUtils
                .getNormalizedDoubleArray(values, 1, numBin);

        NormalDistributionImpl[] normals = getNormals(result.weights);
        double[] ms = getM(normals);
        double[] bs = getB(normals);
        double[] t = getT();
        int eachErrorLength = numErr * 2 + 1;
        long [] gridIds = new long[numProj];
        for (int i = 0; i < numProj; i++) {
            TreeMap<Double, Double> cdf =
                    HistUtils.getDiscreteCDFNormalized(result.weights,
                            getProjection(i));
            List<Double> errors = HistUtils.getMinMaxError(normals[i],
                    cdf, numErr);
            double fullError = HistUtils.getFullError(normals[i], cdf, t[i*2], t[i*2+1]);
            for (int j = 0; j < errors.size(); j++) {
                result.error[i * eachErrorLength + j] = errors.get(j);
            }
            result.error[i * eachErrorLength + errors.size()] = fullError;
            double[] record = new double[2];
            result.location[i * 2] = ms[i];
            result.location[i * 2 + 1] = bs[i];
            record[0] = ms[i];
            record[1] = bs[i];
            gridIds[i] = grids[i].getGridId(record);
        }
        result.combination = FormatUtils.formatCombination(gridIds);
        return result;
    }
}
