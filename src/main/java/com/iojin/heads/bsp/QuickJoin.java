package com.iojin.heads.bsp;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import com.iojin.heads.common.DistanceUtils;
import com.iojin.heads.common.FileUtils;
import com.iojin.heads.common.FormatUtils;
import com.iojin.heads.common.TimerUtils;

import com.sun.xml.bind.v2.model.annotation.Quick;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.YARNBSPJob;
import org.apache.hama.commons.io.VectorWritable;

public class QuickJoin {
    private static int numDuals = 10;

    private static final String HOST = "w00:8032";
    private static final String MEMORY = "3376";

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        if (args.length != 13) {
            System.out.println("USAGE: <QUERY> <NUMTASK> <PARAK/THRESHOLD> <DIMENSION> <NUMBIN> <NUMVEC> <INPUTPATH> <BINPATH> <VECPATH> <OUTPUTPATH> <CACHE> <BATCH> <NUMPIVOTS>");
            System.exit(1);
        }

        HamaConfiguration conf = new HamaConfiguration();
        conf.set("hama.appmaster.memory.mb", MEMORY);
        Path in = new Path(args[6]);
        Path out = new Path(args[9]);
        FileUtils.deleteIfExistOnHDFS(conf, args[9]);

        YARNBSPJob job = new YARNBSPJob(conf);
        job.setJobName("QuickJoin BSP " + args[0]);
        job.setJarByClass(QuickBSP.class);
        job.setBspClass(QuickBSP.class);
        job.setInputPath(in);
        job.setOutputPath(out);
        job.setInputFormat(org.apache.hama.bsp.KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setInputKeyClass(VectorWritable.class);
        job.setInputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMemoryUsedPerTaskInMb(2048);
        job.setNumBspTask(Integer.valueOf(args[1]));
        TimerUtils.start();
        setConf(job.getConfiguration(), args);
        if (args[0].equalsIgnoreCase("rank")) {
            double[][] samples = sample(job.getConfiguration(), Integer.valueOf(args[1]));
            int dimension = job.getConfiguration().getInt(QuickBSP.DIMENSION, 2);
            double[] bins = FormatUtils.toDoubleArray(job.getConfiguration().get(QuickBSP.VALBIN));
            job.getConfiguration().set(QuickBSP.PARATHRESHOLD, rankEmd(samples, bins, dimension, Integer.valueOf(args[2])) + "");
        }

        job.getConfiguration().set("bsp.child.mem.in.mb", MEMORY);
        job.getConfiguration().set("hama.appmaster.memory.mb", MEMORY);
        job.waitForCompletion(true);
        TimerUtils.end();
        TimerUtils.print();

    }

    private static double rankEmd(double[][] hists, double[] bin, int dimension, int n) {
        double[] minimalEmds = new double[n];
        for (int i = 0; i < n; i++) {
            minimalEmds[i] = Double.MAX_VALUE;
        }
        for(int i = 0; i < hists.length; i++) {
            for(int j = i + 1; j < hists.length; j++) {
                double emd = DistanceUtils.getEmdLTwo(hists[i], hists[j], dimension, bin);
                if (emd < minimalEmds[n - 1]) {
                    minimalEmds[n - 1] = emd;
                    Arrays.sort(minimalEmds);
                }
            }
        }
        return minimalEmds[n-1];
    }

    private static double[][] sample(Configuration conf, int num)
            throws IOException, InterruptedException {
        Job sampleJob = Job.getInstance(conf, "Sampling");
        sampleJob.setInputFormatClass(KeyValueTextInputFormat.class);
        sampleJob.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(sampleJob, new Path(conf.get(QuickBSP.PATHIN)));
        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler.RandomSampler<Text, Text>(1.0, num, 100);
        Object[] samples = sampler.getSample(
                new KeyValueTextInputFormat(), sampleJob);
        assert(samples.length == num);
        int numBin = Integer.valueOf(conf.get(QuickBSP.NUMBIN));
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

    private static void setConf(Configuration conf, String[] args) throws IOException, InterruptedException {
        conf.set(QuickBSP.QUERY, args[0]);
        if (args[0].equalsIgnoreCase("rank")) {
            conf.setInt(QuickBSP.PARAK, Integer.valueOf(args[2]));
        }
        else if (args[0].equalsIgnoreCase("distance")) {
            conf.set(QuickBSP.PARATHRESHOLD, args[2]);
        }

        conf.setInt(QuickBSP.DIMENSION, Integer.valueOf(args[3]));
        int numBin = Integer.valueOf(args[4]);
        conf.setInt(QuickBSP.NUMBIN, numBin);

        conf.setInt(QuickBSP.NUMDUAL, numDuals);
        conf.set(QuickBSP.PATHIN, args[6]);
        // set VALPIVOT, VALBIN, VALVEC
        String binVal = FileUtils.readSingleLine(conf, args[7]);
        String vecVal = FileUtils.readSingleLine(conf, args[8]);
        conf.set(QuickBSP.VALBIN, binVal);
        conf.set(QuickBSP.VALVEC, vecVal);

        conf.set(QuickBSP.PATHOUT, args[9]);
        conf.setBoolean(QuickBSP.CACHED, Boolean.valueOf(args[10]));
        conf.setInt(QuickBSP.MSG_BATCH, Integer.valueOf(args[11]));

        int numPivot = Integer.valueOf(args[12]);
        double[][] pivots = sample(conf, numPivot);
        double[] allPivots = new double[numPivot *numBin];
        for (int i = 0; i < numPivot; i++) {
            for (int j = 0; j < numBin; j++) {
                allPivots[i * numBin + j] = pivots[i][j];
            }
        }
        String pivotVal = FormatUtils.formatDoubleArray(allPivots);
        conf.set(QuickBSP.VALPIVOTS, pivotVal);
        conf.set("yarn.resourcemanager.address", HOST);
        conf.set("hama.appmaster.memory.mb", MEMORY);
        conf.set("bsp.tasks.maximum", args[1]);
        conf.set("bsp.max.tasks.per.job", args[1]);
        conf.set("hama.zookeeper.session.timeout", "12000000");
    }
}
