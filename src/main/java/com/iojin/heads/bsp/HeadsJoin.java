package com.iojin.heads.bsp;

import com.iojin.heads.common.*;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.KeyValueTextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.YARNBSPJob;
import org.apache.hama.commons.io.VectorWritable;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class HeadsJoin {

    private static final String HOST = "w00:8032";
    private static final String MEMORY = "3376";

    private static int numInterval = 5;
    private static int lengthError = 2 * numInterval + 1;
    private static int numDuals = 10;
    private static int numGrid = 2;
    private static Percentile percentile = new Percentile();
    private static Random random = new Random();

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        if (args.length != 13) {
            System.out.println("USAGE: <QUERY> <NUM_TASK> <PARAK (PARATHRESHOLD)> <DIMENSION> <NUM_BIN> <NUM_VECTOR> <NUM_GRID> <INPUT_PATH> <BIN_PATH> <VECTOR_PATH> <OUTPUT_PATH> <CACHED> <BATCH>");
            return;
        }
        HamaConfiguration conf = new HamaConfiguration();
//        conf.set("mapred.child.java.opts", "-Xmx8192M");
        Path out = new Path(args[10]);
        FileSystem fs = FileSystem.get(conf);

        FileUtils.deleteIfExistOnHDFS(conf, args[10]);
//        DistributedCache.addCacheFile(new URI(args[8]),conf);
//        DistributedCache.addCacheFile(new URI(args[9]),conf);

        conf.set(HeadsBSP.QUERY, args[0]);

        conf.setInt(HeadsBSP.NUMTASK, Integer.valueOf(args[1]));
        // to support both top-k and distance join
        if (args[0].equalsIgnoreCase("rank")) {
            conf.setInt(HeadsBSP.PARAK, Integer.valueOf(args[2]));
        }
        else if (args[0].equalsIgnoreCase("distance")) {
            conf.set(HeadsBSP.PARATHRESHOLD, args[2]);
        }
        conf.setInt(HeadsBSP.DIMENSION, Integer.valueOf(args[3]));
        conf.setInt(HeadsBSP.NUMBIN, Integer.valueOf(args[4]));
        conf.setInt(HeadsBSP.NUMVEC, Integer.valueOf(args[5]));
        conf.setInt(HeadsBSP.NUMINTERVAL, numInterval);
        conf.setInt(HeadsBSP.NUMDUAL, numDuals);

        conf.setInt(HeadsBSP.LENGTHERROR, lengthError);
        conf.set(HeadsBSP.PATHIN, args[7]);
        conf.set(HeadsBSP.PATHPREIN, args[7] + File.separator + "prepared");
        Path in = new Path(conf.get(HeadsBSP.PATHPREIN));
        if (fs.isFile(in)) {
            System.out.println("Input should be a directory");
            return;
        }

        numGrid = Integer.valueOf(args[6]);
        conf.setInt(HeadsBSP.NUMGRID, numGrid);

        conf.set(HeadsBSP.PATHCELL, args[7] + File.separator + "cell");
        conf.set(HeadsBSP.PATHGRID, args[7] + File.separator + "grid");
        conf.set(HeadsBSP.PATHBIN, args[8]);
        conf.set(HeadsBSP.PATHVEC, args[9]);
        conf.set(HeadsBSP.PATHOUT, args[10]);
        conf.setBoolean(HeadsBSP.CACHED, Boolean.valueOf(args[11]));
        conf.setInt(HeadsBSP.MSG_BATCH, Integer.valueOf(args[12]));
        conf.set("yarn.resourcemanager.address", HOST);
        conf.set("hama.appmaster.memory.mb", MEMORY);

        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHPREIN));
        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHCELL));
        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHGRID));

//        BSPJob job = HeadsBSP.createJob(conf, in, out);
//        YARNBSPJob job = HeadsBSP.createJob(conf, in, out);
        YARNBSPJob job = new YARNBSPJob(conf);
        job.setJobName("HeadsJoin BSP " + args[0]);
        job.setJarByClass(HeadsBSP.class);
        job.setBspClass(HeadsBSP.class);
        job.setInputPath(in);
        job.setOutputPath(out);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setInputKeyClass(VectorWritable.class);
        job.setInputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMemoryUsedPerTaskInMb(2048);
        job.setNumBspTask(Integer.valueOf(args[1]));
        TimerUtils.start();
        prepareInput(conf);
        System.out.println("BSP done the preparation");
        // apparently the configuration in job does not have all the properties
        setConf(job.getConfiguration(), args);
        job.getConfiguration().set("bsp.child.mem.in.mb", MEMORY);
//        System.out.println("HEADS DEBUG: query " + job.getConfiguration().get(HeadsBSP.QUERY));
        job.waitForCompletion(true);
        TimerUtils.end();
        TimerUtils.print();

        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHPREIN));
        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHCELL));
        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHGRID));
    }

    private static void setConf(Configuration conf, String[] args) {
        conf.set(HeadsBSP.QUERY, args[0]);
        conf.setInt(HeadsBSP.NUMTASK, Integer.valueOf(args[1]));
        if (args[0].equalsIgnoreCase("rank")) {
            conf.setInt(HeadsBSP.PARAK, Integer.valueOf(args[2]));
        }
        else if (args[0].equalsIgnoreCase("distance")) {
            conf.set(HeadsBSP.PARATHRESHOLD, args[2]);
        }
        conf.setInt(HeadsBSP.DIMENSION, Integer.valueOf(args[3]));
        conf.setInt(HeadsBSP.NUMBIN, Integer.valueOf(args[4]));
        conf.setInt(HeadsBSP.NUMVEC, Integer.valueOf(args[5]));
        conf.setInt(HeadsBSP.NUMINTERVAL, numInterval);
        conf.setInt(HeadsBSP.NUMDUAL, numDuals);
        conf.setInt(HeadsBSP.LENGTHERROR, lengthError);
        conf.set(HeadsBSP.PATHIN, args[7]);
        conf.set(HeadsBSP.PATHPREIN, args[7] + File.separator + "prepared");
        conf.setInt(HeadsBSP.NUMGRID, numGrid);
        conf.set(HeadsBSP.PATHCELL, args[7] + File.separator + "cell");
        conf.set(HeadsBSP.PATHGRID, args[7] + File.separator + "grid");
        conf.set(HeadsBSP.PATHBIN, args[8]);
        conf.set(HeadsBSP.PATHVEC, args[9]);
        conf.set(HeadsBSP.PATHOUT, args[10]);
        conf.setBoolean(HeadsBSP.CACHED, Boolean.valueOf(args[11]));
        conf.setInt(HeadsBSP.MSG_BATCH, Integer.valueOf(args[12]));
        conf.set("yarn.resourcemanager.address", HOST);
        conf.set("hama.appmaster.memory.mb", MEMORY);
        conf.set("bsp.tasks.maximum", args[1]);
        conf.set("bsp.max.tasks.per.job", args[1]);
        conf.set("hama.zookeeper.session.timeout", "12000000");
        String binVal = FileUtils.readSingleLine(conf, args[8]);
        String vecVal = FileUtils.readSingleLine(conf, args[9]);
        conf.set(HeadsBSP.VALBIN, binVal);
        conf.set(HeadsBSP.VALVEC, vecVal);
    }

    private static void prepareInput(HamaConfiguration conf) throws IOException {
        List<Double[]> histograms = FileUtils.readMultiFromHDFS(conf, conf.get(HeadsBSP.PATHIN));
        double[] bins = FormatUtils.toDoubleArray(FileUtils.readSingleFromHDFS(conf, conf.get(HeadsBSP.PATHBIN)));
        double[] allVectors = FormatUtils.toDoubleArray(FileUtils.readSingleFromHDFS(conf, conf.get(HeadsBSP.PATHVEC)));
        List<Double[]> vectors = new ArrayList<Double[]>();
        int dimension = conf.getInt(HeadsBSP.DIMENSION, 3);
        int numBins = conf.getInt(HeadsBSP.NUMBIN, 0);
        int numVectors = conf.getInt(HeadsBSP.NUMVEC, 0);
        for (int i = 0; i < numVectors; i++) {
            vectors.add(FormatUtils.toObjectDoubleArray(FormatUtils.getNthSubArray(allVectors, dimension, i)));
        }

        double[][] ms = new double[numVectors][histograms.size()];
        double[][] bs = new double[numVectors][histograms.size()];
        double[][] sws = new double[numVectors][histograms.size()];
        double[][] ses = new double[numVectors][histograms.size()];
        int c = 0;
        double[][] projectedBins = new double[numVectors][numBins];
        double[] tmin = new double[numVectors];
        double[] tmax = new double[numVectors];
        for (int v = 0; v < numVectors; v++) {
            projectedBins[v] = HistUtils.substractAvg(HistUtils.projectBins(bins, dimension, FormatUtils.toDoubleArray(vectors.get(v))));
            tmin[v] = HistUtils.getMinIn(projectedBins[v]);
            tmax[v] = HistUtils.getMaxIn(projectedBins[v]);
        }
//		System.out.println("In Use: " +  Runtime.getRuntime().totalMemory() / 1024.0 / 1024.0 + " ; Free: " + Runtime.getRuntime().freeMemory() / 1024.0 / 1024.0);
        double [][] weight = new double[histograms.size()][numBins];
        double [][] allRubner = new double[histograms.size()][dimension];
        double [][][] error = new double[numVectors][histograms.size()][lengthError];
        // build a grid
        for (Double[] each : histograms) {
            double [] weights = HistUtils.normalizeArray(FormatUtils.getSubArray(FormatUtils.toDoubleArray(each), 1, numBins));
            weight[c] = weights;
            for (int v = 0; v < numVectors; v++) {
                NormalDistributionImpl normal = HistUtils.getNormal(weights, projectedBins[v]);
                TreeMap<Double, Double> cdf = HistUtils.getDiscreteCDFNormalized(weights, projectedBins[v]);
                List<Double> errors = HistUtils.getMinMaxError(normal, cdf, numInterval);
                for (int i = 0; i < errors.size(); i++) {
                    error[v][c][i] = errors.get(i);
                }
                error[v][c][lengthError - 1] = HistUtils.getFullError(normal, cdf, tmin[v], tmax[v]);
                ms[v][c] = 1 / normal.getStandardDeviation();
                bs[v][c] = (-1) * normal.getMean() / normal.getStandardDeviation();
            }
            c++;
        }
        double[][] domain = new double[numVectors][4];
        double[][] slope = new double[numVectors][2];
        Grid[] grid = new Grid[numVectors];
        for (int v = 0; v < numVectors; v++) {
            domain[v][0] = HistUtils.getMinIn(ms[v]);
            domain[v][1] = HistUtils.getMaxIn(ms[v]);
            domain[v][2] = HistUtils.getMinIn(bs[v]);
            domain[v][3] = HistUtils.getMaxIn(bs[v]);
            slope[v][0] = -tmax[v];
            slope[v][1] = -tmin[v];
            grid[v] = new Grid(domain[v], slope[v], numGrid);
        }

        DualBound[] duals = new DualBound[numDuals];
        for (int i = 0; i < numDuals; i++) {
            int pickA = random.nextInt(weight.length);
            int pickB = random.nextInt(weight.length);
            while(pickB == pickA) pickB = random.nextInt(weight.length);
            duals[i] = new DualBound(weight[pickA], weight[pickB], bins, dimension);
        }

        // get projections and build quantile grid
        double [] pt = new double[2];
        for (int i = 0; i < c; i++) {
            for (int v = 0; v < numVectors; v++) {
                pt[0] = ms[v][i];
                pt[1] = bs[v][i];
                double[] dist = grid[v].getProjectionDistanceInGrid(pt);
                sws[v][i] = dist[0];
                ses[v][i] = dist[1];
            }
        }
        double[][] swQ = new double[numVectors][numGrid + 1];
        double[][] seQ = new double[numVectors][numGrid + 1];
        QuantileGrid[] qGrid = new QuantileGrid[numVectors];
        for (int v = 0; v < numVectors; v++) {
            percentile.setData(sws[v]);
            swQ[v][0] = percentile.evaluate(0.001);
            for (int i = 1; i <= numGrid; i++) {
                swQ[v][i] = percentile.evaluate(i * 100.0 / numGrid);
            }
            percentile.setData(ses[v]);
            seQ[v][0] = percentile.evaluate(0.001);
            for (int i = 1; i <= numGrid; i++) {
                seQ[v][i] = percentile.evaluate(i * 100.0 / numGrid);
            }
            qGrid[v] = new QuantileGrid(domain[v], slope[v], numGrid, swQ[v], seQ[v]);
        }

        // aggregate errors
        Map<String, Double[][]> cellError = new HashMap<String, Double[][]>();
        HashMap<String, Integer> cellCount = new HashMap<String, Integer>();
        Map<String, List<Integer>> cellIds = new HashMap<String, List<Integer>>();
        Map<String, Double[]> cellRubner = new HashMap<String, Double[]>();
        Map<String, Double[]> cellDual = new HashMap<String, Double[]>();
        double[] record = new double[2];
        for (int i = 0; i < c; i++) {
            String id = "";
            double[] weights = HistUtils.normalizeArray(FormatUtils.getSubArray(FormatUtils.toDoubleArray(histograms.get(i)), 1, numBins));
            double[] rubnerValue = DistanceUtils.getRubnerValue(weights, dimension, bins);
            double[] dualValue = new double[numDuals];
            for (int m = 0; m < numDuals; m++) {
                dualValue[m] = duals[m].getKey(weights);
            }
            allRubner[i] = rubnerValue;
            for (int v = 0; v < numVectors; v++) {
                record[0] = ms[v][i];
                record[1] = bs[v][i];
                id += String.valueOf(qGrid[v].getGridId(record)) + ",";
            }
            if (!cellError.containsKey(id)) {
                Double[][] each = new Double[numVectors][lengthError + 1];
                for (int v = 0; v < numVectors; v++) {
                    each[v] = new Double[lengthError + 1];
                    for (int j = 0; j < lengthError + 1; j++) {
                        if (j%2 == 0) {
                            each[v][j] = Double.MAX_VALUE;
                        }
                        else {
                            each[v][j]= -Double.MAX_VALUE;
                        }
                    }
                }
                cellError.put(id, each);
            }
            if (!cellCount.containsKey(id)) {
                cellCount.put(id, 0);
            }
            if (!cellIds.containsKey(id)) {
                List<Integer> temp = new ArrayList<Integer>();
                temp.add(i);
                cellIds.put(id, temp);
            }
            else {
                cellIds.get(id).add(i);
            }
            if (!cellRubner.containsKey(id)) {
                Double[] rubner = new Double[dimension * 2];
                for (int m = 0; m < dimension; m++) {
                    rubner[m] = Double.MAX_VALUE;
                    rubner[dimension + m] = -Double.MAX_VALUE;
                }
                cellRubner.put(id, rubner);
            }
            if (!cellDual.containsKey(id)) {
                Double[] dual = new Double[numDuals * 2];
                for (int m = 0; m < numDuals; m++) {
                    dual[m] = Double.MAX_VALUE;
                    dual[m + numDuals] = -Double.MAX_VALUE;
                }
                cellDual.put(id, dual);
            }

            cellCount.put(id, cellCount.get(id) + 1);
            Double[][] each = cellError.get(id);
            for (int v = 0; v < numVectors; v++) {
                for (int j = 0; j < lengthError - 1; j++) {
                    if (j % 2 == 0) {
                        each[v][j] = each[v][j] < error[v][i][j] ? each[v][j] : error[v][i][j];
                    }
                    else {
                        each[v][j] = each[v][j] > error[v][i][j] ? each[v][j] : error[v][i][j];
                    }
                }
                each[v][lengthError - 1] = each[v][lengthError - 1] < error[v][i][lengthError - 1] ? each[v][lengthError - 1] : error[v][i][lengthError - 1];
                each[v][lengthError] = each[v][lengthError] > error[v][i][lengthError - 1] ? each[v][lengthError] : error[v][i][lengthError - 1];
            }
            cellError.put(id, each);

            Double[] eachRubner = cellRubner.get(id);
            for (int m = 0; m < dimension; m++) {
                eachRubner[m] = eachRubner[m] < rubnerValue[m] ? eachRubner[m] : rubnerValue[m];
                eachRubner[m + dimension] = eachRubner[m + dimension] > rubnerValue[m] ? eachRubner[m + dimension] : rubnerValue[m];
            }
            cellRubner.put(id, eachRubner);

            Double[] eachDual = cellDual.get(id);
            for (int m = 0; m < numDuals; m++) {
                eachDual[m] = eachDual[m] < dualValue[m] ? eachDual[m] : dualValue[m];
                eachDual[m + numDuals] = eachDual[m + numDuals] > dualValue[m] ? eachDual[m + numDuals] : dualValue[m];
            }
            cellDual.put(id, eachDual);
        }

        HashMap<String, Integer> assignment = Grid.assignGridInt(cellCount, conf.getInt(HeadsBSP.NUMTASK, 1));
        HashMap<Integer, List<String>> workload = new HashMap<Integer, List<String>>();
        for (String each : assignment.keySet()) {
            if (!workload.containsKey(assignment.get(each))) {
                workload.put(assignment.get(each), new ArrayList<String>());
            }
            workload.get(assignment.get(each)).add(each);
        }

//		for (List<String> cell : workload.values()) {
//			int sum = 0;
//			for (String each : cell) {
//				sum += cellIds.get(each).size();
//			}
//			System.out.println("peer with " + sum + " records");
//		}

        // write out
        FileSystem fs = FileSystem.get(conf);
        // data
        for(Integer id : workload.keySet()) {
            String path = conf.get(HeadsBSP.PATHPREIN) + File.separator + Long.toHexString(Double.doubleToLongBits(Math.random()));
            FileUtils.deleteIfExistOnHDFS(conf, path);
            FSDataOutputStream out = fs.create(new Path(path));
//			out.writeBytes(id + "\n");
            for (String cell : workload.get(id)) {
//				out.writeBytes(cell + "\n");
                for (Integer each : cellIds.get(cell)) {
                    out.writeBytes(id + ";" + cell + ";" + FormatUtils.toTextString(histograms.get(each)) + "\t");
                    for (int v = 0; v < numVectors; v++) {
                        out.writeBytes(ms[v][each] + " " + bs[v][each] + " " + FormatUtils.toTextString(error[v][each]) + ",");
                    }
                    out.writeBytes("\n");
                }
            }
            out.flush();
            out.close();
        }
        // cell error, rubner, dual
        FileUtils.deleteIfExistOnHDFS(conf, conf.get(HeadsBSP.PATHCELL));
        FSDataOutputStream cellOut = fs.create(new Path(conf.get(HeadsBSP.PATHCELL)));
        for(String cell : cellError.keySet()) {
            cellOut.writeBytes(cell + ";" + assignment.get(cell) + ";");
            for (Double[] eachError : cellError.get(cell)) {
                cellOut.writeBytes(FormatUtils.toTextString(eachError) + ";");
            }
            cellOut.writeBytes(FormatUtils.toTextString(cellRubner.get(cell)) + ";");
            cellOut.writeBytes(FormatUtils.toTextString(cellDual.get(cell)) + "\n");
        }
        cellOut.flush();
        cellOut.close();
        // grid
        FSDataOutputStream gridOut = fs.create(new Path(conf.get(HeadsBSP.PATHGRID)));
        for(int i = 0; i < numVectors; i++) {
            gridOut.writeBytes(i + ";" + FormatUtils.toTextString(domain[i]) + ";" +
                    FormatUtils.toTextString(slope[i]) + ";" +
                    FormatUtils.toTextString(swQ[i]) + ";" +
                    FormatUtils.toTextString(seQ[i]) + "\n");
        }
        gridOut.flush();
        gridOut.close();
    }

}
