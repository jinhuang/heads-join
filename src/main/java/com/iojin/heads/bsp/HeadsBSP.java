package com.iojin.heads.bsp;

import com.iojin.heads.common.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.*;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class HeadsBSP extends
        BSP<Text, Text, Text, Text, VectorWritable>{

    private Configuration conf;
    private String query;
    private int paraK = 0;
    private double paraThreshold = 0.0d;

    private boolean master;
    private int assignmentId = -1;
    private boolean nativeDone = false;
    private int sentFlag = 0;
    private int numRecord = 0;
    private boolean sendDone = false;
    private boolean finalDone = false;
    private TreeSet<JoinedPair> joinedPairs;
    private double beforeComputationThreshold = Double.MAX_VALUE;

    private int dimension;
    private int numBins;
    private int numVectors;
    private int numGrid;
    private int errorLength;
    //	private int numDual;
    private double[] bins;
    private double[] vectors;
    private double[] projectedBins;
    private EmdFilter filter;
    private int seenSendDone = 0;
    private int seenFinal = 0;

    private long sentStep = 0;

    Map<String, Double[][]> cellError = new HashMap<String, Double[][]>();
    Map<String, Double[]> cellRubner = new HashMap<String, Double[]>();
    Map<String, Double[]> cellDual = new HashMap<String, Double[]>();
    Map<Integer, List<String>> cellWorkload = new HashMap<Integer, List<String>>(); // assigmentId, cellId
    Map<String, Integer> cellPeer = new HashMap<String, Integer>(); // peerName, assignmentId
    private QuantileGrid[] grids;
    private double[] t;

    private static final double PEERACK_MSG = -1.0d;
    private static final double THRESHOLD_MSG = 0.0d;
    private static final double DATA_MSG = 1.0d;
    private static final double FINAL_MSG = 2.0d;
    private static final double SENDACK_MSG = 3.0d;
    private static final double FINALACK_MSG = 4.0d;
    private static final double DATA_BATCH_MSG = 5.0d;

    public static final String QUERY = "emd.join.query";
    public static final String PARAK = "emd.join.para.k";
    public static final String DIMENSION = "emd.join.para.dimension";
    public static final String PARATHRESHOLD = "emd.join.para.threshold";
    public static final String NUMTASK = "emd.join.num.task";
    public static final String NUMBIN = "emd.join.num.bin";
    public static final String NUMVEC = "emd.join.num.vector";
    public static final String NUMINTERVAL = "emd.join.num.interval";
    public static final String NUMDUAL = "emd.join.num.dual";
    public static final String NUMGRID = "emd.join.num.grid";
    public static final String LENGTHERROR = "emd.join.length.error";
    public static final String PATHBIN = "emd.join.path.bin";
    public static final String VALBIN = "emd.join.val.bin";
    public static final String PATHVEC = "emd.join.path.vector";
    public static final String VALVEC = "emd.join.val.vector";
    public static final String PATHIN = "emd.join.path.in";
    public static final String PATHPREIN = "emd.join.path.preparein";
    public static final String PATHCELL = "emd.join.path.cell";
    public static final String PATHGRID = "emd.join.path.grid";
    public static final String PATHOUT = "emd.join.path.out";

    public static final String CACHED = "emd.join.cached";
    public static final String MSG_BATCH = "emd.join.batch";

    //	public static final Log LOG = LogFactory.getLog(NormalBSP.class);
    public static long joinTimer = 0;
    public static long filterTimer = 0;
    public static long constructTimer = 0;
    public static long computeTimer = 0;

    // if cached
    private List<DoubleVector> cache;
    private List<String> cacheCell;

    private List<Double[][]> records;
    private List<Double[][]> errors;

    // batch message
    private DoubleVector[] currentBatch;
    private boolean[][] filterFlag;

    private int msgCount = 0;

    @Override
    public final void setup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
            throws IOException, InterruptedException {
//		LOG.info("initiating peer " + peer.getPeerIndex());
        master = peer.getPeerIndex() == peer.getNumPeers() / 2;
        conf = peer.getConfiguration();
        query = conf.get(QUERY);
        paraK = conf.getInt(PARAK, 0);
//        System.out.println("HEADS DEBUG: query " + conf.get(HeadsBSP.QUERY));
//        System.out.println("HEADS DEBUG: parak " + conf.get(HeadsBSP.PARAK));
        if (query.equalsIgnoreCase("distance")) {
            paraThreshold = Double.valueOf(conf.get(PARATHRESHOLD));
        }
        dimension = conf.getInt(DIMENSION, 0);
        numBins = conf.getInt(NUMBIN, 0);
        numVectors = conf.getInt(NUMVEC, 0);
        numGrid = conf.getInt(NUMGRID, 0);
//		numDual = conf.getInt(NUMDUAL, 0);
        grids = new QuantileGrid[numVectors];


       // System.out.println(System.getProperty("java.class.path"));

//        String binName = FileUtils.getNameFromPath(binPath);
//        String vectorName = FileUtils.getNameFromPath(vectorPath);
//        bins = FileUtils.getFromDistributedCache(conf, binName, numBins * dimension);
//        vectors = FileUtils.getFromDistributedCache(conf, vectorName, numVectors * dimension);
        bins = FormatUtils.toDoubleArray(conf.get(VALBIN));
        vectors = FormatUtils.toDoubleArray(conf.get(VALVEC));
        projectedBins = HistUtils.projectBins(bins, dimension, vectors, numVectors);

        t = new double[numVectors * 2];
        for (int i = 0; i < numVectors; i++) {
            double[] eachProjection = FormatUtils.getNthSubArray(projectedBins, numBins, i);
            t[i * 2] += HistUtils.getMinIn(eachProjection) - HistUtils.avg(eachProjection);
            t[i * 2 + 1] += HistUtils.getMaxIn(eachProjection) - HistUtils.avg(eachProjection);
        }

        filter = new EmdFilter(dimension, bins, vectors, null);

        joinedPairs = new TreeSet<JoinedPair>();

        readCell();
        readGrid();

        if (conf.getBoolean(CACHED, true)) {
            this.cache = new ArrayList<DoubleVector>();
            this.cacheCell = new ArrayList<String>();
        }

        errorLength = conf.getInt(LENGTHERROR, 0);

        records = new ArrayList<Double[][]>();
        errors = new ArrayList<Double[][]>();

        currentBatch = new DoubleVector[conf.getInt(MSG_BATCH, 1)];
        filterFlag = new boolean[peer.getNumPeers()][conf.getInt(MSG_BATCH, 1)];

        System.out.println("HEADS DEBUG: done with setting up peer " + peer.getPeerIndex());

//		LOG.info("starting up peer " + peer.getPeerIndex());
    }

    @Override
    public void bsp(
            BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
            throws IOException, SyncException, InterruptedException {
        //System.out.println("HEADS DEBUG: started bsp procedure");
        while(true) {
//			LOG.info("peer " + peer.getPeerIndex() + " sc: " + peer.getSuperstepCount());
            System.out.println("HEADS DEBUG: to begin superstep " + peer.getSuperstepCount());
            process(peer);
            System.out.println("HEADS DEBUG: to sync at superstep " + peer.getSuperstepCount());
            peer.sync();
            System.out.println("HEADS DEBUG: seenSendDone " + seenSendDone + " seenFinal " + seenFinal);
            if (seenSendDone == peer.getNumPeers() && seenFinal == peer.getNumPeers()) {
                if (master) {
                    mergeResult(peer);
                }
                break;
            }
        }
    }

    @Override
    public final void cleanup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        if (master) {
            int numPrint = 0;
            if (query.equalsIgnoreCase("rank")) {
                numPrint = paraK;
            }
            else if (query.equalsIgnoreCase("distance")) {
                numPrint = joinedPairs.size();
            }
            if ((joinedPairs.size() >= paraK && query.equalsIgnoreCase("rank")) || query.equalsIgnoreCase("distance")) {
                for (int i = 0; i < numPrint; i++) {
                    JoinedPair pair = joinedPairs.pollFirst();
//					peer.write(new Text(String.valueOf(i)), new Text(pair.toString()));
                    peer.write(new Text(""), new Text(pair.getRid() + " " + pair.getSid()));
                }
            }
            else peer.write(new Text("Error"), new Text(String.valueOf(beforeComputationThreshold)));
        }
        double sec = joinTimer / 1000000000.0d;
        peer.write(new Text(peer.getPeerName()), new Text("Guest Join: " + sec + " seconds"));
//		LOG.info("Guest join takes " + sec + " seconds");
        sec = filterTimer / 1000000000.0d;
        peer.write(new Text(peer.getPeerName()), new Text("Filtering Join: " + sec + " seconds"));
//		LOG.info("Filter takes " + sec + " seconds");
        sec = constructTimer / 1000000000.0d;
        peer.write(new Text(peer.getPeerName()), new Text("Constructing in Filtering: " + sec + " seconds"));
        sec = computeTimer / 1000000000.0d;
        peer.write(new Text(peer.getPeerName()), new Text("Computing in Filtering: " + sec + " seconds"));
        peer.write(new Text(peer.getPeerName()), new Text("Final message sent at step " + sentStep));
        System.out.println("HEADS DEBUG: number of messages " + msgCount);
    }

//    public static BSPJob createJob(Configuration configuration, Path in, Path out) throws IOException {
//        HamaConfiguration conf = new HamaConfiguration(configuration);
//        BSPJob job = new BSPJob(conf, HeadsBSP.class);
//        job.setJobName("Normal Top-k Join");
//        job.setJarByClass(HeadsBSP.class);
//        job.setBspClass(HeadsBSP.class);
//        job.setInputPath(in);
//        job.setOutputPath(out);
//        job.setInputFormat(KeyValueTextInputFormat.class);
//        job.setOutputFormat(TextOutputFormat.class);
//        job.setInputKeyClass(VectorWritable.class);
//        job.setInputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        return job;
//    }

    public static YARNBSPJob createJob(HamaConfiguration configuration, Path in, Path out) throws IOException {
        YARNBSPJob job = new YARNBSPJob(configuration);
        job.setBspClass(HeadsBSP.class);
        job.setJobName("Normal Join");
        job.setJarByClass(HeadsBSP.class);
        job.setMemoryUsedPerTaskInMb(500);
        job.setInputPath(in);
        job.setOutputPath(out);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setInputKeyClass(VectorWritable.class);
        job.setInputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    public void mergeResult(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        VectorWritable each;
        while((each = peer.getCurrentMessage()) != null) {
            if (each.getVector().get(0) == FINAL_MSG) {
                double[] array = each.getVector().toArray();
                JoinedPair pair = BSPUtils.toJoinedPair(array);
                joinedPairs.add(pair);
            }
        }
    }

    public void process(BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
            throws IOException {
        //System.out.println("HEADS DEBUG: begin to process");
        if (nativeDone) {
            guestJoin(peer);
        }
        else {
            nativeJoin(peer);
            // NoNodeException
//            peer.reopenInput();
            //System.out.println("HEADS DEBUG: done native join");
        }
        //System.out.println("HEADS DEBUG: done join");
        if (sentFlag == numRecord && !sendDone) {
            sendDone = true;
            sendSeenMessage(peer);
        }
        else if(sentFlag == numRecord && sendDone && !finalDone) {
            finalDone = true;
            sendFinalMessage(peer);
        }
        else {
            sendMessage(peer);
        }
       // System.out.println("HEADS DEBUG: done sending messages");

//		peer.write(new Text(String.valueOf(peer.getSuperstepCount())), new Text("Send: " + seenSendDone + " ; Final " + seenFinal));
//		LOG.info(peer.getSuperstepCount() + ": send " + seenSendDone + ", final " + seenFinal);
    }

    private void sendMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {

        int numFilter = 0;

        if (cellPeer.isEmpty()) {
            double[] tmp = getPeerAckVector(peer);
            for (String other : peer.getAllPeerNames()) {
                if (!other.equals(peer.getPeerName())) {
                    peer.send(other, new VectorWritable(new DenseDoubleVector(tmp)));
                    msgCount++;
                }
            }
            cellPeer.put(peer.getPeerName(peer.getPeerIndex()), assignmentId);
        }
        else {
            double[] tmp = getThresholdVector();
            if (tmp != null) {
                DoubleVector thresholdVector = new DenseDoubleVector(tmp);
                for(String other : peer.getAllPeerNames()) {
                    if (!other.equals(peer.getPeerName())) {
                        peer.send(other, new VectorWritable(thresholdVector));
                        msgCount++;
                    }
                }
            }
//            Text key = new Text();
//            Text val = new Text();
            int numToSend = 0;
            for (int i = 0; i < conf.getInt(MSG_BATCH, 1); i++) {
//				if (cache != null && !cache.isEmpty()) {
//                if (sentFlag < numRecord && peer.readNext(key, val)) {
                if (sentFlag < numRecord) {
//                    String[] split = key.toString().split(";");
//                    assignmentId = Integer.valueOf(split[0]);
                    DoubleVector next = cache.get(sentFlag);
                    currentBatch[i] = next;
                    int peerIndex = 0;
                    filterFlag[peerIndex][i] = false;
                    for (String other : peer.getAllPeerNames()) {
                        filterFlag[peerIndex][i] = false;
                        if (!other.equals(peer.getPeerName())) {
                            if (filter(other, next, beforeComputationThreshold, peer)) {
                                filterFlag[peerIndex][i] = true;
                                numToSend++;
                            }
                        }
                        peerIndex++;
                    }
                    sentFlag++;
                }
            }
            // send message here
            int peerIndex = 0;
            for (String other : peer.getAllPeerNames()) {
                if (!other.equals(peer.getPeerName())) {
                    peer.send(other, buildBatchMessage(currentBatch, filterFlag[peerIndex]));
                    msgCount++;
                }
                peerIndex++;
            }
        }
//		LOG.info("peer " + peer.getPeerIndex() + " filtered " + numFilter + " from " + conf.getInt(MSG_BATCH, 1) * (peer.getAllPeerNames().length - 1) + " with threshold " + beforeComputationThreshold + " in step " + peer.getSuperstepCount());

    }

    private VectorWritable buildBatchMessage(DoubleVector[] currentBatch, boolean[] flag) {
        List<Double> array = new ArrayList<Double>();
        int toBatch = 0;
        array.add(DATA_BATCH_MSG);
        for (int i = 0; i < currentBatch.length; i++) {
            if (!flag[i]) {
                if (currentBatch[i] == null) {
                    System.out.println("sentFlag " + sentFlag + " sendDone " + sendDone + " seenFinal " + seenFinal);
                }
                double[] hist = currentBatch[i].toArray();
                for (int j = 0; j < hist.length; j++) {
                    array.add(hist[j]);
                    toBatch ++;
                }
            }
        }
        return new VectorWritable(new DenseDoubleVector(FormatUtils.listToArray(array)));
    }

    private boolean filter(String cellName, DoubleVector data, double threshold, BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
//		return false;
//				if (query.equalsIgnoreCase("distance")) {
//			threshold = paraThreshold;
//		}
        long start = System.nanoTime();
        double[] hist = data.toArray();
        double[] weight = HistUtils.normalizeArray(FormatUtils.getSubArray(hist, 1, hist.length - 1));
        List<String> cellIds = cellWorkload.get(cellPeer.get(cellName));
        double[] record = new double[2];
        int lengthError = conf.getInt(HeadsBSP.LENGTHERROR, 0);
        int numInterval = conf.getInt(HeadsBSP.NUMINTERVAL, 0);
        double[] error = new double[lengthError];

        if (null == cellIds) {
            return true;
        }

        for (String cellId : cellIds) {

            boolean pruned = false;

            // rubner
            double[] rubnerValue = DistanceUtils.getRubnerValue(weight, dimension, bins);
            double rubnerEmd = DistanceUtils.getRubnerBound(rubnerValue, FormatUtils.toDoubleArray(cellRubner.get(cellId)), dimension);
            if (rubnerEmd > threshold) {
                filterTimer += System.nanoTime() - start;
                pruned = true;
            }

            if (!pruned) {
                // emdbr
                String[] eachCellIds = cellId.split(",");
                for (int v = 0; v < numVectors; v++) {
                    long constructStart = System.nanoTime();
                    record = FormatUtils.toDoubleArray(records.get(sentFlag)[v]);
                    error = FormatUtils.toDoubleArray(errors.get(sentFlag)[v]);
                    constructTimer += System.nanoTime() - constructStart;

                    long computeStart = System.nanoTime();
                    int eachId = Integer.valueOf(eachCellIds[v]);
                    double[] bound = grids[v].getGridBound(eachId);
                    Direction direction = grids[v].locateRecordToGrid(record, bound);
                    double emdBr = grids[v].getEmdBr(record, error, bound, FormatUtils.toDoubleArray(cellError.get(cellId)[v]), direction,numInterval);
                    if (emdBr > threshold) {
                        computeTimer += System.nanoTime() - computeStart;
                        pruned = true;
                        break;
                    }
                    computeTimer += System.nanoTime() - computeStart;
                }
            }

            // if both lower bounds failed to prune then the message shall be sent
            if (!pruned) {
                filterTimer += System.nanoTime() - start;
                return false;
            }
        }
        filterTimer += System.nanoTime() - start;
        return true;
    }


    private void sendFinalMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        for(JoinedPair pair : joinedPairs) {
            double[] each = BSPUtils.toDoubleArray(pair);
            DoubleVector finalVector = new DenseDoubleVector(getFinalVector(each));
            String theMaster = peer.getPeerName(peer.getNumPeers() / 2);
            peer.send(theMaster, new VectorWritable(finalVector));
        }

        for (String other : peer.getAllPeerNames()) {
            peer.send(other, new VectorWritable(new DenseDoubleVector(getFinalAckVector())));
        }
//			LOG.info("peer " + peer.getPeerIndex() + " sent final message at step " + peer.getSuperstepCount());
        sentStep = peer.getSuperstepCount();
    }

    private void sendSeenMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        DoubleVector seenVector = new DenseDoubleVector(getSendDoneVector());
        for (String other : peer.getAllPeerNames()) {
            if (!other.equals(peer.getPeerName())) {
                peer.send(other, new VectorWritable(seenVector));
            }
        }
        seenSendDone++;
    }

    private void nativeJoin(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        long start = System.nanoTime();
        //System.out.println("HEADS DEBUG: start native join");
        join(readInput(peer));
        nativeDone = true;
        //System.out.println("HEADS DEBUG: native join done");
//		LOG.info("native join takes " + (System.nanoTime() - start)/1000000 + "ms");
    }

    private void guestJoin(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        long start = System.nanoTime();
//        List<DoubleVector> nativeHist = readInput(peer);
        List<DoubleVector> nativeHist = cache;
        List<DoubleVector> guestHist = new ArrayList<DoubleVector>();
        double threshold = readMessage(peer, guestHist);
        pruneLocalPairs(threshold);
//		if (guestHist.isEmpty()) {
//			receiveDone = true;
//		}
        System.out.println("HEADS DEBUG: guest joining " + guestHist.size());
        if (!guestHist.isEmpty()){
            join(nativeHist, guestHist, threshold, peer);
        }
        joinTimer += System.nanoTime() - start;
    }

    private void join(List<DoubleVector> histograms) {
        double threshold = 0.0d;
        if (query.equalsIgnoreCase("rank")) {
            threshold = getLocalThreshold();
        }
        else if (query.equalsIgnoreCase("distance")) {
            threshold = paraThreshold;
        }
        for (int i = 0; i < histograms.size(); i++) {
            double[] recordA = histograms.get(i).toArray();
            long rid = (long) recordA[0];
            double[] weightA = HistUtils.normalizeArray(FormatUtils.getSubArray(recordA, 1, recordA.length - 1));
            for (int j = i + 1; j < histograms.size(); j++) {
                double[] recordB = histograms.get(j).toArray();
                long sid = (long) recordB[0];
                double[] weightB = HistUtils.normalizeArray(FormatUtils.getSubArray(recordB, 1, recordB.length - 1));
                if (rid == sid) continue;
                if (!filter.filter(weightA, weightB, threshold)) {
                    double emd = DistanceUtils.getEmdLTwo(weightA, weightB, dimension, bins);
                    if (emd < threshold || (query.equalsIgnoreCase("distance") && (emd - threshold) <= DistanceUtils.EPSILON )) {
                        JoinedPair pair = new JoinedPair(rid, sid, emd);
                        joinedPairs.add(pair);
                        if (query.equalsIgnoreCase("rank")) {
                            if (joinedPairs.size() > paraK) {
                                joinedPairs.pollLast();
                            }
                            threshold = getLocalThreshold();
                        }
                    }
                }

            }
//            System.out.println("HEADS DEBUG: joined " + i);
        }
        System.out.println("HEADS DEBUG: joined " + histograms.size() + " with " + threshold + " results " + joinedPairs.size());
    }

    private void join(List<DoubleVector> nat, List<DoubleVector> gus, double t, BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        double threshold = 0.0d;
        if (query.equalsIgnoreCase("rank")) {
            threshold = getLocalThreshold(t);
        }
        else if (query.equalsIgnoreCase("distance")) {
            threshold = paraThreshold;
        }
        int before = joinedPairs.size();
        for (int i = 0; i < nat.size(); i++) {
            double[] recordA = nat.get(i).toArray();
            long rid = (long) recordA[0];
            double[] weightA = HistUtils.normalizeArray(FormatUtils.getSubArray(recordA, 1, recordA.length - 1));
            for (int j = 0; j < gus.size(); j++) {
                double[] recordB = gus.get(j).toArray();
                long sid = (long) recordB[0];
                double[] weightB = HistUtils.normalizeArray(FormatUtils.getSubArray(recordB, 1, recordB.length - 1));
                if (rid == sid) continue;
                if ((rid + sid) % 2 == 0 && rid < sid
                        || (rid + sid) % 2 == 1 && rid > sid) {
                    if (!filter.filter(weightA, weightB, threshold)) {
                        double emd = DistanceUtils.getEmdLTwo(weightA, weightB, dimension, bins);
//						peer.write(new Text("join "), new Text(rid + " " + sid + " : " + emd));
                        if (emd < threshold|| (query.equalsIgnoreCase("distance") && (emd - threshold) <= DistanceUtils.EPSILON )) {
                            JoinedPair pair = new JoinedPair(rid, sid, emd);
                            joinedPairs.add(pair);
                            if (query.equalsIgnoreCase("rank")) {
                                if (joinedPairs.size() > paraK) {
                                    joinedPairs.pollLast();
                                }
                                threshold = getLocalThreshold(t);
                            }
                        }
                    }
                }
            }
        }
        int extra = joinedPairs.size() - before;
        System.out.println("HEADS DEBUG: joined " + nat.size() + " and " + gus.size() + " with " + threshold + " results " + extra);
    }

    private List<DoubleVector> readInput(BSPPeer<Text, Text,  Text, Text, VectorWritable> peer) throws IOException {
        //System.out.println("HEADS DEBUG: read input");
        if (cache != null && cache.isEmpty() || cache == null) {
            List<DoubleVector> histograms = new ArrayList<DoubleVector>();
            Text key = new Text();
            Text val = new Text();
            while (peer.readNext(key, val)) {
                String[] split = key.toString().split(";");
                assignmentId = Integer.valueOf(split[0]);
                DoubleVector each = BSPUtils.toDoubleVector(split[2]);
                histograms.add(each);
                if (cache != null) {
                    cache.add(each.deepCopy());
                }
                if (cacheCell != null) {
                    cacheCell.add(split[1]);
                }
                String[] eachRecordAndError = val.toString().split(",");
                Double[][] records = new Double[numVectors][2];
                Double[][] errors = new Double[numVectors][errorLength];
                for(int i = 0; i < numVectors; i++) {
                    double[] recordAndError = FormatUtils.toDoubleArray(eachRecordAndError[i]);
                    records[i][0] = recordAndError[0];
                    records[i][1] = recordAndError[1];
                    for (int j = 0; j < errorLength; j++) {
                        errors[i][j] = recordAndError[2 + j];
                    }
                }
                this.records.add(records);
                this.errors.add(errors);
            }

            numRecord = histograms.size();
            //System.out.println("HEADS DEBUG: done reading input " + numRecord);
            return histograms;
        }
        else return cache;
    }

    private double readMessage(BSPPeer<Text, Text,  Text, Text, VectorWritable> peer,
                               List<DoubleVector> guestHist) throws IOException {
//		double threshold = getLocalThreshold();
        double threshold = beforeComputationThreshold;
        VectorWritable each;
        DoubleVector hist;
        int eachLength = numBins + 1;
        while((each = peer.getCurrentMessage()) != null) {
            if (each.getVector().get(0) == THRESHOLD_MSG) {
                double eachThreshold = each.getVector().get(1);
                threshold = eachThreshold < threshold ? eachThreshold : threshold;
            }
            else if (each.getVector().get(0) == DATA_MSG){
//				hist = each.getVector().slice(1, each.getVector().getDimension()); 
                // commented out for HAMA-0.7.0-SNAPSHOT, where the DoubleVector.slice(start, end)
                // both start and end are inclusive, different from HAMA-0.6.3, where start is
                // inclusive and end is exclusive
                hist = each.getVector().slice(1, each.getVector().getDimension() - 1);
                guestHist.add(hist.deepCopy());
            }
            else if (each.getVector().get(0) == FINAL_MSG) {
                if (master) {
                    double[] array = each.getVector().toArray();
                    JoinedPair pair = BSPUtils.toJoinedPair(array);
                    joinedPairs.add(pair);
                }

            }
            else if (each.getVector().get(0) == FINALACK_MSG) {
                seenFinal++;
//				LOG.info("peer " + peer.getPeerIndex() + " " + seenFinal);
            }
            else if (each.getVector().get(0) == SENDACK_MSG) {
                seenSendDone++;
            }
            else if (each.getVector().get(0) == PEERACK_MSG) {
                double[] array = each.getVector().toArray();
                int eachId = (int) array[1];
                String name = peer.getPeerName((int) array[2]);
                cellPeer.put(name, eachId);
            }
            else if (each.getVector().get(0) == DATA_BATCH_MSG) {
                int number = (each.getVector().getLength() - 1) / eachLength;
                for (int i = 0; i < number; i++) {
                    hist = each.getVector().slice(1 + i * eachLength, eachLength + i * eachLength);
                    guestHist.add(hist.deepCopy());
                }
            }
        }
        beforeComputationThreshold = threshold;
        return threshold;
    }

    private double getLocalThreshold() {
        double threshold = Double.MAX_VALUE;
        if (joinedPairs.size() == paraK) {
            threshold = joinedPairs.last().getDist();
        }
        return threshold;
    }

    private double getLocalThreshold(double g) {
        double threshold = g;
        if (joinedPairs.size() == paraK) {
            double local = joinedPairs.last().getDist();
            threshold = local < g ? local : g;
        }
        return threshold;
    }

    private void pruneLocalPairs(double threshold) {
        if (query.equalsIgnoreCase("rank")) {
            int toRemove = 0;
            for (JoinedPair pair : joinedPairs) {
                if (pair.getDist() > threshold) {
                    toRemove++;
                }
            }
            for (int i = 0; i < toRemove; i++) {
                joinedPairs.pollLast();
            }
        }
    }

    private double[] getPeerAckVector(BSPPeer<Text, Text,  Text, Text, VectorWritable> peer) {
        double[] result = new double[3];
        result[0] = PEERACK_MSG;
        result[1] = assignmentId;
        result[2] = peer.getPeerIndex();
        return result;
    }

    private double[] getThresholdVector() {
        if (joinedPairs.isEmpty()) return null;
        double threshold = joinedPairs.last().getDist();
        if (joinedPairs.size() == paraK && threshold < beforeComputationThreshold) {
            double[] vector = new double[2];
            vector[0] = THRESHOLD_MSG;
            vector[1] = threshold;
            return vector;
        }
        else return null;
    }

    private double[] getDataVector(DoubleVector hist) {
        double[] data = hist.toArray();
        double[] vector = new double[data.length + 1];
        vector[0] = DATA_MSG;
        for (int i = 1; i <= data.length; i++) {
            vector[i] = data[i - 1];
        }
        return vector;
    }

    private double[] getFinalVector(double[] each) {
        double[] result = new double[each.length + 1];
        result[0] = FINAL_MSG;
        for (int i = 1; i < result.length; i++) {
            result[i] = each[i - 1];
        }
        return result;
    }

    private double[] getSendDoneVector() {
        double[] tmp = new double[1];
        tmp[0] = SENDACK_MSG;
        return tmp;
    }

    private double[] getFinalAckVector() {
        double[] tmp = new double[1];
        tmp[0] = FINALACK_MSG;
        return tmp;
    }

    private void readCell() throws IOException {
        String path = conf.get(HeadsBSP.PATHCELL);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        int numVectors = conf.getInt(HeadsBSP.NUMVEC, 0);
        int errorLength = conf.getInt(HeadsBSP.LENGTHERROR, 0) + 1;
        while((line = reader.readLine()) != null) {
            String[] split = line.split(";");
            String cell = split[0];
            int assignmentId = Integer.valueOf(split[1]);
            if (!cellWorkload.containsKey(assignmentId)) {
                cellWorkload.put(assignmentId, new ArrayList<String>());
            }
            cellWorkload.get(assignmentId).add(cell);
            Double[][] error = new Double[numVectors][errorLength];
            for (int i = 0; i < numVectors; i++) {
                error[i] = FormatUtils.toObjectDoubleArray(split[2 + i]);
            }
            cellError.put(cell, error);
            Double[] rubner = FormatUtils.toObjectDoubleArray(split[2 + numVectors]);
            cellRubner.put(cell, rubner);
            Double[] dual = FormatUtils.toObjectDoubleArray(split[2 + numVectors + 1]);
            cellDual.put(cell, dual);
        }
    }

    private void readGrid() throws IOException {
        String path = conf.get(HeadsBSP.PATHGRID);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        int i = 0;
        while((line = reader.readLine()) != null) {
            String[] split = line.split(";");
            double[] domain = FormatUtils.toDoubleArray(split[1]);
            double[] slope = FormatUtils.toDoubleArray(split[2]);
            double[] swQ = FormatUtils.toDoubleArray(split[3]);
            double[] seQ = FormatUtils.toDoubleArray(split[4]);
            grids[i] = new QuantileGrid(domain, slope, numGrid, swQ, seQ);
            i++;
        }
    }

    public static void main(String[] args) {
        int a = Integer.valueOf(String.valueOf(Math.round(3 * 100.0/4)));
        System.out.println(a);
    }
}
