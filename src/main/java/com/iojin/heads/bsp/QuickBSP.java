package com.iojin.heads.bsp;

import java.io.IOException;
import java.util.*;

import com.iojin.heads.common.BSPUtils;
import com.iojin.heads.common.DistanceUtils;
import com.iojin.heads.common.EmdFilter;
import com.iojin.heads.common.FileUtils;
import com.iojin.heads.common.FormatUtils;
import com.iojin.heads.common.HistUtils;
import com.iojin.heads.common.JoinedPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.*;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.io.VectorWritable;

public class QuickBSP extends
        BSP<Text, Text, Text, Text, VectorWritable>{
    public static final String QUERY = "emd.join.query";
    public static final String PARAK = "emd.join.para.k";
    public static final String DIMENSION = "emd.join.para.dimension";
    public static final String PARATHRESHOLD = "emd.join.para.threshold";
    public static final String NUMBIN = "emd.join.num.bin";
    public static final String VALBIN = "emd.join.val.bin";
    public static final String VALVEC = "emd.join.val.vector";
    public static final String PATHIN = "emd.join.path.in";
    public static final String PATHOUT = "emd.join.path.out";
    public static final String NUMDUAL = "emd.join.num.dual";
    public static final String VALPIVOTS = "emd.join.val.pivot";

    public static final String CACHED = "emd.join.cached";
    public static final String MSG_BATCH = "emd.join.batch";

    private static final int SAMPLE_SIZE = 10;

    private static final double THRESHOLD_MSG = 0.0d;
    private static final double DATA_MSG = 1.0d;
    private static final double FINAL_MSG = 2.0d;
    private static final double FINALACK_MSG = 3.0d;

    private static String query;

    private static double localThreshold = Double.MAX_VALUE;
    private static boolean localThresholdUpdate = false;
    private static int paraK;
    private static double paraThreshold;
    private static int dimension;
    private static int numBin;
    private static int numPivot;
    private static double[] bins;
    private static double[] vectors;
    private static boolean cacheEnabled;
    private static double globalBound;

    private static int numRecord;
    private static int numSent;
    private static int numBatch;

    private static double[][] pivots;
    private List<DoubleVector> cache;
    private List<DoubleVector> assignedHist;
    private TreeSet<JoinedPair> joinedPairs;

    private EmdFilter filter;

    private boolean inputExhausted = false;
    private int numInputExhausted = 0;
    private boolean jobDone = false;
    private boolean isMaster = false;

    private int msgCount = 0;

    // partition to task map
    private static HashMap<String, String> partitionTaskerMap;
    // task list
    private static HashSet<String> toSendTaskers;

    @Override
    public final void setup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        HamaConfiguration conf = (HamaConfiguration) peer.getConfiguration();
        query = conf.get(QUERY);
        paraK = conf.getInt(PARAK, 0);
        paraThreshold = Double.valueOf(conf.get(PARATHRESHOLD));
        globalBound = Double.valueOf(conf.get(PARATHRESHOLD));
        localThreshold = paraThreshold;
        dimension = conf.getInt(DIMENSION, 2);
        numBin = conf.getInt(NUMBIN, 16);
        cacheEnabled = conf.getBoolean(CACHED, true);
        bins = FormatUtils.toDoubleArray(conf.get(VALBIN));
        vectors = FormatUtils.toDoubleArray(conf.get(VALVEC));
        double[] allPivots = FormatUtils.toDoubleArray(conf.get(VALPIVOTS));
        numPivot = allPivots.length / numBin;
        pivots = new double[numPivot][numBin];
        for (int i = 0; i < numPivot; i++) {
            for (int j = 0; j < numBin; j++) {
                pivots[i][j] = allPivots[i * numBin + j];
            }
        }

        if (cacheEnabled) {
            cache = new ArrayList<DoubleVector>();
        }

        filter = new EmdFilter(dimension, bins, vectors, null);
        joinedPairs = new TreeSet<JoinedPair>();
        assignedHist = new ArrayList<DoubleVector>();

        isMaster = peer.getPeerIndex() == peer.getNumPeers() / 2;

        numBatch = conf.getInt(MSG_BATCH, 10);

        partitionTaskerMap = new HashMap<String, String>();
        toSendTaskers = new HashSet<String>();

//        if (query.equalsIgnoreCase("distance")) {
            initiateThreshold(peer);
//        }
        System.out.println("HEADS DEBUG: about to assign partitions " + peer.getPeerName());
        assignPartitions(peer);
        System.out.println("HEADS DEBUG: set up peer " + peer.getPeerName());
    }

    @Override
    public void bsp(BSPPeer<Text, Text, Text, Text, VectorWritable> peer)
            throws IOException, SyncException, InterruptedException {
        while(true) {
            process(peer);
            peer.sync();
            if (isMaster && inputExhausted && numInputExhausted == peer.getNumPeers()) {
                mergeResult(peer);
            }
            if (jobDone) {
                break;
            }
            System.out.println("HEADS DEBUG: superstep " + (peer.getSuperstepCount() - 1) + " synchronized");
        }
    }

    @Override
    public final void cleanup(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        if (isMaster) {
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
                    peer.write(new Text(String.valueOf(i)), new Text(pair.toString()));
                }
            }

//			else peer.write(new Text("Error"), new Text(String.valueOf(beforeComputationThreshold)));
        }
        System.out.println("HEADS DEBUG: number of messages " + msgCount);
    }

    private List<DoubleVector> readInput(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        if (cache != null && cache.isEmpty() || cache == null) {
            numRecord = 0;
            List<DoubleVector> histograms = new ArrayList<DoubleVector>();
            Text key = new Text();
            Text val = new Text();
            while (peer.readNext(key, val)) {
                DoubleVector each = BSPUtils.toDoubleVector(key);
                histograms.add(each);
                if (cache != null) {
                    cache.add(each.deepCopy());
                }
                numRecord++;
            }
            // if (cache == null) {
            // 	peer.reopenInput();
            // }
            return histograms;
        }
        else return cache;
    }

    private void initiateThreshold(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        List<DoubleVector> sample = readInput(peer).subList(0, SAMPLE_SIZE);
        join(sample);
    }

    private void assignPartitions(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) {
        HashSet<String> partitions = new HashSet<String>();
        for (int i = 0; i < numPivot; i++) {
            partitions.add(String.valueOf(i));
        }
        for (int i = 0; i < numPivot; i++) {
            for(int j = i + 1; j < numPivot; j++) {
                partitions.add(formatMarginPartition(i, j));
            }
        }

        int index = 0;
        String[] peers = sortNames(peer.getAllPeerNames());
        for (String partition : partitions) {
            partitionTaskerMap.put(partition, peers[index]);
            index++;
            index = index % peers.length;
        }
    }

    private String[] sortNames(String[] names) {
        List<String> list = new ArrayList<String>();
        for (String each : names) {
            list.add(each);
        }
        Collections.sort(list);
        String[] result = new String[names.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    private void process(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        System.out.println("HEADS DEBUG: started superstep " + peer.getSuperstepCount() + " on peer " + peer.getPeerName());
        // read messages
        List<DoubleVector> receivedHist = new ArrayList<DoubleVector>();
        readMessage(peer, receivedHist);

        localThresholdUpdate = false;


        // compute
        //join(assignedHist, receivedHist, localThreshold, peer);
        assignedHist.addAll(receivedHist);
        System.out.println("HEADS DEBUG: in processing superstep " + peer.getSuperstepCount() + " on peer " + peer.getPeerName() + " assigned # " + assignedHist.size() + " threshold " + paraThreshold);
        join(assignedHist, receivedHist, localThreshold, peer);
        System.out.println("HEADS DEBUG: numSent " + numSent + " numRecord " + numRecord + " assigned " + assignedHist.size() + " receieved " + receivedHist.size());
        // send messages
        if (localThresholdUpdate) {
            sendTresholdMsg(peer);
        }
        if (numSent < numRecord) {
            for (int i = 0; i < numBatch; i++) {
                if (cacheEnabled && numSent < numRecord) {
                    DoubleVector msg = new DenseDoubleVector(getDataVector(cache.get(numSent)));
                    findTaskers(peer, cache.get(numSent));
                    for (String tasker : toSendTaskers) {
                        peer.send(tasker, new VectorWritable(msg));
                        msgCount ++;
                    }
                    numSent++;
                }
            }
        }
        if (numSent == numRecord && !inputExhausted) {
            inputExhausted = true;
            numInputExhausted++;
            for (String other : peer.getAllPeerNames()) {
                if (!other.equalsIgnoreCase(peer.getPeerName())) {
                    peer.send(other, new VectorWritable(new DenseDoubleVector(getFinalAckVector())));
                    msgCount++;
                }
            }
        }
        else if (inputExhausted && numInputExhausted == peer.getNumPeers()) {
            if (!isMaster) {
                for(JoinedPair pair : joinedPairs) {
                    double[] each = BSPUtils.toDoubleArray(pair);
                    DoubleVector finalVector = new DenseDoubleVector(getFinalVector(each));
                    String theMaster = peer.getPeerName(peer.getNumPeers() / 2);
                    peer.send(theMaster, new VectorWritable(finalVector));
                    msgCount++;
                }
                jobDone = true;
            }
        }
        System.out.println("HEADS DEBUG: done superstep (message sending) " + peer.getSuperstepCount() + " on peer " + peer.getPeerName() + " with joined " + joinedPairs.size());
    }

    private void findTaskers(BSPPeer<Text, Text, Text, Text, VectorWritable> peer, DoubleVector hist) {
//		LOG.info("find taskers for " + peer.getPeerName() + " with threshold " + localThreshold);
        toSendTaskers.clear();
        double minEmd = Double.MAX_VALUE;
        int minIndex = 0;
        int pIndex = 0;
        HashMap<Integer, Double> idDistanceMap = new HashMap<Integer, Double>();
        // find the nearest pivot
        for (double[] pivot : pivots) {
            double[] record = hist.toArray();
            double[] weight = HistUtils.normalizeArray(FormatUtils.getSubArray(record, 1, record.length - 1));
            double emd = DistanceUtils.getEmdLTwo(pivot, weight, dimension, bins);
            idDistanceMap.put(pIndex, emd);
            if (emd < minEmd) {
                minEmd = emd;
                minIndex = pIndex;
            }
            pIndex++;
        }
        minEmd = globalBound;
        toSendTaskers.add(partitionTaskerMap.get(String.valueOf((int)minIndex)));
        // check whether it is in any margin partitions
        for (int i = 0; i < numPivot; i++) {
            if (i != minIndex) {
                double marginLowerBound = (idDistanceMap.get(i) - minEmd) / 2;
                if (marginLowerBound <= localThreshold) {
                    String partition = i < minIndex ? formatMarginPartition(i, minIndex) : formatMarginPartition(minIndex, i);
                    String tasker = partitionTaskerMap.get(partition);
                    toSendTaskers.add(tasker);
                }
            }
        }
    }



    private void readMessage(BSPPeer<Text, Text, Text, Text, VectorWritable> peer,
                             List<DoubleVector> receivedHist) throws IOException {
        VectorWritable each;
        DoubleVector hist;
        while((each = peer.getCurrentMessage()) != null) {
            if (each.getVector().get(0) == THRESHOLD_MSG) {
                double eachThreshold = each.getVector().get(1);
                localThreshold = localThreshold > eachThreshold ? eachThreshold : localThreshold;
            }
            else if (each.getVector().get(0) == DATA_MSG) {
                hist = each.getVector().slice(1, each.getVector().getDimension() - 1);
                receivedHist.add(hist);
            }
            else if (each.getVector().get(0) == FINAL_MSG) {
                if (isMaster) {
                    double[] array = each.getVector().toArray();
                    JoinedPair pair = BSPUtils.toJoinedPair(array);
                    joinedPairs.add(pair);
                }
            }
            else if (each.getVector().get(0) == FINALACK_MSG) {
                numInputExhausted++;
            }
        }
    }

    private void sendTresholdMsg(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        double[] vector = new double[2];
        vector[0] = THRESHOLD_MSG;
        vector[1] = localThreshold;
        DoubleVector thresholdVector = new DenseDoubleVector(vector);
        for(String other : peer.getAllPeerNames()) {
            if (!other.equals(peer.getPeerName())) {
                peer.send(other, new VectorWritable(thresholdVector));
            }
        }
    }

    private void mergeResult(BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        VectorWritable each;
        while((each = peer.getCurrentMessage()) != null) {
            if (each.getVector().get(0) == FINAL_MSG) {
                double[] array = each.getVector().toArray();
                JoinedPair pair = BSPUtils.toJoinedPair(array);
                joinedPairs.add(pair);
            }
        }
        jobDone = true;
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

                if (!filter.filter(weightA, weightB, threshold)) {
                    double emd = DistanceUtils.getEmdLTwo(weightA, weightB, dimension, bins);
                    if (emd < threshold|| (query.equalsIgnoreCase("distance") && (emd - threshold) <= DistanceUtils.EPSILON )) {
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
        }
        if (query.equalsIgnoreCase("rank")) {
            if ((localThreshold - threshold) > 0.0001 ) {
                localThreshold = threshold;
                localThresholdUpdate = true;
            }
        }
    }

    private void join(List<DoubleVector> nat, List<DoubleVector> gus, double t, BSPPeer<Text, Text, Text, Text, VectorWritable> peer) throws IOException {
        double threshold = 0.0d;
        if (query.equalsIgnoreCase("rank")) {
            threshold = getLocalThreshold(t);
        }
        else if (query.equalsIgnoreCase("distance")) {
            threshold = paraThreshold;
        }
        System.out.println("HEADS DEBUG: join using " + threshold);
        for (int i = 0; i < nat.size(); i++) {
            double[] recordA = nat.get(i).toArray();
            long rid = (long) recordA[0];
            double[] weightA = HistUtils.normalizeArray(FormatUtils.getSubArray(recordA, 1, recordA.length - 1));
            for (int j = 0; j < gus.size(); j++) {
                double[] recordB = gus.get(j).toArray();
                long sid = (long) recordB[0];
                double[] weightB = HistUtils.normalizeArray(FormatUtils.getSubArray(recordB, 1, recordB.length - 1));
//				if ((rid + sid) % 2 == 0 && rid < sid
//						|| (rid + sid) % 2 == 1 && rid > sid) {
                if (rid < sid) {
                    if (!filter.filter(weightA, weightB, threshold)) {
                        double emd = DistanceUtils.getEmdLTwo(weightA, weightB, dimension, bins);

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
//                    if (rand.nextInt(90000) % 89999 == 0) {
//                        double emd = DistanceUtils.getEmdLTwo(weightA, weightB, dimension, bins);
//                        if (emd < threshold|| (query.equalsIgnoreCase("distance") && (emd - threshold) <= DistanceUtils.EPSILON )) {
//                            JoinedPair pair = new JoinedPair(rid, sid, emd);
//                            joinedPairs.add(pair);
//
//                            if (query.equalsIgnoreCase("rank")) {
//                                if (joinedPairs.size() > paraK) {
//                                    joinedPairs.pollLast();
//                                }
//                                threshold = getLocalThreshold(t);
//                            }
//                        }
//                    }
                }
            }
        }
        if (query.equalsIgnoreCase("rank")) {
            if ((localThreshold - threshold) > 0.0001 ) {
                localThreshold = threshold;
                localThresholdUpdate = true;
            }
        }
    }

    private double getLocalThreshold() {
//        double threshold = Double.MAX_VALUE;
        double threshold = globalBound;
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

    private double[] getFinalAckVector() {
        double[] tmp = new double[1];
        tmp[0] = FINALACK_MSG;
        return tmp;
    }

    private String formatMarginPartition(int pivotA, int pivotB) {
        return String.valueOf(pivotA) + ";" + String.valueOf(pivotB);
    }
}
