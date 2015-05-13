package com.iojin.heads.mr;

import com.iojin.heads.common.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by Jin on 1/8/15.
 */
public class Utils {
    public static boolean shouldDistribute(String recordCombination, String combination) {
        double[] recordIds = FormatUtils.toDoubleArray(recordCombination);
        double[] combinationIds = FormatUtils.toDoubleArray(combination);
        double recordSum = HistUtils.sum(recordIds);
        double combinationSum = HistUtils.sum(combinationIds);
        if (recordCombination.equals(combination)) {
            return false;
        }
        if (0 == (int)(recordSum + combinationSum) % 2){
            if (recordCombination.compareTo(combination) > 0) {
                return true;
            }
            else {
                return false;
            }
        }
        else {
            if (recordCombination.compareTo(combination) <= 0) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    public static double joinRecords(Double[] recordA, Double[] recordB,
                             int numProj, double[] projBin, double[] projection,
                             int numBin, double[] bin,
                             int dimension, double joinPred,
                             int numDual, DualBound[] duals,
                             int numReduction, ReductionBound[] reductions)
            throws IOException, InterruptedException {
        double[] histA = new double[numBin];
        double[] histB = new double[numBin];
        long ridA = (long) recordA[0].doubleValue();
        long ridB = (long) recordB[0].doubleValue();
        for (int i = 0; i < recordA.length - 1; i++) {
            histA[i] = recordA[i + 1];
            histB[i] = recordB[i + 1];
        }
        // for cases where histAs or histBs are filled with 0s
        if (HistUtils.sum(histA) == 0.0d || HistUtils.sum(histB) == 0.0d) {
            return Double.MAX_VALUE;
        }
        if (ridA != ridB) { // no need to compute EMD for the same record
            boolean candidate = true;
            // projection
            for (int i = 0; i < numProj; i++) {
                System.arraycopy(projBin, i * numBin, projection, 0, numBin);
                double projectEmd = DistanceUtils.get1dEmd(histA, histB, projection);
                if (projectEmd > joinPred) {
                    candidate = false;
                    break;
                }
            }

            if (!candidate) return -1.0D;
            // dual
            int dualCounter = 0;
            if (dualCounter < numDual) {
                duals[dualCounter] = new DualBound(histA, histB, bin, dimension);
                dualCounter++;
            }
            for (int i = 0; i < dualCounter; i++) {
                if (duals[i].getDualEmd(histA, histB) > joinPred) {
                    candidate = false;
                    break;
                }
            }

            if (!candidate) return -1.0D;
            // reduced
            for (int i = 0; i < numReduction; i++) {
                if (reductions[i].getReducedEmd(histA, histB) > joinPred) {
                    candidate = false;
                    break;
                }
            }

            if (!candidate) return -1.0D;
            // independent minimization
            if (DistanceUtils.getIndMinEmd(histA, histB, dimension, bin,
                    DistanceType.LTWO, null) > joinPred) {
                candidate = false;
            }


            if (candidate) {
                double emd = DistanceUtils.getEmdLTwo(histA, histB, dimension, bin);
                if ( (emd - joinPred) <= DistanceUtils.EPSILON) {
                    return emd;
                }
            }
        }
        return -1.0D;
    }

    public static class JoinPairComparator implements Comparator<JoinedPair> {

        @Override
        public int compare(JoinedPair o1, JoinedPair o2) {
            double diff = o1.getDist() - o2.getDist();
            if (diff < 0) return -1;
            else if (diff > 0) return 1;
            else return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

    }
}
