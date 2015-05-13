package com.iojin.heads.single;

import com.iojin.heads.common.*;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class Join {

    public static void run(String[] args) {
        Random R = new Random();
        int numHist = Integer.valueOf(args[0]);
        int numBin = Integer.valueOf(args[1]);
        int dimension = Integer.valueOf(args[2]);
        int rank = Integer.valueOf(args[3]);
        double[][] hists = new double[numHist][numBin];
        for (int i = 0; i < numHist; i++) {
            double[] hist = new double[numBin];
            double sum = 0.0d;
            for (int j = 0; j < numBin; j++) {
                hist[j] = R.nextInt(1000);
                sum += hist[j];
            }
            for (int j = 0; j < numBin; j++) {
                hist[j] = hist[j] * 1.0 / sum;
            }
            hists[i] = hist;
        }
        double[] bin = new double[numBin * dimension];
        for (int i = 0; i < numBin * dimension; i++) {
            bin[i] = R.nextInt(100) * 1.0;
        }
        double[] minimals = new double[rank];
        for (int i = 0; i < rank; i++) {
            minimals[i] = Double.MAX_VALUE;
        }

        TimerUtils.start();
        for(int i = 0; i < numHist; i++) {
            Double[] a = FormatUtils.toObjectDoubleArray(hists[i]);
            for (int j = i + 1; j < numHist; j++) {
                Double[] b = FormatUtils.toObjectDoubleArray(hists[j]);
            }
        }
//        Percentile[] ps = new Percentile[6];
//        for (int i = 0; i < 6; i++) {
//            ps[i] = new Percentile();
//        }
//        double[][] data = new double[6][numHist * (numHist - 1) / 2];
//        for(int i = 0; i < 6; i++) {
//            data[i] = new double[numHist * (numHist - 1) / 2];
//        }
//        int count = 0;
//        Random rand = new Random();
//        TreeSet<JoinedPair> set = new TreeSet<JoinedPair>();
//        for (int i = 0; i < rank; i++) {
//            JoinedPair joinedPair = new JoinedPair(rand.nextLong(), rand.nextLong(), Double.MAX_VALUE);
//            set.add(joinedPair);
//        }
//        DualBound[] duals = new DualBound[5];
//        for (int i = 0; i < 5; i++) {
//            duals[i] = new DualBound(hists[i], hists[rand.nextInt(numHist)], bin, dimension);
//        }
//        for (int i = 0; i < numHist; i++) {
//            for (int j = i + 1; j < numHist; j++) {
//
//                double projectEmd = HistUtils.getProjectEmd(hists[i], hists[j], bin, dimension);
//                double maxDual = -Double.MAX_VALUE;
//                for (int k = 0; k < 5; k++) {
//                    double dualEmd = duals[k].getDualEmd(hists[i], hists[j]);
//                    maxDual = maxDual > dualEmd ? maxDual : dualEmd;
//                }
//                double rubnerEmd = DistanceUtils.getRubnerEmd(hists[i], hists[j], dimension, bin, DistanceType.LTWO);
//                double indEmd = DistanceUtils.getIndMinEmd(hists[i], hists[j], dimension, bin, DistanceType.LTWO, null);
//                double maxEmd = Math.max(Math.max(Math.max(projectEmd, maxDual), rubnerEmd), indEmd);
////                double emd = DistanceUtils.getEmdLTwo(hists[i], hists[j], dimension, bin);
//                if (maxEmd < minimals[rank - 1]) {
//                    double emd = DistanceUtils.getEmdLTwo(hists[i], hists[j], dimension, bin);
//                    if (emd < minimals[rank - 1]) {
//                        minimals[rank - 1] = emd;
//                        Arrays.sort(minimals);
//                    }
//                }
////                JoinedPair pair = set.pollLast();
////                if (maxEmd < pair.getDist()) {
////                    double emd = DistanceUtils.getEmdLTwo(hists[i], hists[j], dimension, bin);
////                    if (emd < pair.getDist()) {
////                        JoinedPair newPair = new JoinedPair(rand.nextLong(), rand.nextLong(), emd);
////                        set.add(newPair);
////                    }
////                    else {
////                        set.add(pair);
////                    }
////                } else {
////                    set.add(pair);
////                }
////                data[0][count] = emd;
////                data[1][count] = projectEmd;
////                data[2][count] = dualEmd;
////                data[3][count] = rubnerEmd;
////                data[4][count] = indEmd;
////                data[5][count] = maxEmd;
////                if (emd <= 0) {
////                    System.out.println(FormatUtils.toTextString(hists[i]) + " : " + FormatUtils.toTextString(hists[j]) + " => " + emd);
////                }
//            }
//        }
        TimerUtils.end();
        TimerUtils.print();
//        for (int i = 0; i < 6; i++) {
//            ps[i].setData(data[i]);
//            StringBuilder builder = new StringBuilder();
//            for (int j = 0; j <= 10; j++) {
//                if (j == 0) builder.append(ps[i].evaluate(0.001)).append(" ");
//                else builder.append(ps[i].evaluate(j * 10)).append(" ");
//            }
//            System.out.println("percentile: " + builder.toString().trim());
//        }
    }

}
