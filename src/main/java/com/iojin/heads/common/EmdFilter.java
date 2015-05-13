package com.iojin.heads.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EmdFilter {
    private int dimension;
    private double[] bins;
    private int numVectors;
    private List<Double[]> projectedBins;
    private final int numReductions = 4;
    private ReductionBound[] reductions = new ReductionBound[numReductions];
    private int numDuals = 0;
    private DualBound[] duals = new DualBound[numDuals];
    private static Random r = new Random();

    public EmdFilter(int dimension, double[] bins, double[] vectors, List<Double[]> samples) {
        this.numDuals = 0;
        this.dimension = dimension;
        this.bins = bins;
        int numBins = bins.length / dimension;
        this.numVectors = vectors.length / dimension;
        List<Double[]> vectors1 = new ArrayList<Double[]>();
        for (int i = 0; i < this.numVectors; i++) {
            vectors1.add(FormatUtils.toObjectDoubleArray(FormatUtils.getNthSubArray(vectors, dimension, i)));
        }
        this.projectedBins = new ArrayList<Double[]>();
        for (int i = 0; i < this.numVectors; i++) {
            this.projectedBins.add(FormatUtils.toObjectDoubleArray(HistUtils.projectBins(this.bins, dimension, FormatUtils.toDoubleArray(vectors1.get(i)))));
        }

        for (int i = 0; i < numReductions; i++) {
            int reducedDimension = 8;
            reductions[i] = new ReductionBound(dimension, reducedDimension, bins);
        }

        for (int i = 0; i < numDuals; i++) {
            int pickA = r.nextInt(samples.size());
            int pickB = r.nextInt(samples.size());
            while(pickA == pickB) r.nextInt(samples.size());
            double[] histA = HistUtils.normalizeArray(FormatUtils.getSubArray(FormatUtils.toDoubleArray(samples.get(pickA)), 1, numBins));
            double[] histB = HistUtils.normalizeArray(FormatUtils.getSubArray(FormatUtils.toDoubleArray(samples.get(pickB)), 1, numBins));
            duals[i] = new DualBound(histA, histB, bins, dimension);
        }
    }

    public boolean filter(double[] weightA, double[] weightB, double threshold) {
        // projection
        for (int i = 0; i < numVectors; i++) {
            if (HistUtils.getProjectEmd(weightA, weightB, FormatUtils.toDoubleArray(this.projectedBins.get(i))) > threshold) {
                return true;
            }
        }

        // rubner
        if (DistanceUtils.getRubnerEmd(weightA, weightB, dimension, bins, DistanceType.LTWO) > threshold) {
            return true;
        }

        // duals
        for (int i = 0; i < numDuals; i++) {
            if (duals[i].getDualEmd(weightA, weightB) > threshold) {
                return true;
            }
        }

        // reductions
        for (int i = 0; i < numReductions; i++) {
            if (reductions[i].getReducedEmd(weightA, weightB) > threshold) {
                return true;
            }
        }

        // indmin
        return DistanceUtils.getIndMinEmd(weightA, weightB, dimension, bins, DistanceType.LTWO, null) > threshold;

    }
}
