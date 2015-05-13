package com.iojin.heads.common;

import org.apache.hadoop.io.Text;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;

/**
 * Created by Jin on 3/11/15.
 */
public class BSPUtils {
    private static final int JOINEDPAIR_LENGTH = 3;

    public static double[] toDoubleArray(JoinedPair pair) {
        double[] result = new double[JOINEDPAIR_LENGTH];
        result[0] = pair.getRid();
        result[1] = pair.getSid();
        result[2] = pair.getDist();
        return result;
    }

    public static JoinedPair toJoinedPair(double[] array) {
        if (array.length < 4) {
            return null;
        }
        JoinedPair pair = new JoinedPair((long)array[1], (long)array[2], array[3]);
        return pair;
    }

    public static DoubleVector toDoubleVector(Text text) {
        double[] array = FormatUtils.toDoubleArray(text.toString());
        return new DenseDoubleVector(array);
    }

    public static DoubleVector toDoubleVector(String text) {
        double[] array = FormatUtils.toDoubleArray(text);
        return new DenseDoubleVector(array);
    }
}
