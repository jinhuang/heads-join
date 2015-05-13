package com.iojin.heads.spark

import org.apache.spark.Partitioner

/**
 * Created by Jin on 3/18/15.
 */
class KeyPartitioner(numParts : Int) extends Partitioner {
    override def numPartitions: Int = numParts

    override def getPartition(key: Any): Int = {
        if (key == null) 1
        else key.toString.toInt
    }
}
