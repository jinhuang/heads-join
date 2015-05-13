package com.iojin.heads.spark

import java.util

import com.esotericsoftware.kryo.Kryo
import com.iojin.heads.common._
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by Jin on 3/17/15.
 */
class HeadsKryoRegistrator extends KryoRegistrator {
    def registerClasses(kryo: Kryo) = {
        kryo.register(classOf[DualBound])
        kryo.register(classOf[ReductionBound])
        kryo.register(classOf[Grid])
        kryo.register(classOf[QuantileGrid])
        kryo.register(classOf[Candidate])
        kryo.register(classOf[Array[Grid]])
        kryo.register(classOf[Array[QuantileGrid]])
        kryo.register(classOf[Array[DualBound]])
        kryo.register(classOf[Array[ReductionBound]])
        kryo.register(classOf[util.TreeSet[Candidate]])
    }
}
