package com.iojin.heads.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable


object Utils {

    def readArray(hdfsPath: String, sc: SparkContext): Array[Double] = {
        sc.textFile(hdfsPath).collect()(0).split(" ").map(_.toDouble)
    }

    def readData(hdfsPath: String, sc: SparkContext): RDD[(Long, Array[Double])] = {
        sc.textFile(hdfsPath).map{line =>
            val ary = line.split(" ")
            (ary(0).toLong, ary.slice(1, ary.length).map(_.toDouble))
        }
    }

    def assignGrid(count: Map[String, Int], num: Int)
    : java.util.HashMap[String, Int] = {
        val map = new mutable.HashMap[String, Int]()
        val longCount =
            count.map(each =>
                (each._1.split(" ").map(_.toDouble.toLong.toString).reduce(_ + " " + _),
                    each._2)).toMap
        // this is incorrect, if use count as the key, combinations with the same count will be overwritten
        //val sorted = longCount.map(each => (each._2, each._1)).toArray.sortBy(_._1)
        val sorted = longCount.toArray.sortBy(_._2)
        val workloads = Array.fill(num)(0)
        (0 until sorted.length).foreach{i =>
            val work = sorted(i)
            val worker = workloads.zipWithIndex.minBy(_._1)._2
            map.update(work._1, worker)
            val oldWorkload = workloads(worker)
            workloads(worker) = oldWorkload + work._2
        }
        new java.util.HashMap[String, Int](map.asJava)
    }

    def productMap(a: Map[String, Int], b: Map[String, Int])
    : Map[String, Int] = {
//        a.foreach { each =>
//            val key = toInt(each._1)
//            val c = each._2
//            println(f"a $key => $c")
//        }
//        b.foreach { each =>
//            val key = each._1
//            val c = each._2
//            println(f"b $key => $c")
//        }
        a.map { each =>
            val key = toInt(each._1)
            val guest = if (b.contains(key)) b(key) else 0
            (each._1, each._2 * (each._2 + guest))
        }
    }

    def toInt(d : String): String = {
        d.split(" ").map(_.toDouble.toInt.toString).reduce(_ + " "  + _)
    }
}
