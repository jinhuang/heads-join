package com.iojin.heads.spark

import com.iojin.heads.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Random

/**
 * Created by Jin on 4/1/15.
 */
object BNLJoin {
    def main(args: Array[String]) = {

        type JDouble = java.lang.Double
        //        type JHashMap = java.util.HashMap
        type JStringDoubleArrayMap = java.util.HashMap[String, Array[JDouble]]
        type JDoubleList = java.util.ArrayList[JDouble]
        type JDoubleArrayList = java.util.ArrayList[Array[JDouble]]

        val maxJDouble = java.lang.Double.MAX_VALUE
        val minJDouble = -java.lang.Double.MAX_VALUE

        /**
         * ---------------------------------------------------------------------
         * Command line parsing
         * ---------------------------------------------------------------------
         */
        if (args.length < 2) {
            System.err.println(
                "Usage: HeadsJoin " +
                    "--type=<distance|rank> " + // join predicate
                    "--predicate=<Float> " + // predicate value
                    "--input=<Path> " + // input files
                    "--dimension=<Int> " + // dimension
                    "--bins=<Path> " + // bin location file
                    "--numBins=<Int> " + // number of bins
                    "--projections=<Path> " + // projection file
                    "--numProjections=<Int> " + // number of projections
                    "--numErrorIntervals=<Int> " + // number of error interval
                    "--granularity=<Int> " + // grid granularity
                    "--sampleRate=<Float> " + // sample rate in rank join
                    "--parallelism=<Int> " + // number of workers
                    "--output=<Path> " // output file
            )
            System.exit(1)
        }

        val paras = mutable.Map(
            args.map { arg =>
                arg.dropWhile(_ == '-').split('=') match {
                    case Array(opt, v) => opt -> v
                    case _ => throw new IllegalArgumentException(
                        "Invalid " +
                            "argument: " + arg)
                }
            }: _*)

        val conf = new SparkConf().set("spark.locality.wait", "100000")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "com.iojin.heads.spark.HeadsKryoRegistrator")


        val joinType = paras.remove("type").getOrElse("distance")
        val joinPred = paras.remove("predicate").map(_.toDouble).getOrElse(1e-2)
        val inputPath = paras.remove("input").getOrElse("/apps/heads/input/test")
        val dimension = paras.remove("dimension").map(_.toInt).getOrElse(2)
        val binPath = paras.remove("bins").getOrElse("/apps/heads/input/bin")
        val numBin = paras.remove("numBins").map(_.toInt).getOrElse(32)
        val projPath = paras.remove("projections")
            .getOrElse("/apps/heads/input/projection")
        val numProj = paras.remove("numProjections").map(_.toInt).getOrElse(3)
        val numErr = paras.remove("numErrorIntervals").map(_.toInt).getOrElse(5)
        val grnu = paras.remove("granularity").map(_.toInt).getOrElse(7)
        val splRate = paras.remove("sampleRate").map(_.toDouble).getOrElse(0.01)
        val numWrk = paras.remove("parallelism").map(_.toInt).getOrElse(60)
        val output = paras.remove("output").getOrElse("/apps/heads/spark/output/test")

        val sc = new SparkContext(
            conf.setAppName(
                "HeadsJoin(" +
                    inputPath + ")"))
        val data = Utils.readData(inputPath, sc)
        val bin = Utils.readArray(binPath, sc)
        val proj = Utils.readArray(projPath, sc)

        def sample= {
            data.sample(withReplacement = false, splRate, Random.nextLong())
        }

        def sampleDual = {
            data.takeSample(withReplacement = false, numWrk * 2, Random.nextLong())
        }

        def buildDuals(samples: Array[(Long, Array[Double])]) = {
            val numPairs = samples.length / 2
            (0 until numPairs).map{i =>
                val rA = samples(2 * i)
                val rB = samples(2 * i + 1)
                new DualBound(rA._2, rB._2, bin, dimension)
            }.toArray
        }

        val projBin = HistUtils.projectBins(bin, dimension, proj, numProj)

        val dualSamples = sampleDual
        val duals = buildDuals(dualSamples)

        val reductions = (0 until 10).map{i =>
            new ReductionBound(numBin, numBin / 4, bin)}.toArray

        def joinRecords(a: (Long, Array[Double]), b: (Long, Array[Double]),
                        pred: Double)
        : (Boolean, (Long, Long, Double)) = {
            var emd = -1.0D
            if (a._1 < b._1) {
                val projLB = (0 until numProj).map{i =>
                    val projection = HistUtils.substractAvg(
                        FormatUtils.getNthSubArray(projBin, numBin, i))
                    DistanceUtils.get1dEmd(a._2, b._2, projection)
                }.max
                if (projLB < pred) {
                    val dualLB = duals.map{dual =>
                        dual.getDualEmd(a._2, b._2)
                    }.max
                    if (dualLB < pred) {
                        val reductionLB = reductions.map{reduction =>
                            reduction.getReducedEmd(a._2, b._2)
                        }.max
                        if (reductionLB < pred) {
                            val indLB = DistanceUtils.getIndMinEmd(
                                a._2, b._2, dimension, bin, DistanceType.LTWO,
                                null)
                            if (indLB < pred) {
                                emd = DistanceUtils.getEmdLTwo(a._2, b._2,
                                    dimension, bin)
                            }
                        }
                    }
                }
            }
            (emd > 0 && emd < pred, if (emd > 0 && emd < pred) (a._1, b._1, emd) else null.asInstanceOf[(Long, Long, Double)])
        }

        def join: RDD[(Long, Long, Double)] = {
            val localData = data.collect()
            val k = joinPred.toInt
            val globalBound = if (joinType.equalsIgnoreCase("rank")) {
                val samples = sample.collect()
                val references = samples.map{record =>
                    val each : Array[JDouble] = record._2.map(new JDouble(_))
                    val list = new JDoubleArrayList
                    list.add(each)
                    list
                }.reduce{(a, b) =>
                    a.addAll(b)
                    a
                }
                HistUtils.getKEmd(references, bin, dimension, k)
            } else joinPred
            val minimalEmds = Array.fill(k)((0L, 0L, globalBound))
            data.flatMap{a =>
                val each = localData.map{b =>
                    joinType match {
                        case "distance" =>
                            val threshold = joinPred
                            joinRecords(a, b, joinPred)
                        case "rank" =>
                            val threshold = minimalEmds(k - 1)._3
                            joinRecords(a, b, threshold)
                    }
                }.filter(_._1).map(_._2)
                joinType match {
                    case "distance" =>
                        each
                    case "rank" =>
                        val result = (each.take(k) ++ minimalEmds).sortBy(_._3).take(k)
                        (0 until k).foreach{i =>
                            minimalEmds(i) = result(i)
                        }
                        result
                }
            }
        }

        TimerUtils.start()
        join.sortBy(_._3).saveAsTextFile(output)
        TimerUtils.end()
        TimerUtils.print()
    }
}
