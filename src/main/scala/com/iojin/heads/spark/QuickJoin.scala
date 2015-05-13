package com.iojin.heads.spark

import com.iojin.heads.common._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.Random

object QuickJoin {
    def main(args: Array[String]) = {
        if (args.length < 2) {
            System.err.println(

            )
            System.exit(1)
        }

        val paras = mutable.Map(args.map{arg =>
            arg.dropWhile(_ == '-').split('=') match {
                case Array(opt, v) => opt -> v
                case _ => throw new IllegalArgumentException("Invalid " +
                    "argument: " + arg)
            }
        }: _*)

        val conf = new SparkConf()
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "com.iojin.heads.spark.HeadsKryoRegistrator")
            .set("spark.locality.wait", "100000")

        val joinType = paras.remove("type").getOrElse("distance")
        val joinPred = paras.remove("predicate").map(_.toDouble).getOrElse(1e-2)
        val inputPath = paras.remove("input").getOrElse("/apps/heads/input/test")
        val sc = new SparkContext(conf.setAppName("QuickJoin (" +
            inputPath + ")"))
        val dimension = paras.remove("dimension").map(_.toInt).getOrElse(2)
        val data = Utils.readData(inputPath, sc)
        val binPath = paras.remove("bins").getOrElse("/apps/heads/input/bin")
        val numBin = paras.remove("numBins").map(_.toInt).getOrElse(32)
        val bin = Utils.readArray(binPath, sc)
        val projPath = paras.remove("projections").getOrElse("/apps/heads/input/projection")
        val numProj = paras.remove("numProjections").map(_.toInt).getOrElse(3)
        val proj = Utils.readArray(projPath, sc)
        val numWrk = paras.remove("parallelism").map(_.toInt).getOrElse(60)
        val numPivot = paras.remove("numPivots").map(_.toInt).getOrElse(numWrk)
        val output = paras.remove("output").getOrElse("/apps/heads/output/test")

        val projBin = HistUtils.projectBins(bin, dimension, proj, numProj)

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

        val dualSamples = sampleDual
        val duals = buildDuals(dualSamples)

        val reductions = (0 until 10).map{i =>
            new ReductionBound(numBin, numBin / 4, bin)}.toArray

        def joinPartition(partition: Iterable[(String, (Long, Array[Double]))])
        : Iterator[(Long, Long, Double)]= {
            val array = partition.toArray
            val num = array.length
            println("HEADS DEBUG: join " + num + " records")
            (0 until num).flatMap{i =>
                val result = ((i + 1) until num).map{j =>
                    val a = array(i)._2
                    val b = array(j)._2
                    joinRecords(a, b, joinPred)
                }
                result.filter(_._1).map(_._2).iterator
            }.iterator
        }

        def rankJoinPartition(partition: Iterable[(String, (Long, Array[Double]))], global: Double)
        : Iterator[(Long, Long, Double)]= {
            val array = partition.toArray
            val num = array.length
            var bound = global
            val n = joinPred.toInt
            val minimalEmds = Array.fill(n)((0L, 0L, global))
            println("HEADS DEBUG: join " + num + " records")
            (0 until num).foreach{i =>
                ((i + 1) until num).foreach{j =>
                    val a = array(i)._2
                    val b = array(j)._2
                    val pair = joinRecords(a, b, bound)
                    if (pair._1 && pair._2._3 < minimalEmds(n - 1)._3) {
                        minimalEmds(n - 1) = pair._2
                        minimalEmds.sortBy(_._3)
                        bound = minimalEmds(n - 1)._3
                    }
                }
                minimalEmds.sortBy(_._3)
                bound = minimalEmds(n - 1)._3
            }
            minimalEmds.iterator
        }

        def joinRecords(a: (Long, Array[Double]), b: (Long, Array[Double]), pred: Double)
        : (Boolean, (Long, Long, Double)) = {
            var emd = -1.0D
            if (a._1 < b._1) {
                val projLB = (0 until numProj).map{i =>
                    val projection = (0 until numBin).map{j =>
                        projBin(i * numBin + j)
                    }.toArray
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
                                a._2, b._2, dimension, bin, DistanceType.LTWO, null)
                            if (indLB < pred) {
                                emd = DistanceUtils.getEmdLTwo(a._2, b._2, dimension, bin)
//                                emd = 1.0
                            }
                        }
                    }
                }
//                emd = if (Random.nextInt(1000000) < pred) 1.0 else -1.0
            }
            (emd > 0, if (emd > 0) (a._1, b._1, emd) else null.asInstanceOf[(Long, Long, Double)])
        }

        // sample a few pivots
        val pivot = data.takeSample(withReplacement = false, numPivot, Random.nextLong())

        def distanceJoin(): RDD[(Long, Long, Double)] = {

            // partition data according to pivots
            data.flatMap{record =>
                // the nearest pivot and window-pair
                val pivotD = pivot.map{p =>
                    val emd = DistanceUtils.getEmdLTwo(record._2, p._2, dimension, bin)
                    (emd, p._1)
                }
                val nearest = pivotD.minBy(_._1)._2
                val minEmd = pivotD.minBy(_._1)._1
                val awindow = pivotD.filter(each => each._2 != nearest && (each._1 - minEmd) / 2 < joinPred).map(_._2)
                val windows = awindow.filter(_ < nearest).map(_.toString + " " + nearest.toString) ++
                    awindow.filter(_ > nearest).map(nearest.toString + " " + _.toString)
                (windows :+ nearest.toString).map(pid => (pid, record))
            }.groupBy(_._1).flatMap{ partition =>
                joinPartition(partition._2)
            }
        }

        def rankJoin(): RDD[(Long, Long, Double)] = {

            // compute the global bound
            val k = joinPred.toInt
            val dP = (0 until numPivot).flatMap{i =>
                ((i + 1) until numPivot).map{j =>
                    DistanceUtils.getEmdLTwo(pivot(i)._2, pivot(j)._2, dimension, bin)
                }
            }.sorted.seq
            val global = if (dP.length > k) dP(k) else 0.0

            // partition data according to pivots
            data.flatMap{record =>
                // the nearest pivot and window-pair
                val pivotD = pivot.map{p =>
                    val emd = DistanceUtils.getEmdLTwo(record._2, p._2, dimension, bin)
                    (emd, p._1)
                }
                val nearest = pivotD.minBy(_._1)._2
                val minEmd = pivotD.minBy(_._1)._1
                val awindow = pivotD.filter(each => each._2 != nearest && (each._1 - minEmd) / 2 < joinPred).map(_._2)
                val windows = awindow.filter(_ < nearest).map(_.toString + " " + nearest.toString) ++
                    awindow.filter(_ > nearest).map(nearest.toString + " " + _.toString)
                (windows :+ nearest.toString).map(pid => (pid, record))
            }.groupBy(_._1).flatMap{ partition =>
                rankJoinPartition(partition._2, global)
            }
        }

        /**
         * ---------------------------------------------------------------------
         * Main routine
         * ---------------------------------------------------------------------
         */
        TimerUtils.start()
        joinType match {
            case "distance" =>
                val result = distanceJoin()
                result.saveAsTextFile(output)
                sc.stop()
                TimerUtils.end()
            case "rank" =>
                val result = rankJoin()
                result.saveAsTextFile(output)
                sc.stop()
                TimerUtils.end()
        }
        TimerUtils.print()
    }
}
