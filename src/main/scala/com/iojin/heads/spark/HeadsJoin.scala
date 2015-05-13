package com.iojin.heads.spark

import com.iojin.heads.common._
import org.apache.commons.math.distribution.NormalDistributionImpl
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

object HeadsJoin {
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

        val paras = mutable.Map(args.map{arg =>
            arg.dropWhile(_ == '-').split('=') match {
                case Array(opt, v) => opt -> v
                case _ => throw new IllegalArgumentException("Invalid " +
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

        val sc = new SparkContext(conf.setAppName("HeadsJoin(" +
            inputPath + ")"))
        val data = Utils.readData(inputPath, sc)
        val bin = Utils.readArray(binPath, sc)
        val proj = Utils.readArray(projPath, sc)

        /**
         * ---------------------------------------------------------------------
         * Common HeadsJoin variables and methods
         * Variables defined here do not involve RDD computation
         * ---------------------------------------------------------------------
         */
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

        val t = (0 until numProj).map{i => // each projection
                val p = FormatUtils.getNthSubArray(projBin, numBin, i)
                (p.min - p.sum / p.length, p.max - p.sum / p.length)
        }

        val dualSamples = sampleDual
        val duals = buildDuals(dualSamples)

        val reductions = (0 until 10).map{i =>
            new ReductionBound(numBin, numBin / 4, bin)}.toArray

        def toNormal(record: Array[Double], i: Int) = {
            HistUtils.getNormal(record,
                HistUtils.substractAvg(
                    FormatUtils.getNthSubArray(projBin, numBin, i)))
        }

        def toNormals(record: (Long, Array[Double]))
        : Array[NormalDistributionImpl] = {
            (0 until numProj).map{i =>
                HistUtils.getNormal(record._2,
                    HistUtils.substractAvg(
                        FormatUtils.getNthSubArray(projBin, numBin, i)))
            }.toArray
        }

        def toCDF(record: Array[Double], i: Int) = {
            HistUtils.getDiscreteCDFNormalized(record,
                FormatUtils.subtractAvg(
                    FormatUtils.getNthSubArray(projBin, numBin, i)))
        }

        val slope = t.map(e => (e._2 * -1.0, e._1 * -1.0))
        val percentile = new Percentile()

        def toNormalAux(record: Array[Double], grids: Array[Grid]) = {
            (0 until numProj).map{i =>
                val normal = toNormal(record, i)
                val cdf = toCDF(record, i)
                val error = HistUtils.getMinMaxError(normal, cdf, numErr)
                val fullError = HistUtils.getFullError(normal, cdf, t(i)._1,
                    t(i)._2)
                val m = 1.0 / normal.getStandardDeviation
                val b =  -1.0 * normal.getMean / normal.getStandardDeviation
                val id = {
                    val id = grids(i).getGridId(Array(m, b))
                    if (id < 0) 0 else id
                }
                (id, normal, cdf, error, fullError, m, b)
            }
        }

        def toCount(error: Array[Double]) = {
            val lengthInterest = numProj + 1
            val lengthPerCombination = numProj + 1 + (numErr + 1) * 2 * numProj
            val numComb = error.length / lengthPerCombination
            (0 until numComb).map{i =>
                val interest =
                    FormatUtils.getSubArray(
                        FormatUtils.getNthSubArray(
                            error, lengthPerCombination, i), 0, lengthInterest - 1)
                val count = interest(lengthInterest - 1).toInt
                val key = FormatUtils.getSubArray(
                    interest, 0, interest.length - 2).map(_.toString)
                    .reduce(_ + " " + _)
                (key, count)
            }.toMap
        }

        def toCombAux(projected: IndexedSeq[(Long, Array[Double], Array[Double])]) = {
            val combination = projected.map(_._1.toString).reduceLeft(_ + " " + _)
            val recordLocation = projected.map(_._2).reduceLeft(_ ++ _)
            val recordError = projected.map(_._3).reduceLeft(_ ++ _)
            (combination, recordLocation, recordError)
        }

        def buildSpaces = {
            val spaces = data.map { record =>
                val normals = toNormals(record)
                val ms = normals.map(1.0 / _.getStandardDeviation)
                val bs = normals.map(
                    n =>
                        -1.0 * n.getMean / n.getStandardDeviation)
                ms ++ bs // m0, m1, ..., b0, b1, ...
            }
            (0 until 2 * numProj).map{i => spaces.map(_.apply(i)).collect()} // [m0] [m1] ... [b0] [b1] ...
        }

        def buildGrids: Array[Grid] = {
            val localSpace = buildSpaces

            val mDomain = ((0 until numProj).map{i => localSpace(i).min}.toArray,
                (0 until numProj).map{i => localSpace(i).max}.toArray)
            val bDomain = (
                (numProj until 2 * numProj).map{i => localSpace(i).min}.toArray,
                (0 until numProj).map{i => localSpace(i).max}.toArray)

            (0 until numProj).map{i =>
                val domain = Array(mDomain._1(i), mDomain._2(i), bDomain._1(i),
                    bDomain._2(i))
                val grid = new Grid(
                    domain,
                    Array(slope(i)._1, slope(i)._2),
                    grnu)
                // here this is incorrect, localSpace is a [2 * numProj][record] array, not a [record][numProj] array
//                val dist = localSpace.map{record =>
//                    val point = Array(record(i), record(i + numProj))
//                    grid.getProjectionDistanceInGrid(point)
//                }
                val ms =localSpace(i)
                val bs = localSpace(i + numProj)
                val dist = (0 until ms.length).map{j =>
                    val point = Array(ms(j), bs(j))
                    grid.getProjectionDistanceInGrid(point)
                }


                val sw = dist.map(_.apply(0))
                val se = dist.map(_.apply(1))
                percentile.setData(sw.toArray)
                val swPercentile = (0 to grnu).map{j =>
                    val rate = if (j == 0) 1e-7 else if (j == grnu) 100
                    else (100.0 / grnu) * j
                    percentile.evaluate(rate)
                }.toArray
                percentile.setData(se.toArray)
                val sePercentile = (0 to grnu).map{j =>
                    val rate = if (j == 0) 1e-7 else if (j == grnu) 100
                    else (100.0 / grnu) * j
                    percentile.evaluate(rate)
                }.toArray
                new QuantileGrid(domain, Array(slope(i)._1, slope(i)._2), grnu,
                    swPercentile, sePercentile)
            }.toArray
        }

        def aggregateSpace(grids: Array[Grid], duals: Array[DualBound]) = {
            val numDuals = duals.length
            val allComb = data.map{record =>
                val errors = {
                    toNormalAux(record._2, grids).map {
                        case (id, _, _, rError, fullError, _, _) =>
                            (id, rError, fullError)
                    }
                }
                val dualKeys = duals.map(dual => dual.getKey(record._2))
                val rubnerKeys = DistanceUtils.getRubnerValue(record._2, dimension, bin)
                val combination = errors.map(_._1.toString).reduceLeft(_ + " " + _)
                // combinationId, record(error proj0, error proj1, ...),
                // record(full error proj0,...)
                (combination, errors.map(_._2), errors.map(_._3), dualKeys, rubnerKeys)
            }.groupBy(_._1) // combinationId -> records in the same combCell
                .map{combCell => // each combCell
                val combArray = combCell._2.toArray
                val error = combArray.map(_._2).reduceLeft{(a, b) => // two records
                    (0 until numProj).map{i => // in each projection
                        (0 until numErr).map { j => // in each interval
                            val aMinError = a(i).get(j * 2)
                            val bMinError = b(i).get(j * 2)
                            val aMaxError = a(i).get(j * 2 + 1)
                            val bMaxError = b(i).get(j * 2 + 1)
                            val minError = new JDouble(Array(aMinError, bMinError).min)
                            val maxError = new JDouble(Array(aMaxError, bMaxError).max)
                            val list = new JDoubleList
                            list.add(new JDouble(minError))
                            list.add(new JDouble(maxError))
                            list
                        }.reduce{(rA, rB) =>
                            rA.addAll(rB)
                            rA
                        }
                    }
                }
                val fullError = (0 until numProj).map { i =>
                    val fullError = combArray.map(_._3(i))
                    (fullError.min, fullError.max)
                }

                val minDual: Array[JDouble] = Array.fill(numDuals)(minJDouble)
                val maxDual: Array[JDouble] = Array.fill(numDuals)(maxJDouble)
                val minRubner: Array[JDouble] = Array.fill(dimension)(minJDouble)
                val maxRubner: Array[JDouble] = Array.fill(dimension)(maxJDouble)
                combArray.map(_._4).foreach{dual =>
                    (0 until duals.length).foreach{i =>
                        minDual(i) = if (minDual(i) < dual(i))
                            minDual(i) else dual(i)
                        maxDual(i) = if (maxDual(i) > dual(i))
                            maxDual(i) else dual(i)
                    }
                }
                combArray.map(_._5).foreach{rubner =>
                    (0 until dimension).foreach{i =>
                        minRubner(i) = if (minRubner(i) < rubner(i))
                            minRubner(i) else rubner(i)
                        maxRubner(i) = if (maxRubner(i) > rubner(i))
                            maxRubner(i) else rubner(i)
                    }
                }
                (combCell._1, (error, fullError, combArray.length,
                    (minDual, maxDual), (minRubner, maxRubner)))
            }.collectAsMap()
            val combErrorArray = allComb.map{eachComb =>

                val ids = eachComb._1.split(" ").map(e => new JDouble(e.toDouble))
                val count = eachComb._2._3
                val combError = (0 until numProj).map{i =>
                    val fullError = eachComb._2._2.apply(i)
                    eachComb._2._1.apply(i).toArray(new Array[JDouble](0)) ++
                        Array(new JDouble(fullError._1), new JDouble(fullError._2))
                }.reduceLeft(_ ++ _)

                ids ++ Array(new JDouble(count.toDouble)) ++ combError
            }.reduceLeft(_ ++ _)
            val dualMap = new mutable.HashMap[String, Array[JDouble]]()
            val rubnerMap = new mutable.HashMap[String, Array[JDouble]]()
            allComb.foreach{eachComb =>
                dualMap.update(eachComb._1,
                    eachComb._2._4._1 ++ eachComb._2._4._2)
                rubnerMap.update(eachComb._1,
                    eachComb._2._5._1 ++ eachComb._2._5._2)
            }
            (combErrorArray, new JStringDoubleArrayMap(dualMap.asJava),
                new JStringDoubleArrayMap(rubnerMap.asJava))
        }

        def joinPartition(eachComb: (Int,
            Iterable[(Boolean, (Long, Array[Double]), Array[String])])) = {
            val natives = eachComb._2.filter(!_._1)
                .map(e => (e._2, e._3)).seq.toArray
            val guests = eachComb._2.filter(_._1)
                .map(e => (e._2, e._3)).seq.toArray

            val distinctKeys = natives.flatMap(_._2).distinct

            val nativeResult = distinctKeys.flatMap{key =>
                val comb = natives.filter(_._2(0) == key).map(_._1)
                (0 until comb.length).flatMap{i =>
                    ((i + 1) until comb.length).map{j =>
                        joinRecords(comb(i), comb(j), joinPred)
                    }
                }
            }.filter(_._1).map(_._2)
            val guestResult = distinctKeys.flatMap{key =>
                val nativeComb = natives.filter(_._2(0) == key).map(_._1)
                val guestComb = guests.filter(_._2.contains(key)).map(_._1)
                (0 until nativeComb.length).flatMap{i =>
                    (0 until guestComb.length).map{j =>
                        joinRecords(nativeComb(i), guestComb(j), joinPred)
                    }
                }
            }.filter(_._1).map(_._2)

            (nativeResult ++ guestResult).iterator
        }

        def rankJoinPartition(eachComb: (Int, Iterable[(Boolean,
            (Long, Array[Double]), Array[String])]), globalBound: Double,
                              n: Int) = {
            val natives = eachComb._2.filter(!_._1)
                .map(e => (e._2, e._3)).seq.toArray
            val guests = eachComb._2.filter(_._1)
                .map(e => (e._2, e._3)).seq.toArray

            val distinctKeys = natives.flatMap(_._2).distinct

            val minimalEmds = Array.fill(n)((0L, 0L, globalBound))
            var bound = globalBound
            distinctKeys.foreach{key =>
                val comb = natives.filter(_._2(0) == key).map(_._1)
                (0 until comb.length).foreach{i =>
                    val result = ((i + 1) until comb.length).map{j =>
                        joinRecords(comb(i), comb(j), bound)
                    }.filter(_._1).map(_._2).sortBy(_._3).take(n)
                    result.foreach{pair =>
                        (0 until n).foreach{j =>
                            if (minimalEmds(j)._3 > pair._3) {
                                minimalEmds(j) = pair
                            }
                        }
                    }
                    minimalEmds.sortBy(_._3)
                    bound = minimalEmds(n - 1)._3
                }
            }
            println("HEADS DEBUG: done native joins")
            distinctKeys.foreach{key =>
                val nativeComb = natives.filter(_._2(0) == key).map(_._1)
                val guestComb = guests.filter(_._2.contains(key)).map(_._1)
                (0 until nativeComb.length).foreach{i =>
                    val result = (0 until guestComb.length).map{j =>
                        joinRecords(nativeComb(i), guestComb(j), bound)
                    }.filter(_._1).map(_._2).sortBy(_._3).take(n)
                    result.foreach{pair =>
                        (0 until n).foreach{j =>
                            if (minimalEmds(j)._3 > pair._3) {
                                minimalEmds(j) = (pair._1, pair._2, pair._3)
                            }
                        }
                    }
                    minimalEmds.sortBy(_._3)
                    bound = minimalEmds(n - 1)._3
                }
            }

//            (nativeResult ++ guestResult).sortBy(_._3).take(n).toIterator
            minimalEmds.iterator
//            nativeResult.sortBy(_._3).take(n).toIterator
        }

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

        /**
         * ---------------------------------------------------------------------
         * Join routines
         * ---------------------------------------------------------------------
         */
        def distanceJoin: RDD[(Long, Long, Double)] = {

            println("HEADS DEBUG: granularity " + grnu)

            // step 0 obtain domain
            val grids = buildGrids

            // step 1 error aggregation
            val space = aggregateSpace(grids, duals)


            // step 2 assign workers
            val spaceFirstPart = space._1.map(_.toDouble)
            val count = toCount(spaceFirstPart)

            val guests = data.map { record =>
                val projected = {
                    toNormalAux(record._2, grids).map {
                        case (id, _, _, rError, fullError, m, b) =>
                            (id, Array(m, b),
                                rError.toArray(new Array[JDouble](0))
                                    .map(_.toDouble) :+ fullError)
                    }
                }

                val combAux = toCombAux(projected)
                val dualMap = space._2
                val rubnerMap = space._3

                (combAux match {
                    case (combination, recordLocation, recordError) =>
                        Grid.getGuestWithDual(
                            recordLocation,
                            recordError, combination, joinPred,
                            spaceFirstPart, numErr, numProj, grids.toArray,
                            record._1, duals, dualMap, record._2, bin,
                            dimension, rubnerMap)
                }, combAux._1, record)
            }.persist()

            val guestCount = guests.flatMap{candidateComb =>
                candidateComb._1.map(id => (id, 1))
            }.reduceByKey(_ + _).collect().toMap

            val realCount = Utils.productMap(count, guestCount)

            val assignment = Utils.assignGrid(realCount, numWrk)

            // step 3 filter
            guests.flatMap { each =>
                val rid = each._2
                val record = each._3
                val dist = each._1.map{comb =>
                    (assignment.get(comb), comb)
                }.groupBy(_._1).mapValues(_.map(_._2)).toList

                dist.map{id=>
                    (id._1, (true, record, id._2.toArray))
                } :+ (assignment.get(rid), (false, record, Array(rid)))
            }.partitionBy(new KeyPartitioner(numWrk)).groupByKey()
                .flatMap(joinPartition)
        }

        def rankJoin: RDD[(Long, Long, Double)] = {
            // sample a subset and a few for the dual bound
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

            val k = joinPred.toInt

            val grids = buildGrids

            val globalBound = HistUtils.getKEmd(references, bin, dimension, k)

            // step 1 error aggregation
            val space = aggregateSpace(grids, duals)

            // step 2 assign workers
            val spaceFirstPart = space._1.map(_.toDouble)
            val count = toCount(spaceFirstPart)

            val guests= data.map{record=>
                val projected = {
                    toNormalAux(record._2, grids).map {
                        case (id, _, _, rError, fullError, m, b) =>
                            (id, Array(m, b) ,
                                rError.toArray(new Array[JDouble](0))
                                    .map(_.toDouble) :+ fullError)
                    }
                }

                val combAux = toCombAux(projected)
                val dualMap = space._2
                val rubnerMap = space._3

                (combAux match {
                    case (combination, recordLocation, recordError) =>
                        Grid.getGuestForRank(recordLocation,
                            recordError, combination, joinPred,
                            spaceFirstPart, numErr, numProj, grids.toArray,
                            record._1, duals, dualMap, record._2,
                            references.length, joinPred.toInt, references, bin,
                            dimension, globalBound, rubnerMap)
                }, combAux._1, record)
            }.persist()

            val guestCount = guests.flatMap {candidateComb =>
                candidateComb._1.map(id => (id.getCombination, 1)).toIterator
            }.reduceByKey(_ + _).collect().toMap

            val realCount = Utils.productMap(count, guestCount)

            val assignment = Utils.assignGrid(realCount, numWrk)

            // step 3 filter
            guests.flatMap{each =>
                val record = each._3
                each._1.map{id =>
                    (assignment.get(id.getCombination), id.getCombination)
                }.groupBy(_._1).mapValues(_.map(_._2)).toList
                .map{id =>
                    (id._1, (true, record, id._2.toArray))
                } :+ (assignment.get(each._2), (false, record, Array(each._2)))
            }.partitionBy(new KeyPartitioner(numWrk)).groupByKey()
                .flatMap(rankJoinPartition(_, globalBound, k))
        }

        /**
         * ---------------------------------------------------------------------
         * Main routine
         * ---------------------------------------------------------------------
         */
        TimerUtils.start()
        joinType match {
            case "distance" =>
                val result = distanceJoin
                result.saveAsTextFile(output)
                TimerUtils.end()
                sc.stop()
            case "rank" =>
                TimerUtils.start()
                val result = rankJoin
                result.saveAsTextFile(output)
                TimerUtils.end()
                sc.stop()
        }
        TimerUtils.print()
    }
}
