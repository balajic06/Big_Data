package io.purush.spark.giscup.script

import java.io._
import java.util

import org.apache.spark.{SparkConf, SparkContext}

object CellScript {
  def main(args: Array[String]): Unit = {

    if(args.length<2){
      println("Usage -> spark-submit -- spark-submit --master <SPARK_URL> --class io.purush.spark.giscup.script.CellScript group25_phase3.jar <HDFSPATH> <OUTPUTFILE>")

      return
    }
    // Setup csv file path on hdfs
    val textFile = args(0)

    // Setup Spark Context
    val config = new SparkConf().setAppName("Phase-III-GISCUP")
    val sc = new SparkContext(config)

    val start: Long = System.currentTimeMillis()
    //create RDD from csv file on hdfs
    val csvFileRDD = sc textFile textFile

    // Remove csv header from RDD
    val first = csvFileRDD.first
    val headerRemovedRDD = csvFileRDD filter { x => x != first }

    // Function to round out the double string -> longitude by i=5, latitude by i=6 to include - sign
    def roundUp(d: String, i: Int): Double = d.substring(0, i).toDouble

    // Function to split the csv field and return only those columns we need-> 1,5,6
    // Also round out the lat,long fields and take only day field from datetime field
    def splitFilter(s: Array[String]): (Int, Double, Double) = {
      def reRound(x: Int, i: Int): Double = if (s(x).length > i) roundUp(s(x), i) else s(x).toDouble
      (s(1).split(" ")(0).split("-")(2).toInt, s(5).toDouble, s(6).toDouble)
    }

    // Apply splitting, filtering and rounding on headerRemovedRDD
    val roundedRDD = headerRemovedRDD map {
      x => splitFilter(x.split(","))
    }

    // Filter out envelope around NYC in Long,Lat values
    val envelopeFilteredRDD = roundedRDD filter {
      x =>
        x._3 >= 40.50 &&
          x._3 <= 40.90 &&
          x._2 >= -74.25 &&
          x._2 <= -73.70
    }

    // Cutout longitude and compare it across tuple3s
    val orderByLongitude = new Ordering[(Int, Double, Double)]() {
      override def compare(x: (Int, Double, Double), y: (Int, Double, Double)): Int =
        x._2 compare y._2
    }

    // Cutout latitude and compare it across tuple3s
    val orderByLatitude = new Ordering[(Int, Double, Double)]() {
      override def compare(x: (Int, Double, Double), y: (Int, Double, Double)): Int =
        x._3 compare y._3
    }

    // Intify lat,longs, have an extra digit to make sure points are properly bucketed in cells
    //val intedRDD = envelopeFilteredRDD map { x => (x._1, (x._2 * 1000).toInt, (x._3 * 1000).toInt) }

    // Find ranges to partition cells
    val maxLongitude = envelopeFilteredRDD.max()(orderByLongitude)._2
    val minLongitude = envelopeFilteredRDD.min()(orderByLongitude)._2
    val maxLatitude = envelopeFilteredRDD.max()(orderByLatitude)._3
    val minLatitude = envelopeFilteredRDD.min()(orderByLatitude)._3
    val xcells = ((maxLongitude - minLongitude) * 100).toInt + 1
    val ycells = ((maxLatitude - minLatitude) * 100).toInt + 1
    //    println(xcells + " , " + ycells)
    //    println(maxLatitude + " " + maxLongitude)
    //    println(minLatitude + " " + minLongitude)
    // Transform points relative to envelope borders (40.5,-74.25)
    val spatiallyTransformedRDD = envelopeFilteredRDD map { x => (x._1, x._2 + math.abs(minLongitude), x._3 - math.abs(minLatitude)) }

    //    println(spatiallyTransformedRDD.count)
    def isInCell(d: (Int, Double, Double), c: (Int, Int)): Boolean = {
      val d2 = d._2 * 100
      val d3 = d._3 * 100
      (c._1 <= d2) &&
        (d2 <= (c._1 + 1)) &&
        (c._2 <= d3) &&
        (d3 <= (c._2 + 1))
    }

    // Create a cell-grid of ycells x xcells, flatten and zip to get row wise column order indices for any cell
    val zippedGrid = Array.tabulate(xcells + 1, ycells + 1)((x, y) => (x, y)).flatten.zipWithIndex

    // Cellify cutRDD
    val spatialCelledRDD = spatiallyTransformedRDD map { x => (zippedGrid filter { y => isInCell(x, y._1) }, x._1) }

    // Make (CellId,Day) the key and filter those cells with array length=0
    val keyedSpatialCelledRDD = spatialCelledRDD filter {
      x => x._1.length > 0
    } map {
      x => ((x._1(0)._2, x._2), 1)
    }
    //println(keyedSpatialCelledRDD.count + " records in total before reducing cells downto grouped cells.")
    // ReduceByKey and wordCount -> trips in cell obtained!
    val reducedCelledRDD = keyedSpatialCelledRDD.reduceByKey(_ + _)

    val orderInt = new Ordering[((Int, Int), Int)]() {
      override def compare(x: ((Int, Int), Int), y: ((Int, Int), Int)): Int =
        x._2 compare y._2
    }
    //println(reducedCelledRDD.map(x => x._2).reduce(_ + _) + " sum over cells")

    def findNeighbor(x: Int, t: Int): List[(Int, Int)] = {
      /*Ugliest scala function ever written*/

      val ycc = ycells + 1
      var result = List[(Int, Int)]()

      if (x == 0) {
        // Left Bottom
        List(
          (x, t - 1),
          (x, t + 1),

          (x + 1, t - 1),
          (x + 1, t),
          (x + 1, t + 1),

          (x + ycc + 1, t - 1),
          (x + ycc + 1, t),
          (x + ycc + 1, t + 1),

          (x + ycc, t - 1),
          (x + ycc, t),
          (x + ycc, t + 1)

        )
      } else if (x == (xcells * ycc + ycells)) {
        //top right
        List(
          (x, t - 1),
          (x, t + 1),

          (x - 1, t - 1),
          (x - 1, t),
          (x - 1, t + 1),

          (x - ycc, t - 1),
          (x - ycc, t),
          (x - ycc, t + 1),

          (x - ycc - 1, t - 1),
          (x - ycc - 1, t),
          (x - ycc - 1, t + 1)

        )
      } else if (x == (ycells)) {
        //left top
        List(
          (x, t - 1),
          (x, t + 1),

          (x - 1, t - 1),
          (x - 1, t),
          (x - 1, t + 1),

          (x + ycc, t - 1),
          (x + ycc, t),
          (x + ycc, t + 1),

          (x + ycc - 1, t - 1),
          (x + ycc - 1, t),
          (x + ycc - 1, t + 1)

        )
      } else if (x == (xcells * ycc)) {
        //right bottom
        List(
          (x, t - 1),
          (x, t + 1),

          (x + 1, t - 1),
          (x + 1, t),
          (x + 1, t + 1),

          (x - ycc, t - 1),
          (x - ycc, t),
          (x - ycc, t + 1),

          (x - ycc + 1, t - 1),
          (x - ycc + 1, t),
          (x - ycc + 1, t + 1)

        )
      } else if (x % ycc == 0) {
        //Bottom gutter
        List(
          (x, t - 1),
          (x, t + 1),

          (x + 1, t - 1),
          (x + 1, t),
          (x + 1, t + 1),

          (x + ycc, t - 1),
          (x + ycc, t),
          (x + ycc, t + 1),

          (x + ycc + 1, t - 1),
          (x + ycc + 1, t),
          (x + ycc + 1, t + 1),

          (x - ycc, t - 1),
          (x - ycc, t),
          (x - ycc, t + 1),

          (x - ycc + 1, t - 1),
          (x - ycc + 1, t),
          (x - ycc + 1, t + 1)
        )
      } else if ((1 until xcells).contains(x)) {
        //Left gutter
        List(
          (x, t - 1),
          (x, t + 1),

          (x + 1, t - 1),
          (x + 1, t),
          (x + 1, t + 1),

          (x - 1, t - 1),
          (x - 1, t),
          (x - 1, t + 1),

          (x + ycc, t - 1),
          (x + ycc, t),
          (x + ycc, t + 1),

          (x + ycc + 1, t - 1),
          (x + ycc + 1, t),
          (x + ycc + 1, t + 1),

          (x + ycc - 1, t - 1),
          (x + ycc - 1, t),
          (x + ycc - 1, t + 1)

        )
      } else if ((xcells * ycc to xcells * ycc + ycells).contains(x)) {
        //Right gutter
        List(
          (x, t - 1),
          (x, t + 1),

          (x + 1, t - 1),
          (x + 1, t),
          (x + 1, t + 1),

          (x - 1, t - 1),
          (x - 1, t),
          (x - 1, t + 1),

          (x - ycc, t - 1),
          (x - ycc, t),
          (x - ycc, t + 1),

          (x - ycc + 1, t - 1),
          (x - ycc + 1, t),
          (x - ycc + 1, t + 1),

          (x - ycc - 1, t - 1),
          (x - ycc - 1, t),
          (x - ycc - 1, t + 1)

        )
      } else if ((x + 1) % ycc == 0) {
        //top gutter
        List(
          (x, t - 1),
          (x, t + 1),

          (x - 1, t - 1),
          (x - 1, t),
          (x - 1, t + 1),

          (x - ycc, t - 1),
          (x - ycc, t),
          (x - ycc, t + 1),

          (x - ycc - 1, t - 1),
          (x - ycc - 1, t),
          (x - ycc - 1, t + 1),

          (x + ycc, t - 1),
          (x + ycc, t),
          (x + ycc, t + 1),

          (x + ycc - 1, t - 1),
          (x + ycc - 1, t),
          (x + ycc - 1, t + 1))
      }
      else {

        List((x + 1, t), (x - 1, t),
          (x + ycc - 1, t), (x + ycc, t), (x + ycc + 1, t),
          (x - ycc - 1, t), (x - ycc, t), (x - ycc + 1, t),
          (x + 1, t - 1), (x - 1, t - 1), (x, t - 1),
          (x + ycc - 1, t - 1), (x + ycc, t - 1), (x + ycc + 1, t - 1),
          (x - ycc - 1, t - 1), (x - ycc, t - 1), (x - ycc + 1, t - 1),
          (x + 1, t + 1), (x - 1, t + 1), (x, t + 1),
          (x + ycc - 1, t + 1), (x + ycc, t + 1), (x + ycc + 1, t + 1),
          (x - ycc - 1, t + 1), (x - ycc, t + 1), (x - ycc + 1, t + 1)
        )
      }
    }

    def checkTimeBounds(x: (Int, Int)): Boolean = {
      x._2 >= 1 && x._2 <= 31
    }
    // Common constants need this guy
    val xValues = reducedCelledRDD map { x => x._2 }

    val xSquareValues = xValues map { x => x.toLong } map { x => x * x }

    // Common constants
    val n = xValues.count
    //    println(n)
    val X = xValues.reduce(_ + _) / n
    //    println(X)
    //    xValues.saveAsTextFile("./xValues.csv")
    val sumXSq = xSquareValues.reduce(_ + _) / n
    //    println(sumXSq)
    val XSq = X * X
    //    println(XSq)
    val S = math.sqrt(sumXSq - XSq)
    //    println(S)


    //HashMap the cells and their values
    // ******************COLLECT*********************
    val cellValuesMap = reducedCelledRDD.collect.toMap
    // ******************COLLECT*********************

    def getis(xt: (Int, Int)): Double = {
      val neighs = findNeighbor(xt._1, xt._2).filter(checkTimeBounds) :+ (xt._1, xt._2)
      val W = neighs.size
      val denominator = S * math.sqrt((n * W - W * W) / (n - 1))
      val neighVals = neighs.map(y => cellValuesMap.get(y))
      val nrSecondHalf = X * W
      val nrFirstHalf = neighVals.foldLeft(0)((a, x) => a + (if (x.isEmpty) 0 else x.get))
      val numerator = nrFirstHalf - nrSecondHalf
      numerator / denominator
    }


    val getisReducedRDD = reducedCelledRDD.map(x => (x, getis(x._1._1, x._1._2)))

    val orderGetis = new Ordering[(((Int, Int), Int), Double)]() {
      override def compare(x: (((Int, Int), Int), Double), y: (((Int, Int), Int), Double)): Int =
        x._2 compare y._2
    }

    val outputFile = "./takeOrdered.csv"

    //    val file = new File(outputFile)
    //    val bw = new BufferedWriter(new FileWriter(file))
    // ******************COLLECT*********************
    val result: Array[(((Int, Int), Int), Double)] = getisReducedRDD.takeOrdered(50)(orderGetis.reverse)
    // ******************COLLECT*********************
    //    result.foreach(println)
    val end: Long = System.currentTimeMillis()
    println("Total processing time: " + (end - start) / 1000 + " milliseconds.")
    /**
      *
      * @param cell - (theta,t)
      * @return (lat, long, t)
      */
    def reverseCellToLatLong(cell: (Int, Int)): (Int, Int, Int) = {

      val ycc = ycells + 1
      val x = (cell._1 - (cell._1 % ycc)) / ycc
      val y = cell._1 % ycc
      val latitude = (minLatitude * 100).toInt + (x - 1)
      val longitude = (minLongitude * 100).toInt + (y + 1)

      (latitude, longitude, cell._2 - 1)
    }

    val writeToFileOutput: Array[(((Int, Int, Int)), Double)] = result.map(x => (reverseCellToLatLong((x._1._1._1, x._1._1._2)), x._2))
    writeToFileOutput map {
      x => x
    }
    writeToFileOutput.foreach(println)

    def printString(x: (((Int, Int, Int)), Double)): String = {

      val resultArr: Array[String] = Array(x._1._1.toString, x._1._2.toString, x._1._3.toString, x._2.toString)
      var resultString = resultArr.mkString(",")
      resultString + "\n"

    }
    val writeStrings: Array[String] = writeToFileOutput map {
      x => printString(x)
    }
    try {
      val bufferedWriter: BufferedWriter = new BufferedWriter(new FileWriter(new File(args(1))))

      writeStrings.foreach(x => bufferedWriter.write(x))
      bufferedWriter.flush()
      bufferedWriter.close()
    } catch {
      case ex: Exception =>
        println("Error writing to csv file.")

    }
  }

}
                  
