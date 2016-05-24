/**
  * Created by sinash on 5/14/16.
  * Distributed version of "Random Sampling With a Fixed Reservoir (Vitter)"
  */

import java.util.Random

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

object DistributedRandomSamplingFixedReservoir {

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: <source hostname> <source port> <interval duration> <reservoir size>")
      System.exit(1)
    }

    val reservoirSize = args(3).toInt
    var sampleSet = new Array[Long](reservoirSize)

    Logger.getLogger("org").setLevel(Level.OFF) //disable logging

    // at least 2 cores to prevent starvation (1 core for each stream source, and at least one worker)
    //    val conf = new SparkConf().setMaster("local[2]").setAppName("DistributedRandomSampling")
    val conf = new SparkConf().setAppName("DistributedRandomSampling")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong)) // batch interval duration FIXME set from runtime arguments


    //DStream receiving from localhost:9999

    val stream = ssc.socketTextStream(args(0), args(1).toInt)
    val indexedDStream = stream.transform(rdd => rdd.map(element => {
      val splitted = element.split(" ")
      (splitted(0), splitted(1).toLong)
    }))

    val filteredDStream = indexedDStream.filter(element => (reservoirSize.toFloat / ((element._2).toFloat)) > (new Random()).nextFloat())

    filteredDStream.foreachRDD(rdd => {
      val updateSet = rdd.collect()
      val sortedUpdateSet = updateSet.sortWith(_._2 < _._2)
      //update the sample
      sortedUpdateSet.foreach(element => {

        val replaceIndex = (new Random()).nextInt(reservoirSize)
        sampleSet.update(replaceIndex, (element._1).toLong)

        println("ToBeIncluded: " + element)
      })

      println("Current Sample: " + sampleSet.mkString(" "))
    })

    // to start the processing
    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate
  }
}
