/**
  * Created by sinash on 5/14/16.
  */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Random

object DistributedRandomSamplingFixedReservoir {
  //FIXME overflow may happen, if the stream is very big!
  var globalCount: Long = 1
  var reservoirSize = 10

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF) //disable logging


    // at least 2 cores to prevent starvation (1 core for each stream source, and at least one worker)

    //FIXME set cores from runtime arguments
    //FIXME set master address from runtime arguments
    val conf = new SparkConf().setMaster("local[2]").setAppName("DistributedRandomSampling")
    val ssc = new StreamingContext(conf, Seconds(10)) // batch interval duration FIXME set from runtime arguments


    //DStream receiving from localhost:9999
    //FIXME set input source via SDMiner GUI

    val stream = ssc.socketTextStream("localhost", 9999)
    //FIXME zipWithIndex or zipWithUniqueId ?
    val indexedStream = stream.transform(rdd => rdd.zipWithUniqueId().mapValues( id => id + globalCount))
    //    indexedStream.filter(element => element._2 % 5 == 0 )
    //      indexedStream.transform(rdd => rdd.filter(element => element._2 % 5 == 0)).print()
//    indexedStream.transform(rdd => rdd.map(element => (new Random()).nextFloat())).print()
//        indexedStream.saveAsTextFiles("/home/sinash/elements.txt")

    val filteredDStream = indexedStream.filter(element => (reservoirSize.toFloat/((element._2).toFloat)) > (new Random()).nextFloat())
//val tripletDStream = indexedStream.map(element => new Triplet(element._1, element._2, (new Random()).nextFloat()))
//    val filteredDStream = indexedStream.filter(element => element._2 % 5 == 0 )
//val filteredDStream = tripletDStream.filter(element => (reservoirSize.toFloat/((element.value).toFloat)) < (new Random()).nextFloat())
//    val filteredDStream = tripletDStream.map(element => element.index)
      filteredDStream.saveAsTextFiles("/home/sinash/sparktest/elements.txt")
    indexedStream.foreachRDD{(rdd => globalCount += rdd.count())} //is it safe to use indexedStream again?





    // to start the processing
    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate

  }



}
