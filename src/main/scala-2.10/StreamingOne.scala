/**
  * Simple Streaming Word Count.
  * Created by sinash on 5/7/16.
  * Based on the Quick Start example from Apache Spark Streaming's programming guide.
  */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object StreamingOne {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF) //disable logging

    // 2 cores for master to prevent starvation
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingOne")
    val ssc = new StreamingContext(conf, Seconds(1)) // batch interval of 1 second



    //DStream receiving from localhost:9999

    val lines = ssc.socketTextStream("localhost", 9999)

    // split lines into words (DStream of words)
    val words = lines.flatMap(_.split(" ")) //flatMap : one-to-many

    //count each word in each batch

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()


    // to start the processing

    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate

  }

}

