/**
  * Created by sinash on 5/9/16.
  *
  * A simple Spark Streaming program that prints and counts stream input elements in intervals of 10 seconds
  * To use it, run netcat on port 9999: nc -lk 9999
  */


import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level



object IntervalPrinter {

  var intervalCount = 1 // FIXME should use accumulators! , cluster mode consideration
  var elementCount = 0 // FIXME should use accumulators! , cluster mode consideration

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF) //disable logging


    // 2 cores for master to prevent starvation
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingOne")
    val ssc = new StreamingContext(conf, Seconds(10)) // batch interval of 10 seconds

    //DStream receiving from localhost:9999

    val lines = ssc.socketTextStream("localhost", 9999)

    lines.foreachRDD{ (rdd, time) =>

      println("Batch " + intervalCount + " contents: ")
      rdd.foreach(element => {

        println(element)
        elementCount = elementCount + 1
      })

      intervalCount = intervalCount + 1
      println("element count: " + elementCount)

    }
    // to start the processing

    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate

  }



}
