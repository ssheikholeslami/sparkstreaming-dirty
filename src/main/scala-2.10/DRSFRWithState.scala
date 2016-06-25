import java.util.Random

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext, State}

/**
  * Created by sinash on 5/27/16.
  */
object DRSFRWithState {


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
    val conf = new SparkConf().setAppName("DRSFR-WithState")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong)) // batch interval duration FIXME set from runtime arguments

    ssc.checkpoint("_checkstatepoint")

    val stream = ssc.socketTextStream(args(0), args(1).toInt)

    val parsedStream = stream.map( elem => ("sharedKey", elem.toInt))

    val initialRDD = ssc.sparkContext.parallelize(List(("sharedKey", 0)))

    val mappingFunc = (sharedKey: String, element: Option[Int], globalIndex: State[Int]) => {
      val newGlobalIndex = globalIndex.getOption.getOrElse(0) + 1
      val output = (element.getOrElse(0), newGlobalIndex)
      globalIndex.update(newGlobalIndex)
      output
    }

    val indexedStream = parsedStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    indexedStream.print()


/*
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
*/
    // to start the processing
    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate
  }

}
