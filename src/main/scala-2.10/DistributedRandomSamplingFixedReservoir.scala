/**
  * Created by sinash on 5/14/16.
  * Distributed version of "Random Sampling With a Fixed Reservoir (Vitter)"
  */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Random

object DistributedRandomSamplingFixedReservoir {
  //FIXME overflow may happen, if the stream is very big!
  var globalCount: Long = 1
  var reservoirSize = 10 //FIXME runtime argument!
  var sampleSet = new Array[Long](reservoirSize)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF) //disable logging

    // at least 2 cores to prevent starvation (1 core for each stream source, and at least one worker)

    //FIXME set cores from runtime arguments
    //FIXME set master address from runtime arguments
//    val conf = new SparkConf().setMaster("local[2]").setAppName("DistributedRandomSampling")

    val conf = new SparkConf().setAppName("DistributedRandomSampling")
    val ssc = new StreamingContext(conf, Seconds(10)) // batch interval duration FIXME set from runtime arguments


    //DStream receiving from localhost:9999
    //FIXME set input source via SDMiner GUI

    val stream = ssc.socketTextStream("localhost", 9999)
    //FIXME zipWithIndex or zipWithUniqueId ?
    val indexedDStream = stream.transform(rdd => rdd.zipWithUniqueId().mapValues( id => id + globalCount))

    val filteredDStream = indexedDStream.filter(element => (reservoirSize.toFloat/((element._2).toFloat)) > (new Random()).nextFloat())

    filteredDStream.foreachRDD(rdd => {
      val updateSet = rdd.collect()
      val sortedUpdateSet = updateSet.sortWith(_._2 < _._2)
      //update the sample
      sortedUpdateSet.foreach(element => {

        val replaceIndex = (new Random()).nextInt(10)
        sampleSet.update(replaceIndex, (element._1).toLong)

        println(element)
        println("UPDATED")
      })

      println("Current Sample: " + sampleSet.mkString(" "))    })

    indexedDStream.foreachRDD{(rdd => globalCount += rdd.count())} //is it safe to use indexedDStream again?

    // to start the processing
    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate
  }
}