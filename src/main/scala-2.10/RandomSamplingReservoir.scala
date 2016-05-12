/**
  * Created by sinash on 5/12/16.
  * Implementation of "Random Sampling with a Fixed Reservoir" stream mining algorithm on top of Apache Spark
  *
  */
//FIXME Random generation does not work
//FIXME incomplete code

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Random
// scala.util.Random is not serializable until scala 2.11

object RandomSamplingReservoir {
  //TODO detailed discussion on using global variables in cluster-mode

  var intervalCount = 1
  var elementCount = 0
  val reservoirSize = 10
  val sample = new Array[Int](reservoirSize)
  var randomProbability: Float = 0
  var neededProbabilityForInclusion: Float = 0
  var randomProbForReplacement: Int = 0



  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF) //disable logging



    // 2 cores for master to prevent starvation
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingOne")
    val ssc = new StreamingContext(conf, Seconds(10)) // batch interval of 10 seconds

    //random generator

    val randomGenerator = new Random
//    random.setSeed(1)


    //DStream receiving from localhost:9999

    val lines = ssc.socketTextStream("localhost", 9999)


    lines.foreachRDD { (rdd, time) =>
      println("Batch " + intervalCount + " contents: ")
      rdd.foreach(element => {

        println("***element: " + element)
        if(elementCount > reservoirSize - 1 ){

          //do probabilities here :D
          randomProbability = randomGenerator.nextFloat()
          neededProbabilityForInclusion = reservoirSize/(elementCount+1)

          println("NEEEEDED " + neededProbabilityForInclusion)

          if(randomProbability > neededProbabilityForInclusion){
            println("prob: " + randomProbability + " , needed: " + neededProbabilityForInclusion)
            randomProbForReplacement = randomGenerator.nextInt(reservoirSize)
            println("replacing index " + randomProbForReplacement)
            sample(randomProbForReplacement) = element.toInt
          }
        }
        else{
          println("added to sample, initialization")
          sample(elementCount) = element.toInt
          println("instant print: " +sample(elementCount))
        }
        elementCount = elementCount + 1
      }
      )
      intervalCount = intervalCount + 1
      println("element count: " + elementCount)

      println("Current Sample: ")
      println(sample.mkString(" "))


    }
    // to start the processing

    ssc.start()
    ssc.awaitTermination() // wait for the computation to terminate

  }


}
