/**
  * Created by sinash on 5/14/16.
  */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Random

object DistributedRandomSamplingFixedReservoir {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF) //disable logging


    // at least 2 cores to prevent starvation (1 core for each stream source, and at least one worker)

    //FIXME set cores from runtime arguments
    //FIXME set master address from runtime arguments
    val conf = new SparkConf().setMaster("local[2]").setAppName("DistributedRandomSampling")
    val ssc = new StreamingContext(conf, Seconds(10)) // batch interval duration FIXME set from runtime arguments

  }



}
