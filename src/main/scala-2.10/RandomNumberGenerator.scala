import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

/**
  * Created by sinash on 5/12/16.
  * Using an example code from "Machine Learning with Spark" book by Nick Pentreath, Packt Publishing
  */
object RandomNumberGenerator {


  def main(args: Array[String]) {
    val random = new Random()
//    random.setSeed(1)
    val maxEventsPerSecond = 5

    //create a network producer
    val listener = new ServerSocket(9999)
    println("listening on 9999")

    while(true){
      val socket = listener.accept()

      new Thread(){
        override def run = {
          println("client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while(true){
            Thread.sleep(1)
            val num = random.nextInt(10)
            out.print(num)
            out.write("\n")
            out.flush()
          }
          socket.close()
        }

      }.start()

    }
  }
}
