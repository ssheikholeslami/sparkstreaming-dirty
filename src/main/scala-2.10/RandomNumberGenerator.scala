import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

/**
  * Created by sinash on 5/12/16.
  * Based on example code from "Machine Learning with Spark" book by Nick Pentreath, Packt Publishing
  */
object RandomNumberGenerator {
  var counter = 1

  def main(args: Array[String]) {
    val random = new Random()

    if (args.length < 4) {
      System.err.println("Usage: <port> <max value> <events per second> <indexed?: y/n>")
      System.exit(1)
    }
    //create a network producer
    val listener = new ServerSocket(args(0).toInt)
    println("listening on 9999")
    var sleepTime = 100 //default, 10 events per second
    if (args(2).toInt != 0) {
      sleepTime = 1000 / args(2).toInt
    }
    if(args(3).toString.equalsIgnoreCase("y")) {
      while (true) {
        val socket = listener.accept()
        counter = 1
        new Thread() {
          override def run = {
            println("client connected from: " + socket.getInetAddress)
            val out = new PrintWriter(socket.getOutputStream(), true)

            while (true) {

              Thread.sleep(sleepTime)
              val num = random.nextInt(args(1).toInt)
              out.print(num + " " + counter)
              counter = counter + 1
              out.write("\n")
              out.flush()
            }
            socket.close()
          }

        }.start()

      }
    }
    else{
      var num = 1
      while (true) {
        val socket = listener.accept()
        new Thread() {
          override def run = {
            println("client connected from: " + socket.getInetAddress)
            val out = new PrintWriter(socket.getOutputStream(), true)
            while (true) {
              Thread.sleep(sleepTime)
//              val num = random.nextInt(args(1).toInt)
              num += 1
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
}
