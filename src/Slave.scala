import java.net.InetSocketAddress
import java.nio.ByteBuffer

import scala.util.{Failure, Success}
import tasks._
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.locks.{Lock, ReentrantLock}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionException
import scala.util.control.Breaks.{break, breakable}

object Slave extends App {
  val hostname = if (args.length > 0) args(0) else "master"
  val db = new DB(if (args.length > 1) args(1) else "db")

  val client = AsynchronousSocketChannel.open
  println(s"${client.getLocalAddress}s start")

  var tries = 0
  breakable {
    for( i <- 0 until 10) {
      try {
        client.connect(new InetSocketAddress(hostname, 9000)).get()
        //got it
        break()
      } catch {
        case e:ExecutionException => println(s"try $i can not find master")
      }
      // sleep for 2 seconds
      Thread.sleep(2000)
    }
    //out of tries
    println("no master found after 10 tries")
    System.exit(1)
  }
  println(s"${client.getLocalAddress}s ${Thread.currentThread()} connected to master: ${client.getRemoteAddress}")

  val byteBuffer = Task.newResultByteBuffer

  val lock = new ReentrantLock()

  def write(arr: ByteBuffer) {
    lock.lock();  //write sync
    try {
      client.write(arr).get()
    } finally {
      lock.unlock()
    }
  }

  breakable {
    while (true) {
      //println(s"${client.getLocalAddress}s ${Thread.currentThread()} wait for tasks")
      byteBuffer.rewind()
      try {
        val num = client.read(byteBuffer).get()
        if (num == 0) {
          break()
        }
        byteBuffer.flip()
        val task = Task(byteBuffer)
        println(s"${client.getLocalAddress}s ${Thread.currentThread()} got task: $task")

        task.compute() onComplete {
          case Success(r) =>
            println(s"${client.getLocalAddress}s ${Thread.currentThread()} success: $r")
            write(r.byteBuffer)
          case Failure(TaskException(r)) =>
            println(s"${client.getLocalAddress}s ${Thread.currentThread()} failure: $r")
            write(r.byteBuffer)
          case Failure(e) =>
            println(s"${client.getLocalAddress}s ${Thread.currentThread()} unknown error: $e")
            e.printStackTrace()
            write(InvalidTaskResult(task.uid).byteBuffer)
        }
      } catch {
        case e:Exception =>
        println(s"${client.getLocalAddress}s ${Thread.currentThread()} master died", e)
        //e.printStackTrace()
        break()
      }
    }
  }
  client.close()
}