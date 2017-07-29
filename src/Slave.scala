import java.net.InetSocketAddress
import java.nio.ByteBuffer

import scala.util.{Failure, Success}
import tasks._
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.locks.{Lock, ReentrantLock}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.control.Breaks.{break, breakable}

object Slave extends App {
  val client = AsynchronousSocketChannel.open
  println(s"${client.getLocalAddress}s start")
  client.connect(new InetSocketAddress("localhost", 9000)).get()
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
      println(s"${client.getLocalAddress}s ${Thread.currentThread()} wait for tasks")
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
        }
      } catch {
        case e:Exception =>
        println(s"${client.getLocalAddress}s ${Thread.currentThread()} master died", e)
          e.printStackTrace()
        break()
      }
    }
  }
  client.close()
}