import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress
import scala.util.{Failure, Success}
import calc.{compute, Equation}
import java.nio.channels.{AsynchronousSocketChannel, Channels}
import java.util.concurrent.locks.{Lock, ReentrantLock}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionException
import scala.util.control.Breaks.{break, breakable}

object Slave extends App {
  val hostname = if (args.length > 0) args(0) else "master"
  val db = new DB(if (args.length > 1) args(1) else "db")
  println("start")

  private def connect(): AsynchronousSocketChannel = {
    var tries = 0
    for(i <- 0 until 10) {
      try {
        val client = AsynchronousSocketChannel.open
        client.connect(new InetSocketAddress(hostname, 9000)).get()
        //got it
        return client
      } catch {
        case e:ExecutionException => println(s"try $i can not find master")
      }
      // sleep for 2 seconds
      Thread.sleep(2000)
    }
    //out of tries
    println("no master found after 10 tries")
    System.exit(1)
    null
  }

  val client = connect()
  println(s"${client.getLocalAddress}s ${Thread.currentThread()} connected to master: ${client.getRemoteAddress}")

  val in = new ObjectInputStream(Channels.newInputStream(client))
  val out = new ObjectOutputStream(Channels.newOutputStream(client))

  val lock = new ReentrantLock()

  def write(taskId: Int, result: AnyRef) {
    lock.lock();  //write sync
    try {
      out.writeInt(taskId)
      if (result != null) {
        out.writeObject(result)
      }
      out.flush()
    } finally {
      lock.unlock()
    }
  }

  breakable {

    while (true) {
      //println(s"${client.getLocalAddress}s ${Thread.currentThread()} wait for tasks")
      try {
        val taskId = in.readInt()
        val args = in.readObject()
        println(s"${client.getLocalAddress}s ${Thread.currentThread()} got task: $taskId $args")

        compute(db, args.asInstanceOf[Equation]) onComplete {
          case Success(r) =>
            println(s"${client.getLocalAddress}s ${Thread.currentThread()} success: $taskId $r")
            write(taskId, r.asInstanceOf[AnyRef])
          case Failure(e) =>
            println(s"${client.getLocalAddress}s ${Thread.currentThread()} failure: $taskId $e")
            e.printStackTrace()
            write(-taskId, null)
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