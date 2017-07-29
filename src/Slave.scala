import java.io._
import java.net.InetSocketAddress

import scala.util.{Failure, Success}
import tasks._
import java.nio.channels.AsynchronousSocketChannel

import scala.concurrent.ExecutionContext.Implicits.global

object Slave extends App {
  val client = AsynchronousSocketChannel.open
  client.connect(new InetSocketAddress("master", 9000)).get()

  var byteBuffer = Task.newResultByteBuffer

  while (true) {
    client.read(byteBuffer).get()

    val task = Task(byteBuffer)

    task.compute() onComplete {
      case Success(r) =>
        client.write(r.byteBuffer).get()
      case Failure(TaskException(r)) =>
        client.write(r.byteBuffer)
    }
  }
}