import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel.open
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}

import scala.concurrent.{ExecutionException, Future, Promise}
import tasks._

class Scheduler(val port: Int = 9000) {
  private val listener = open().bind(new InetSocketAddress(port))

  private val tasks = new TaskQueue()

  case class Slave(channel: AsynchronousSocketChannel, tasks: TaskQueue) extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          val t = tasks.take()

          println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} send task $t")
          channel.write(t.byteBuffer).get()
          var readBuffer = TaskResult.newResultByteBuffer
          try {
            readBuffer.rewind()
            channel.read(readBuffer).get()
            readBuffer.flip()
            val tasksId = readBuffer.getInt()
            readBuffer.rewind()
            if (tasksId < 0) {
              println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} got task result: failure")
              t.promise failure null
            } else {
              val result = TaskResult(readBuffer)
              println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} got task result: $result")
              t.promise success result.c
            }
          } catch {
            case _: ExecutionException =>
              //slave dead reschedule task
              println(s"${channel.getRemoteAddress}m: dead: reschedule $t")
              tasks.addFirst(t)
              return
          }
        }
      } finally {
        channel.close()
      }
    }
  }

  println(s"start server at ${listener.getLocalAddress}")
  listener.accept(null, new CompletionHandler[AsynchronousSocketChannel, Void]() {
    def completed(channel: AsynchronousSocketChannel, att: Void) {
      listener.accept(null, this)

      val s = Slave(channel, tasks)
      println(s"${channel.getRemoteAddress}m: Hello master")
      new Thread(s, s"SlaveOf${channel.getRemoteAddress}").start()
    }

    def failed(e: Throwable, att: Void) {
      e.printStackTrace()
    }
  })

  def apply(task: TaskTypeValue, a: Int, b: Int): Future[Int] = {
    val p = Promise[Int]()

    val t = Task(task, a, b, p)

    println(s"schedule ${Thread.currentThread()} $t")
    tasks.add(t)

    p.future
  }
}
