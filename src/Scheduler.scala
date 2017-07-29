import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel.open
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}

import scala.concurrent.{Future, Promise}
import tasks._

class Scheduler(val port: Int = 9000) {
  private val listener = open().bind(new InetSocketAddress(port))

  private val tasks = new TaskQueue()

  case class Slave(channel: AsynchronousSocketChannel, tasks: TaskQueue) extends Runnable {
    override def run(): Unit = {

      while(true) {
        val t = tasks.poll()

        channel.write(t.byteBuffer).get()
        var readBuffer = TaskResult.newResultByteBuffer
        channel.read(readBuffer).get()

        val tasksId = readBuffer.getInt
        if (tasksId < 0)
          t.promise failure null
        else {
          val result = TaskResult(readBuffer)
          t.promise success result.c
        }
      }
    }
  }

  listener.accept(null, new CompletionHandler[AsynchronousSocketChannel,Void]() {
    def completed(channel: AsynchronousSocketChannel, att: Void) {
      listener.accept(null, this)

      val s = Slave(channel, tasks)
      new Thread(s).start()
    }

    def failed(e: Throwable, att: Void) {
      e.printStackTrace()
    }
  })

  def apply(task: TaskTypeValue, a: Int, b: Int): Future[Int] = {
    val p = Promise[Int]()

    val t = Task(task, a, b, p)
    tasks.add(t)

    p.future
  }
}
