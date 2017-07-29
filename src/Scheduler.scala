import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel.open
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import scala.concurrent.{ExecutionException, Future, Promise}
import tasks._

class Scheduler(val port: Int = 9000, val paralleTasksPerSlave: Int = 5) {
  private val listener = open().bind(new InetSocketAddress(port))

  private val tasks = new TaskQueue()

  case class Slave(channel: AsynchronousSocketChannel, tasks: TaskQueue) {
    val active = new Semaphore(paralleTasksPerSlave)
    val running = new ConcurrentHashMap[Int, Task]()

    val writer = new Runnable {
      override def run(): Unit = {
        try {
          while (true) {
            active.acquire()
            val t = tasks.take()
            running.put(t.uid, t)
            println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} send task ${running.size()}: $t")
            channel.write(t.byteBuffer).get()
          }
        } catch {
          case _: ExecutionException =>
            ???
        } finally {
          channel.close()
        }
      }
    }
    val reader = new Runnable {
      override def run(): Unit = {
        try {
          var readBuffer = TaskResult.newResultByteBuffer
          while (true) {
            try {
              readBuffer.rewind()
              channel.read(readBuffer).get()
              readBuffer.flip()
              val uid = readBuffer.getInt()
              readBuffer.rewind()
              val task = running.remove(Math.abs(uid))
              active.release()
              if (uid < 0) {
                println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} got task result: failure")
                task.promise failure null
              } else {
                val result = TaskResult(readBuffer)
                println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} got task result: $result")
                task.promise success result.c
              }
            } catch {
              case _: ExecutionException =>
                import collection.JavaConverters._
                //slave dead reschedule task
                val toReschedule = running.values()
                running.clear()
                println(s"${channel.getRemoteAddress}m: dead: reschedule $toReschedule")
                toReschedule.asScala.foreach(tasks.addFirst)
                active.release(toReschedule.size())
                return
            }
          }
        } finally {
          channel.close()
        }
      }
    }
  }

  val counter = new AtomicInteger()

  println(s"start server at ${listener.getLocalAddress}")
  listener.accept(null, new CompletionHandler[AsynchronousSocketChannel, Void]() {
    def completed(channel: AsynchronousSocketChannel, att: Void) {
      listener.accept(null, this)

      val s = Slave(channel, tasks)
      println(s"${channel.getRemoteAddress}m: Hello master")
      new Thread(s.writer, s"WriterSlaveOf${channel.getRemoteAddress}").start()
      new Thread(s.reader, s"ReaderSlaveOf${channel.getRemoteAddress}").start()
    }

    def failed(e: Throwable, att: Void) {
      e.printStackTrace()
    }
  })

  def apply(task: TaskTypeValue, a: Int, b: Int): Future[Int] = {
    val p = Promise[Int]()

    val t = Task(counter.incrementAndGet(), task, a, b, p)

    println(s"schedule ${Thread.currentThread()} $t")
    tasks.add(t)

    p.future
  }
}
