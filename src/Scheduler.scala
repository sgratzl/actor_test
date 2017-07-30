import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel.open
import java.nio.channels.{AsynchronousSocketChannel, Channels, CompletionHandler}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Semaphore}
import java.util.function.Consumer

import scala.concurrent.{ExecutionException, Future, Promise}
import tasks._

import scala.util.Random

class Scheduler(val port: Int = 9000, val paralleTasksPerSlave: Int = 5) {
  private val listener = open().bind(new InetSocketAddress(port))

  private val tasks = new TaskQueue()
  private val delayed = new ConcurrentLinkedQueue[Task]()
  private var noMoreDelayed = false

  case class Slave(channel: AsynchronousSocketChannel) {
    val active = new Semaphore(paralleTasksPerSlave)
    val running = new ConcurrentHashMap[Int, Task]()


    val writer = new Runnable {
      override def run(): Unit = {
        try {
          val out = new ObjectOutputStream(Channels.newOutputStream(channel))
          while (true) {
            active.acquire()
            val t = tasks.take()
            running.put(t.uid, t)
            //println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} send task ${running.size()}: $t")
            out.writeInt(t.uid)
            out.writeObject(t.args)
            out.flush()
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
          val in = new ObjectInputStream(Channels.newInputStream(channel))
          while (true) {
            try {
              val uid = in.readInt()
              val task = running.remove(Math.abs(uid))
              active.release()
              if (uid < 0) {
                println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} got task result: $task failure")
                task.promise failure null
              } else {
                val result = in.readObject()
                //println(s"${channel.getRemoteAddress}m: ${Thread.currentThread()} got task result: $task $result")
                task.promise success result.asInstanceOf[AnyRef]
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

      val s = Slave(channel)
      println(s"${channel.getRemoteAddress}m: Hello master")
      new Thread(s.writer, s"WriterSlaveOf${channel.getRemoteAddress}").start()
      new Thread(s.reader, s"ReaderSlaveOf${channel.getRemoteAddress}").start()
    }

    def failed(e: Throwable, att: Void) {
      e.printStackTrace()
    }
  })

  def apply[P<:AnyRef, T](args: P): Future[T] = {
    val p = Promise[T]()

    val t = Task(counter.incrementAndGet(), args, p.asInstanceOf[Promise[AnyRef]])

    println(s"schedule ${Thread.currentThread()} $t")
    tasks.add(t)

    p.future
  }

  def delay[P<:AnyRef, T](args: P): Future[T] = {
    val p = Promise[T]()

    val t = Task(counter.incrementAndGet(), args, p.asInstanceOf[Promise[AnyRef]])

    delayed.synchronized({
      if (noMoreDelayed) {
        println(s"schedule ${Thread.currentThread()} $t")
        tasks.add(t)
      } else {
        println(s"schedule delayed ${Thread.currentThread()} $t")
        delayed.add(t)
      }
    })

    p.future
  }

  def shuffleAndInsertDelayed(): Unit = {
    delayed.synchronized({
      noMoreDelayed = true
      val shuffled = Random.shuffle((0 until delayed.size()).map(_ => delayed.poll()))
      delayed.clear()
      shuffled.foreach(tasks.add)
    })
  }
}
