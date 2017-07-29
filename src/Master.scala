import tasks.{Task, TaskType}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object Master extends App {
  val schedule = new Scheduler()

  val count = args(0).toInt

  println(s"${count} x i * 5 = ")
  println("wait for it...")

  val all = for {i <- 0 to count} yield schedule(TaskType.values.toIndexedSeq(i % TaskType.values.size), i,5)

  val r = Await.result(Future.sequence(all), Duration("100s"))
  Thread.currentThread()
  println(r)
}