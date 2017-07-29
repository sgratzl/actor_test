import tasks.{Task, TaskType}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object Master extends App {
  val schedule = new Scheduler()

  val a = args(0).toInt
  val task = args(1)
  val b = args(2).toInt

  println(s"${a} ${task} ${b} = ")
  println("wait for it...")

  val all = for {i <- 0 to 10000} yield schedule(TaskType.Plus, i, b)

  val r = Await.result(Future.sequence(all), Duration("100s"))
  Thread.currentThread()
  println(r)
}