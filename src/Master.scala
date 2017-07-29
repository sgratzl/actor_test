import tasks.{Task, TaskType}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object Master extends App {
  val schedule = new Scheduler()

  val a = args(0).toInt
  val task = args(1)
  val b = args(2).toInt

  println(s"${a} ${task} ${b} = ")
  println("wait for it...")

  val r = Await.result(schedule(TaskType.Plus, a, b), Duration("100s"))
  println(r)
}