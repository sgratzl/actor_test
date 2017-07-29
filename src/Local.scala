import tasks._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object Local extends App{
  val a = args(0).toInt
  val task = args(1)
  val b = args(2).toInt

  println(s"${a} ${task} ${b} = ")
  println("wait for it...")
  val r = Await.result(Task(0, TaskType.Plus, a, b).compute(), Duration("1000s"))
  println(r.c)
}