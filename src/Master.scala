import matrix.Matrix
import calc._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object Master extends App {
  val db = new DB(if (args.length > 0) args(0) else "db")

  //db.delete(10)
  //db.load(10, Matrix("./data/10a.csv"))

  val schedule = new Scheduler()

  val count = args(1).toInt
  println(s"$count x i * 5 = ")
  println("wait for it...")

  val all = for {i <- 0 to count} yield schedule(parse(s"$i * 5"))

  val r = Await.result(Future.sequence(all), Duration("100s"))
  Thread.currentThread()
  println(r)
}