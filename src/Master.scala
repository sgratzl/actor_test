import matrix.Matrix

import multiply._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object Master extends App {
  val db = new DB(if (args.length > 0) args(0) else "db")

  //db.delete(10)
  //db.load(10, Matrix("./data/10a.csv"))

  val schedule = new Scheduler()

  val size = args(1)
  val a = Matrix(s"./data/${size}a.csv")
  val b = Matrix(s"./data/${size}b.csv")
  val c = Matrix(s"./data/${size}c.csv")
  println(s"$size: a * b = ")
  println("wait for it...")

  val f = remote(db, schedule, a, b)

  val r = Await.result(f, Duration("100s"))
  println(r == c)
}