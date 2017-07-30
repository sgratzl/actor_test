import matrix.Matrix

import multiply._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object Master extends App {
  val db = new FS(if (args.length > 0) args(0) else "./data")

  //db.delete(10)
  //db.load(10, Matrix("./data/10a.csv"))

  val schedule = new Scheduler()

  val size = args(1).toInt

  println(s"prepare data...")
  val a = db.toBinaryFile(s"${size}a.csv")
  val b = db.toBinaryFile(s"${size}b.csv")
  val ab = db.emptyBinary(s"${size}ab.csv")

  println(s"$size: a * b = ")
  println("wait for it...")
  val f = remote(db, schedule, a, b, ab, size)

  val r = Await.result(f, Duration("200s"))
  println(r)
  val c = Matrix(s"./data/${size}c.csv")
  println(c)
  println(r == c)
}