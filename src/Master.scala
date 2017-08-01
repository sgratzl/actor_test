import matrix.Matrix
import multiply._
import tasks.TaskCode

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration


object Master extends App {
  val db = new FS(if (args.length > 0) args(0) else "./data")

  //for simpler profiling
  //Thread.sleep(10000)

  //db.delete(10)
  //db.load(10, Matrix("./data/10a.csv"))

  val schedule = new Scheduler()

  val size = args(1).toInt

  println(s"prepare data...")
  val a = db.toBinaryFile(s"${size}a.csv")
  val b = db.toBinaryFile(s"${size}b.csv")
  val c = db.toBinaryFile(s"${size}c.csv")

  val ab = s"${size}ab"

  println(s"$size: a * b = ")
  println("wait for it...")
  val f = remote(db, (t) => schedule(t), a, b, ab, size)
  //val f = remote(db, (t) => t(db), a, b, ab, size)
  schedule.shuffleAndInsertDelayed()

  val r = Await.result(f, Duration("200s"))
  //db.toTextFile(s"${size}ab.csv", r)
  print("done comparing result: ok? ")
  println(db.compare(c, r))
}