import matrix.Cannon
import utils.loadFile


object Local extends App {
  val file = args(0)

  val a = loadFile(s"./data/${file}a.csv")
  val b = loadFile(s"./data/${file}b.csv")
  val c = loadFile(s"./data/${file}c.csv")

  val n = a.nrow

  val ab = a * b
  val ab2 = Cannon(a, b)
  println(s"$n: A * B = C ${if (ab == c) "CORRECT" else "INCORRECT"}")
  println(s"$n: CANNON A * B = C ${if (ab2 == c) "CORRECT" else "INCORRECT"}")
}