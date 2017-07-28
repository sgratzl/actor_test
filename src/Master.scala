import matrix.{Matrix, Cannon}
import utils.loadFile


object Master extends App{
  val file = args(0)
  val a = loadFile(s"./data/${file}a.csv")
  val b = loadFile(s"./data/${file}b.csv")
  val c = loadFile(s"./data/${file}c.csv")

  val n = a.nrow

  //TODO
}