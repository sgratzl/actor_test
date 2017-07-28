import matrix.{Matrix, Cannon}


object Main {
  type OptionMap = Map[Symbol, Any]
  val usage =
    """
    Usage: matrix [--slaves <num>] --mode <(master|slave)> --file <file>
  """

  def runMaster(slaves: Int, file: String): Unit = {
    val a = loadFile(s"./data/${file}a.csv")
    val b = loadFile(s"./data/${file}b.csv")
    val c = loadFile(s"./data/${file}c.csv")

    val n = a.nrow

    if (slaves == 0) {
      val ab = a * b
      val ab2 = Cannon(a, b)
      println(s"$n: A * B = C ${if (ab == c) "CORRECT" else "INCORRECT"}")
      println(s"$n: CANNON A * B = C ${if (ab2 == c) "CORRECT" else "INCORRECT"}")
      //writeFile(s"./data/${file}ab.csv", ab)
    } else {
      import matrix.Master

      def onResult(ab: Matrix) {
        println(s"$n: DISTRIBUTED CANNON A * B = C ${if (ab == c) "CORRECT" else "INCORRECT"}")
        //writeFile(s"./data/${file}ab.csv", ab)
      }

      new Master(slaves, a, b, onResult).start()
    }
  }

  def runSlave(index: Int, slaves: Int): Unit = {
    import matrix.Slave
    println("Starting Slave")
    new Slave().start()
  }

  def main(args: Array[String]) {
    val (mode, slaves, file, index) = parseArgs(args)
    println(mode, slaves, file)

    mode match {
      case "master" => runMaster(slaves, file)
      case "slave" => runSlave(index, slaves)
    }
  }

  def parseArgs(args: Array[String]): (String, Int, String, Int) = {
    if (args.length == 0) {
      println(usage)
    }

    var mode = "master"
    var slaves = 0
    var index = 0
    var file = ""
    args.sliding(2, 2).toList.collect {
      case Array("--mode", v: String) => mode = v
      case Array("--slaves", v: String) => slaves = v.toInt
      case Array("--index", v: String) => index = v.toInt
      case Array("--file", v: String) => file = v
    }

    (mode, slaves, file, index)
  }


  def loadFile(file: String): Matrix = {
    import scala.io.Source
    val values = Source.fromFile(file, "utf-8").getLines().map(_.split("\t").map(_.toInt).toIndexedSeq).toIndexedSeq
    Matrix(values)
  }

  def writeFile(file: String, matrix: Matrix): Unit = {
    import java.io.PrintWriter
    val w = new PrintWriter(file)
    val v = matrix.values
    for (row <- v) {
      w.println(row.map(_.toString).mkString("\t"))
    }
    w.close()
  }
}