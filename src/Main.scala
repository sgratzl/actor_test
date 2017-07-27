import matrix.Matrix


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

    if (slaves == 0) {
      val ab = a * b
      println(ab == c)
      writeFile(s"./data/${file}ab.csv", ab)
    } else {
      import matrix.Master

      new Master(slaves).start()
    }
  }

  def runSlave(slave: Int): Unit = {
    import matrix.Slave
    println("Starting Slave")
    new Slave(slave).start()
  }

  def main(args: Array[String]) {
    val (mode, slaves, file) = parseArgs(args)
    println(mode, slaves, file)

    mode match {
      case "master" => runMaster(slaves, file)
      case "slave" => runSlave(slaves)
    }
  }

  def parseArgs(args: Array[String]): (String, Int, String) = {
    if (args.length == 0) {
      println(usage)
    }

    var mode = "master"
    var slaves = 0
    var file = ""
    args.sliding(2, 2).toList.collect {
      case Array("--mode", modeN: String) => mode = modeN
      case Array("--slaves", slavesN: String) => slaves = slavesN.toInt
      case Array("--file", fileN: String) => file = fileN
    }

    (mode, slaves, file)
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