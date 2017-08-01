import java.io._
import java.nio.ByteBuffer
import matrix.Matrix

class FS(val baseDir: String = "./data") {
  val base = new File(baseDir)

  val byteSize = 8

  private def use[R <: AutoCloseable, T](resource: R)(code: (R) => T): T =
    try
      code(resource)
    finally
      resource.close()

  private def use[R <: AutoCloseable, S <: AutoCloseable, T](r1: R, r2: S)(code: (R, S) => T): T =
    try
      code(r1, r2)
    finally {
      r1.close()
      r2.close()
    }

  private def toBinaryName(f: File): (File, String) = {
    val ex = f.getName.lastIndexOf('.')
    val baseName = f.getName.substring(0, ex)
    val parent = f.getParentFile
    val binary = new File(parent, s"$baseName.bin")
    val relativeBinaryName = binary.getAbsolutePath.substring(parent.getAbsolutePath.length + 1)
    (binary, relativeBinaryName)
  }

  def toTextFile(path: String, m: Matrix) {
    use(new PrintWriter(new FileWriter(new File(base, path))))(out => {
      m.foreach((row) => out.println(row.map((d) => "%d".format(d.toLong)).mkString("\t")))
    })
  }

  def toBinaryFile(path: String): String = {
    val f = new File(base, path)
    assume(f.exists())
    val (binary, relativeBinaryName) = toBinaryName(f)
    if (binary.exists()) {
      return relativeBinaryName
    }

    use(new DataOutputStream(new FileOutputStream(binary)))(out => {
      import scala.io.Source
      Source.fromFile(f, "utf-8")
        .getLines()
        .foreach(_.split("\t")
          .map(_.toDouble).foreach(out.writeDouble))
    })
    relativeBinaryName
  }

  def emptyBinary(path: String): String = {
    val f = new File(base, path)
    val (binary, relativeBinaryName) = toBinaryName(f)
    if (!binary.exists()) {
      return relativeBinaryName
    }
    if (binary.isDirectory) {
      binary.listFiles().foreach(_.delete())
    }
    binary.delete()
    relativeBinaryName
  }

  def getMatrix(path: String, size: (Int, Int)): Matrix = {
    val f = new File(base, path)
    assume(f.exists())
    readFile(f, size)
  }

  def getMatrix(path: String, size: (Int, Int), rowStart: Int, rowEnd: Int, colStart: Int, colEnd: Int): Matrix = {
    val rowSize = rowEnd - rowStart
    val colSize = colEnd - colStart
    val f = new File(base, path)
    assume(f.exists() && f.isFile)
    use(new RandomAccessFile(f, "r").getChannel) { channel =>
      val buffer = ByteBuffer.allocate(colSize * byteSize)
      val rows = for (row <- rowStart until rowEnd) yield {
        channel.read(buffer, (row * size._2 + colStart) * byteSize)
        buffer.rewind()
        val ints = buffer.asDoubleBuffer()
        (0 until colSize).map(_ => ints.get())
      }
      val r = new Matrix(rows)
      //println(path, rowStart, rowSize, colStart, colEnd, r)
      r
    }
  }

  private def readFile(f: File, size: (Int, Int)): Matrix = {
    assume(f.exists() && f.isFile)
    use(new DataInputStream(new BufferedInputStream(new FileInputStream(f)))) { in =>

      val rows = for (i <- 0 until size._1) yield {
        (0 until size._2).map(_ => in.readDouble())
      }
      new Matrix(rows)
    }
  }

  private def fin(f: File): DataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))

  private def fout(f: File): DataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))

  def compare(path: String, m: Matrix): Boolean = {
    val f = new File(base, path)
    assume(f.exists() && f.isFile)
    use(fin(f)) { in =>
      m.flatten.forall(_ == in.readDouble())
    }
  }

  def compare(aPath: String, bPath: String): Boolean = {
    val a = new File(base, aPath)
    assume(a.exists() && a.isFile)
    val b = new File(base, aPath)
    assume(b.exists() && b.isFile)
    if (a.length() != b.length()) {
      return false
    }
    use(fin(a), fin(b)) { (aIn, bIn) =>
      try {
        while (true) {
          if (aIn.readDouble() != bIn.readDouble()) {
            return false
          }
        }
      } catch {
        case e: EOFException => //ok
        case e: Exception => throw e
      }
    }
    true
  }

  def mergeMatrices(path: String, path11: String, path12: String, path21: String, path22: String, size: (Int, Int)) {
    val f = new File(base, path)
    f.getParentFile.mkdirs()
    f.createNewFile()
    assume(f.exists() && f.isFile)

    val colSize = size._2 / 2
    val rowSize = size._1 / 2

    use(fout(f)) {
      out =>
        for ((leftPath, rightPath) <- Array((path11, path12), (path21, path22))) {
          val left = new File(base, leftPath)
          assume(left.exists() && left.isFile)
          assume(left.length() == colSize * rowSize * byteSize, s"$leftPath ${left.length()} $colSize $rowSize ${colSize * rowSize * byteSize}")
          val right = new File(base, rightPath)
          assume(right.exists() && right.isFile)
          assume(right.length() == colSize * rowSize * byteSize, s"$rightPath ${right.length()} $colSize $rowSize ${colSize * rowSize * byteSize}")
          use(fin(left), fin(right)) { (leftIn, rightIn) =>
            var i = rowSize
            while (i > 0) {
              var j = colSize
              while (j > 0) {
                out.writeDouble(leftIn.readDouble())
                j -= 1
              }
              j = colSize
              while (j > 0) {
                out.writeDouble(rightIn.readDouble())
                j -= 1
              }
              i -= 1
            }
            out.flush()
          }
          left.delete()
          right.delete()
        }
        out.flush()
    }
    assume(f.length() == size._1 * size._2 * byteSize, s"$path ${f.length()} $size ${size._1 * size._2 * byteSize}")
  }

  def addMatrices(path: String, aPath: String, bPath: String, size: (Int, Int)) {
    val f = new File(base, path)
    f.getParentFile.mkdirs()
    f.createNewFile()
    assume(f.exists() && f.isFile)

    val a = new File(base, aPath)
    assume(a.exists() && a.isFile)
    val b = new File(base, bPath)
    assume(b.exists() && b.isFile)
    assume(a.length() == b.length())
    assume(a.length() == size._1 * size._2 * byteSize, s"$aPath ${a.length()} $size ${size._1 * size._2 * byteSize}")
    assume(b.length() == size._1 * size._2 * byteSize, s"$bPath ${b.length()} $size ${size._1 * size._2 * byteSize}")
    use(fout(f)) {
      out =>
        use(fin(a), fin(b)) { (aIn, bIn) =>
          assume((0 until (size._1 * size._2)).length == (size._1 * size._2))
          var i = size._1 * size._2
          while (i > 0) {
            val va = aIn.readDouble()
            val vb = bIn.readDouble()
            out.writeDouble(va + vb)
            i -= 1
          }
        }
        out.flush()
        a.delete()
        b.delete()
    }
    assume(f.length() == size._1 * size._2 * byteSize, s"$path ${f.length()} $size ${size._1 * size._2 * byteSize}")
  }


  def setMatrix(path: String, m: Array[Array[Double]]) {
    val f = new File(base, path)
    f.getParentFile.mkdirs()
    f.createNewFile()
    assume(f.exists() && f.isFile)

    use(fout(f)) {
      out =>
        var i = 0
        while (i < m.length) {
          val r = m(i)
          var j = 0
          while (j < r.length) {
            out.writeDouble(r(j))
            j += 1
          }
          i += 1
        }
        out.flush()
    }
    assume(f.length() == m.length * m(0).length * byteSize, s"$path ${f.length()} ${m.length} ${m(0).length} ${m.length * m(0).length * byteSize}")
  }
}
