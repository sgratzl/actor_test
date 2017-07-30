import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, FileChannel}

import matrix.Matrix

class FS(val baseDir: String = "./data") {
  val base = new File(baseDir)

  private def use[R <: AutoCloseable, T](resource: R)(code: R => T): T =
    try
      code(resource)
    finally
      resource.close()

  private def toBinaryName(f: File): (File, String) = {
    val ex = f.getName.lastIndexOf('.')
    val baseName = f.getName.substring(0, ex)
    val parent = f.getParentFile
    val binary = new File(parent, s"$baseName.bin")
    val relativeBinaryName = binary.getAbsolutePath.substring(parent.getAbsolutePath.length + 1)
    (binary, relativeBinaryName)
  }

  def toBinaryFile(path: String): String = {
    val f = new File(base, path)
    assume(f.exists())
    val (binary, relativeBinaryName) = toBinaryName(f)
    if (binary.exists()) {
      return relativeBinaryName
    }

    use(new DataOutputStream(new FileOutputStream(binary)))( out => {
      import scala.io.Source
      Source.fromFile(f, "utf-8")
        .getLines()
        .foreach(_.split("\t")
          .map(_.toInt).foreach(out.writeInt))
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
    if (f.isDirectory) {
      collectFile(f, size)
    } else {
      readFile(f, size)
    }
  }

  def getMatrix(path: String, size: (Int, Int), rowStart: Int, rowEnd: Int, colStart: Int, colEnd: Int): Matrix = {
    val rowSize = rowEnd - rowStart
    val colSize = colEnd - colStart
    val f = new File(base, path)
    assume(f.exists() && f.isFile)
    use(new RandomAccessFile(f, "r").getChannel) { channel =>
      val rows = for (row <- rowStart until rowEnd) yield {
        val buffer = ByteBuffer.allocate(colSize * 4)
        channel.read(buffer, (row * size._2 + colStart) * 4 )
        buffer.rewind()
        val ints = buffer.asIntBuffer()
        (0 until colSize).map(ints.get)
      }
      val r = new Matrix(rows)
      //println(path, rowStart, rowSize, colStart, colEnd, r)
      r
    }
  }

  private def readFile(f: File, size: (Int, Int)): Matrix = {
    assume(f.exists() && f.isFile)
    use(new DataInputStream(new FileInputStream(f))) { in =>

      val rows = for(i <- 0 until size._1) yield {
        (0 until size._2).map(_ => in.readInt())
      }
      new Matrix(rows)
    }
  }

  def compare(path: String, m: Matrix): Boolean = {
    val f = new File(base, path)
    assume(f.exists() && f.isFile)
    use(new DataInputStream(new FileInputStream(f))) { in =>
      m.flatten.forall(_ == in.readInt())
    }
  }

  private def collectFile(f: File, size: (Int, Int)): Matrix = {
    assume(f.exists() && f.isDirectory)
    val r = Matrix.empty(size._1, size._2)
    for (sub <- f.listFiles().sortBy(_.getName)) {
      val indices = sub.getName.split("[;-]").map(_.toInt).toIndexedSeq
      val rowStart = indices(0)
      val colStart = indices(1)
      val rowSize = indices(2)
      val colSize = indices(3)

      val subM = readFile(sub, (rowSize, colSize))
      //println(rowStart, rowSize, colStart, colSize, subM)
      for ((row, i) <- subM.zipWithIndex; (v, j) <- row.zipWithIndex) {
        r(i + rowStart)(j + colStart) = v
      }
    }
    Matrix(r)
  }

  def addMatrix(path: String, rowStart: Int, rowEnd: Int, colStart: Int, colEnd: Int, m: Matrix) {
    val rowSize = rowEnd - rowStart
    val colSize = colEnd - colStart
    val name = Symbol(s"$path/%05d-%05d;%05d;%05d".format(rowStart, colStart, rowSize, colSize))

    //ensure just one thread at a time
    name.synchronized({
      val f = new File(base, name.name)
      f.getParentFile.mkdirs()
      f.createNewFile()
      assume(f.exists() && f.isFile)
      use(new RandomAccessFile(f, "rw").getChannel) { channel =>
        //ensure just one process at a time
        use(channel.lock()) { _ =>
          val out = new DataOutputStream(Channels.newOutputStream(channel))
          if (channel.size() == 0) {
            //first time
            m.foreach(_.foreach( v => {
              out.writeInt(v)
            }))
          } else {
            val in = new DataInputStream(Channels.newInputStream(channel))
            m.foreach(_.foreach( v => {
              val old = in.readInt()
              channel.position(channel.position() - 4) //revert read
              out.writeInt(old + v)
            }))
          }
          channel.position(0) //reset
        }
      }
    })
  }
}
