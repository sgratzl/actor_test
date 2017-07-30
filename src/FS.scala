import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channel, Channels, FileChannel, FileLock}

import matrix.Matrix

import scala.util.Random

class FS(val baseDir: String = "./data") {
  val base = new File(baseDir)

  val byteSize = 8

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
    use(new DataInputStream(new FileInputStream(f))) { in =>

      val rows = for (i <- 0 until size._1) yield {
        (0 until size._2).map(_ => in.readDouble())
      }
      new Matrix(rows)
    }
  }

  def compare(path: String, m: Matrix): Boolean = {
    val f = new File(base, path)
    assume(f.exists() && f.isFile)
    use(new DataInputStream(new FileInputStream(f))) { in =>
      m.flatten.forall(_ == in.readDouble())
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

  private def lock(path: File): (FileChannel, FileLock) = {
    //windows detects deadlocks on files however, not considering multi threading
    for( i <- 1 until 20) {
      val file = new RandomAccessFile(path, "rw")
      try {
        val channel = file.getChannel
        channel.force(false)
        println(path.getName, channel.hashCode())
        return (channel, channel.lock())
      } catch {
        case e:IOException if e.getMessage.startsWith("Resource deadlock would occur") =>
          file.close()
          //ok wait a random time from 0 to 4s
          val w = Random.nextInt(4000)
          println("deadlock detected wait", w)
          Thread.sleep(w)
        case e:Exception => throw e //unknown error
      }
    }
    throw new IOException("cannot aquire lock after 20 random tries")
  }

  def addMatrix(path: String, rowStart: Int, rowEnd: Int, colStart: Int, colEnd: Int, m: Array[Array[Double]]) {
    val rowSize = rowEnd - rowStart
    val colSize = colEnd - colStart
    val name = Symbol(s"$path/%05d-%05d;%05d;%05d".format(rowStart, colStart, rowSize, colSize))

    //ensure just one thread at a time
    name.synchronized({
      val f = new File(base, name.name)
      f.getParentFile.mkdirs()
      f.createNewFile()
      assume(f.exists() && f.isFile)


      var channel: FileChannel = null
      var lock: FileLock = null
      try {
        val r = this.lock(f)
        channel = r._1
        lock = r._2
        //ensure just one process at a time
        //println(Thread.currentThread(), "lock", name)
        //println(Thread.currentThread(), "locked", name)
        val out = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(channel)))
        if (channel.size() == 0) {
          //first time
          var i = 0
          while (i < rowSize) {
            val r = m(i)
            var j = 0
            while (j < colSize) {
              out.writeDouble(r(j))
              j += 1
            }
            i += 1
          }
        } else {
          val buffer = ByteBuffer.allocate(rowSize * colSize * byteSize)
          channel.read(buffer, 0)
          buffer.rewind()
          val ints = buffer.asDoubleBuffer()

          var i = 0
          while (i < rowSize) {
            val r = m(i)
            var j = 0
            while (j < colSize) {
              out.writeDouble(r(j) + ints.get())
              j += 1
            }
            i += 1
          }
        }
        out.flush()
        channel.position(0) //reset
      } finally {
        //println(Thread.currentThread(), "release", name)
        lock.release()
        if (channel.isOpen) {
          channel.close()
        }
      }
    })
  }
}
