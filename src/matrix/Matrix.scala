package matrix

import scala.collection.mutable

case class Matrix(values: IndexedSeq[IndexedSeq[Int]]) extends IndexedSeq[IndexedSeq[Int]] {
  val nrow: Int = values.length
  val ncol: Int = if (nrow > 0) values(0).length else 0

  override def seq: IndexedSeq[IndexedSeq[Int]] = values
  val length: Int = values.length

  def apply(idx:Int): IndexedSeq[Int] = values(idx)

  def *(matrix: Matrix): Matrix = {
    val a = this.values
    val b = matrix.values

    val values = for (i <- 0 until nrow) yield {
      val arow = a(i)
      for (j <- 0 until ncol) yield {
        val bcol = b.map(_ (j))
        arow.zip(bcol).map((t) => t._1 * t._2).sum
      }
    }
    Matrix(values)
  }

  override def toString(): String = values.map(_.map("%5d".format(_)).mkString(" ")).mkString("\t")
}

object Matrix {
  def apply(file: String): Matrix = {
    import scala.io.Source
    val values = Source.fromFile(file, "utf-8").getLines().map(_.split("\t").map(_.toInt).toIndexedSeq).toIndexedSeq
    Matrix(values)
  }

  def empty(rows: Int, cols: Int): IndexedSeq[mutable.IndexedSeq[Int]] = {
    (0 until rows).map((_) => mutable.IndexedSeq.fill(cols)(0)).toIndexedSeq
  }
}
