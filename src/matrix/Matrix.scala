package matrix

import scala.collection.mutable

case class Matrix(values: IndexedSeq[IndexedSeq[Double]]) extends IndexedSeq[IndexedSeq[Double]] {
  val nrow: Int = values.length
  val ncol: Int = if (nrow > 0) values(0).length else 0

  override def seq: IndexedSeq[IndexedSeq[Double]] = values
  val length: Int = values.length

  def apply(idx:Int): IndexedSeq[Double] = values(idx)

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

  override def toString(): String = values.map(_.map("%5f".format(_)).mkString(" ")).mkString("\t")
}

object Matrix {
  val zero = 0.0

  def apply(file: String): Matrix = {
    import scala.io.Source
    val values = Source.fromFile(file, "utf-8").getLines().map(_.split("\t").map(_.toDouble).toIndexedSeq).toIndexedSeq
    Matrix(values)
  }

  def empty(rows: Int, cols: Int): IndexedSeq[mutable.IndexedSeq[Double]] = {
    (0 until rows).map((_) => mutable.IndexedSeq.fill(cols)(zero))
  }
}
