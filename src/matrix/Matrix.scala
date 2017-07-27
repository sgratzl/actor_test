package matrix

case class Matrix(values: IndexedSeq[IndexedSeq[Int]]) {
  val nrow: Int = values.length
  val ncol: Int = if (nrow > 0) values(0).length else 0

  def *(matrix: Matrix): Matrix = {
    val a = this.values
    val b = matrix.values

    val values = for(i <- 0 until nrow) yield {
      val arow = a(i)
      for(j <- 0 until ncol) yield {
        val bcol = b.map(_(j))
        arow.zip(bcol).map((t) => t._1 * t._2).sum
      }
    }
    Matrix(values)
  }
}
