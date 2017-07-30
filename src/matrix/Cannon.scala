package matrix

import scala.collection.mutable

object Cannon {
  type Ints = IndexedSeq[Double]
  type IntsInts = IndexedSeq[Ints]

  def apply(a: Matrix, b: Matrix): Matrix = {
    val n = a.nrow
    def circularShift(v: IntsInts): IntsInts = {
      v.zipWithIndex.map((v) => {
        val (row, i) = v
        row.drop(i) ++ row.take(i)
      })
    }
    def circularShift1(v: IntsInts): IntsInts = {
      v.map((r) => r.drop(1) ++ r.take(1))
    }
    def transpose(v: IntsInts): IntsInts = {
      (0 until n).map((i) => v.map(_(i)))
    }

    var aShifted = circularShift(a.values)
    var bShiftedTransposed = circularShift(transpose(b.values))

    val c = Matrix.empty(b.nrow, a.ncol)

    for (k <- 1 to n) {
      for (i <- 0 until n; j <- 0 until n) {
        c(i)(j) += aShifted(i)(j) * bShiftedTransposed(j)(i)
      }
      aShifted = circularShift1(aShifted)
      bShiftedTransposed = circularShift1(bShiftedTransposed)
    }
    Matrix(c.map(_.toIndexedSeq))
  }
}


