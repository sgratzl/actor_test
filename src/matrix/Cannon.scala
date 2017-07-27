package matrix

import scala.collection.mutable

object Cannon {
  type Ints = IndexedSeq[Int]
  type IntsInts = IndexedSeq[Ints]

  def compute(a: Matrix, b: Matrix) = {
    val n = 3
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

    val c = (0 until b.nrow).map((_) => mutable.IndexedSeq.fill(a.ncol)(0)).toIndexedSeq

    for (k <- 1 to n) {
      for (i <- 0 until n; j <- 0 until n) {
        c(i)(j) += aShifted(i)(j) * bShiftedTransposed(j)(i)
        aShifted = circularShift1(aShifted)
        bShiftedTransposed = circularShift1(bShiftedTransposed)
      }
    }
  }
}


