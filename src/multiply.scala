import matrix.Matrix

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

package object multiply {
  //http://www.norstad.org/matrix-multiply/

  def local(a: Matrix, b: Matrix): Future[Matrix]  = {
    Future {
      a * b
    }
  }

  def remote(db: FS, scheduler: Scheduler, A: String, B: String, C: String, N: Int): Future[Matrix] = {
    for {
      task <- MultiplyTask(A, 0, 0, B, 0, 0, C, 0, 0, N, N)(db, scheduler)
    } yield {
      val r = db.getMatrix(C, (N,N))
      r
    }
  }


  //https://stackoverflow.com/questions/5472744/fork-join-matrix-multiplication-in-java

  val THRESHOLD = 2
  case class MultiplyTask(A: String, aRow: Int, aCol: Int, B: String, bRow: Int, bCol: Int, C: String, cRow: Int, cCol: Int, size: Int, N: Int) {
    /**
      * Multiply matrices AxB by dividing into quadrants, using algorithm:
      * <pre>
      *      A      x      B
      *
      *  A11 | A12     B11 | B12     A11*B11 | A11*B12     A12*B21 | A12*B22
      * |----+----| x |----+----| = |--------+--------| + |---------+-------|
      *  A21 | A22     B21 | B22     A21*B11 | A21*B12     A22*B21 | A22*B22
      * </pre>
      */
    def apply(db: FS, schedule: Scheduler): Future[Any] = {
      //if (size == 1) {
      //  Future(db.addCell(C, cRow, cCol, db.getCell(A, aRow, aCol) * db.getCell(B, bRow, bCol)))
      //} else
      if (size <= THRESHOLD) {
        compute(db, this)
      } else {
        val h = size / 2
        val c11_1 = MultiplyTask(
          A, aRow, aCol, // A11
          B, bRow, bCol, // B11
          C, cRow, cCol, // C11
          h, N)
        val c11_2 = MultiplyTask(
          A, aRow, aCol + h, // A12
          B, bRow + h, bCol, // B21
          C, cRow, cCol,     // C11
          h, N)
        val c12_1 = MultiplyTask(
          A, aRow, aCol, // A11
          B, bRow, bCol + h, // B12
          C, cRow, cCol + h, // C12
          h, N)
        val c12_2 = MultiplyTask(
          A, aRow, aCol + h,     // A12
          B, bRow + h, bCol + h, // B22
          C, cRow, cCol + h,     // C12
          h, N)
        val c21_1 = MultiplyTask(
          A, aRow + h, aCol, // A21
          B, bRow, bCol,     // B11
          C, cRow + h, cCol, // C21
          h, N)
        val c21_2 = MultiplyTask(
          A, aRow + h, aCol + h,  // A22
          B, bRow + h, bCol,     // B21
          C, cRow + h, cCol,     // C21
          h, N)
        val c22_1 = MultiplyTask(
          A, aRow + h, aCol,     // A21
          B, bRow, bCol + h,     // B12
          C, cRow + h, cCol + h, // C22
          h, N)
        val c22_2 = MultiplyTask(
          A, aRow + h, aCol + h, // A22
          B, bRow + h, bCol + h, // B22
          C, cRow + h, cCol + h, // C22
          h, N)

        val forks = Array(c11_1, c11_2, c12_1, c12_2, c21_1, c21_2, c22_1, c22_2).map(_(db, schedule))
        Future.reduce(forks)((_,_)=>Unit)
      }
    }
  }

  def compute(db: FS, task: MultiplyTask): Future[Int] = {
    import task._
    Future {
      val A = db.getMatrix(task.A, (N,N), aRow, aRow + size, aCol, aCol + size)
      val B = db.getMatrix(task.B, (N,N), bRow, bRow + size, bCol, bCol + size)
      val C = Matrix.empty(size, size)

      for (j <- 0 until size by 2; i <- 0 until size by 2) {
        val a0 = A(i)
        val a1 = A(i + 1)
        var s00 = 0
        var s01 = 0
        var s10 = 0
        var s11 = 0
        for (k <- 0 until size by 2) {
          val b0 = B(k)
          s00 += a0(k) * b0(j)
          s10 += a1(k) * b0(j)
          s01 += a0(k) * b0(j + 1)
          s11 += a1(k) * b0(j + 1)
          val b1 = B(k + 1)
          s00 += a0(k + 1) * b1(j)
          s10 += a1(k + 1) * b1(j)
          s01 += a0(k + 1) * b1(j + 1)
          s11 += a1(k + 1) * b1(j + 1)
        }
        C(i)(j) += s00
        C(i)(j + 1) += s01
        C(i + 1)(j) += s10
        C(i + 1)(j + 1) += s11
      }
      db.addMatrix(task.C, cRow, cRow + size, cCol, cCol + size, Matrix(C))
      size * size
    }
  }
}
