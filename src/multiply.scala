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

  def remote(db: DB, scheduler: Scheduler, a: Matrix, b: Matrix): Future[Matrix] = {
    val A = 1
    val B = 2
    val C = 3
    val N = a.nrow
    db.delete(A)
    db.delete(B)
    db.delete(C)

    val load = Future.reduce(Array(Future(db.load(A, a)), Future(db.load(B, b), Future(db.init(C, N, N)))))((_,_)=>Unit)
    load
      .map((_) => MultiplyTask(A, 0, 0, B, 0, 0, C, 0, 0, N).apply(scheduler))
      .map((_) => db.getMatrix(C, 0, N, 0, N))
      .andThen { case r =>
        db.delete(A)
        db.delete(B)
        db.delete(C)
        r
      }
  }


  //https://stackoverflow.com/questions/5472744/fork-join-matrix-multiplication-in-java

  val THRESHOLD = 64
  case class MultiplyTask(A: Int, aRow: Int, aCol: Int, B: Int, bRow: Int, bCol: Int, C: Int, cCol: Int, cRow: Int, size: Int) {
    def apply(schedule: Scheduler): Future[Any] = {
      if (size <= THRESHOLD) {
        schedule(this)
      } else {
        val h = size / 2
        val r1 = MultiplyTask(A, aRow, aCol, // A11
          B, bRow, bCol, // B11
          C, cRow, cCol, // C11
          h)
        val r2 = MultiplyTask(A, aRow, aCol + h, // A12
          B, bRow + h, bCol, // B21
          C, cRow, cCol, h)
        val r3 = MultiplyTask(A, aRow, aCol, B, bRow, bCol + h, // B12
          C, cRow, cCol + h, // C12
          h)
        val r4 = MultiplyTask(A, aRow, aCol + h, B, bRow + h, bCol + h, // B22
          C, cRow, cCol + h, h)
        val r5 = MultiplyTask(A, aRow + h, aCol, // A21
          B, bRow, bCol, C, cRow + h, cCol, // C21
          h)
        val r6 = MultiplyTask(A, aRow + h, aCol + h, // A22
          B, bRow + h, bCol, C, cRow + h, cCol, h)
        val r7 = MultiplyTask(A, aRow + h, aCol, B, bRow, bCol + h, C, cRow + h, cCol + h, // C22
          h)
        val r8 = MultiplyTask(A, aRow + h, aCol + h, B, bRow + h, bCol + h, C, cRow + h, cCol + h, h)

        val forks = Array(r1, r2, r3, r4, r6, r6, r7, r8).map(_(schedule))
        Future.reduce(forks)((_,_)=>Unit)
      }
    }
  }

  def compute(db: DB, task: MultiplyTask): Future[Int] = {
    import task._
    Future {
      val A = db.getMatrix(task.A, aRow, aRow + size, aCol, aCol + size)
      val B = db.getMatrix(task.B, bRow, bRow + size, bCol, bCol + size)
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
    }
  }
}
