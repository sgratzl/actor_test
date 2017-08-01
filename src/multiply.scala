import matrix.Matrix
import tasks.TaskCode

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

package object multiply {
  //http://www.norstad.org/matrix-multiply/
  val THRESHOLD = 256

  def local(a: Matrix, b: Matrix): Future[Matrix]  = {
    Future {
      a * b
    }
  }

  def remote(db: FS, scheduler: (TaskCode[String])=>Future[String], A: String, B: String, C: String, N: Int): Future[Matrix] = {
    for {
      name <- multiply(A, 0, 0, B, 0, 0, C, 0, 0, N, N, "")(db, scheduler)
    } yield {
      val r = db.getMatrix(name, (N,N))
      r
    }
  }

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
  private def multiply(A: String, aRow: Int, aCol: Int, B: String, bRow: Int, bCol: Int, C: String, cRow: Int, cCol: Int, size: Int, N: Int, suffix: String)(db: FS, schedule: (TaskCode[String])=>Future[String]): Future[String] = {
      //if (size == 1) {
      //  Future(db.addCell(C, cRow, cCol, db.getCell(A, aRow, aCol) * db.getCell(B, bRow, bCol)))
      //} else
      if (size <= THRESHOLD) {
        schedule(MultiplyTask(A, aRow, aCol, B, bRow, bCol, C, cRow, cCol, size, N, suffix))
      } else {
        val h = size / 2
        val c11_1 = multiply(
          A, aRow, aCol, // A11
          B, bRow, bCol, // B11
          C, cRow, cCol, // C11
          h, N, suffix + "1")(db, schedule)
        val c11_2 = multiply(
          A, aRow, aCol + h, // A12
          B, bRow + h, bCol, // B21
          C, cRow, cCol,     // C11
          h, N, suffix + "2")(db, schedule)
        val c12_1 = multiply(
          A, aRow, aCol, // A11
          B, bRow, bCol + h, // B12
          C, cRow, cCol + h, // C12
          h, N, suffix + "1")(db, schedule)
        val c12_2 = multiply(
          A, aRow, aCol + h,     // A12
          B, bRow + h, bCol + h, // B22
          C, cRow, cCol + h,     // C12
          h, N, suffix + "2")(db, schedule)
        val c21_1 = multiply(
          A, aRow + h, aCol, // A21
          B, bRow, bCol,     // B11
          C, cRow + h, cCol, // C21
          h, N, suffix + "1")(db, schedule)
        val c21_2 = multiply(
          A, aRow + h, aCol + h,  // A22
          B, bRow + h, bCol,     // B21
          C, cRow + h, cCol,     // C21
          h, N, suffix + "2")(db, schedule)
        val c22_1 = multiply(
          A, aRow + h, aCol,     // A21
          B, bRow, bCol + h,     // B12
          C, cRow + h, cCol + h, // C22
          h, N, suffix + "1")(db, schedule)
        val c22_2 = multiply(
          A, aRow + h, aCol + h, // A22
          B, bRow + h, bCol + h, // B22
          C, cRow + h, cCol + h, // C22
          h, N, suffix + "2")(db, schedule)

        val c11 = add(c11_1, c11_2, C, cRow, cCol, h, suffix)(db, schedule)
        val c12 = add(c12_1, c12_2, C, cRow, cCol + h, h, suffix)(db, schedule)
        val c21 = add(c21_1, c21_2, C, cRow + h, cCol, h, suffix)(db, schedule)
        val c22 = add(c22_1, c22_2, C, cRow + h, cCol + h, h, suffix)(db, schedule)

        merge(c11, c12, c21, c22, C, cRow, cCol, size, suffix)(db, schedule)
      }
  }

  private def add(A: Future[String], B: Future[String], C: String, cRow: Int, cCol: Int, size: Int, suffix: String)(db: FS, schedule: (TaskCode[String])=>Future[String]): Future[String] = {
    for {
      nameA <- A
      nameB <- B
      r <- schedule(AddTask(nameA, nameB, C, cRow, cCol, size, suffix))
    } yield {
      r
    }
  }

  private def merge(c11: Future[String], c12: Future[String], c21: Future[String], c22: Future[String], C: String, cRow: Int, cCol: Int, size: Int, suffix: String)(db: FS, schedule: (TaskCode[String])=>Future[String]): Future[String] = {
    for {
      nameC11 <- c11
      nameC12 <- c12
      nameC21 <- c21
      nameC22 <- c22
      r <- schedule(MergeTask(nameC11, nameC12, nameC21, nameC22, C, cRow, cCol, size, suffix))
    } yield {
      r
    }
  }

  //https://stackoverflow.com/questions/5472744/fork-join-matrix-multiplication-in-java

  case class MultiplyTask(A: String, aRow: Int, aCol: Int, B: String, bRow: Int, bCol: Int, C: String, cRow: Int, cCol: Int, size: Int, N: Int, suffix: String) extends TaskCode[String] {
    def apply(db: FS): Future[String] = {
      Future {
        val A = db.getMatrix(this.A, (N,N), aRow, aRow + size, aCol, aCol + size).map(_.toArray).toArray
        val B = db.getMatrix(this.B, (N,N), bRow, bRow + size, bCol, bCol + size).map(_.toArray).toArray
        val C = Matrix.emptyArray(size, size)

        var j = 0
        while (j < size) {
          var i = 0
          while (i < size) {
            val a0 = A(i)
            val a1 = A(i + 1)
            var s00 = Matrix.zero
            var s01 = Matrix.zero
            var s10 = Matrix.zero
            var s11 = Matrix.zero
            var k = 0
            while (k < size) {
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
              k += 2
            }
            C(i)(j) += s00
            C(i)(j + 1) += s01
            C(i + 1)(j) += s10
            C(i + 1)(j + 1) += s11
            i += 2
          }
          j += 2
        }
        val name = s"${this.C}/%05d-%05d;%05d;%05d_%s.bin".format(cRow, cCol, size, size, suffix)
        db.setMatrix(name, C)
        //db.toTextFile(name + ".csv", db.getMatrix(name, (size, size)))
        name
      }
    }
  }

  case class MergeTask(c11: String, c12: String, c21: String, c22: String, C: String, cRow: Int, cCol: Int, size: Int, suffix: String) extends TaskCode[String] {
    def apply(db: FS): Future[String] = {
      Future {
        val name = s"${this.C}/%05d-%05d;%05d;%05d_%s.bin".format(cRow, cCol, size, size, suffix)
        db.mergeMatrices(name, c11, c12, c21, c22, (size, size))
        //db.toTextFile(name + ".csv", db.getMatrix(name, (size, size)))
        name
      }
    }
  }

  case class AddTask(A: String, B: String, C: String, cRow: Int, cCol: Int, size: Int, suffix: String) extends TaskCode[String] {
    def apply(db: FS): Future[String] = {
      Future {
        val name = s"${this.C}/%05d-%05d;%05d;%05d_%s.bin".format(cRow, cCol, size, size, suffix)
        db.addMatrices(name, A, B, (size, size))
        //db.toTextFile(name + ".csv", db.getMatrix(name, (size, size)))
        name
      }
    }
  }
}
