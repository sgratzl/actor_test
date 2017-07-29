import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, SQLTransactionRollbackException}
import java.util.function.Supplier

import matrix.Matrix

import scala.collection.mutable


class DB(val host: String = "db") {
  private val driver = "org.apache.derby.jdbc.EmbeddedDriver"

  Class.forName(driver).newInstance()

  private val conn = ThreadLocal.withInitial(new Supplier[Connection]() {
    override def get(): Connection = DriverManager.getConnection(s"jdbc:derby://$host:1527/matrixStore;create=true", null)
  })
  private val cellQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? i = ? AND j = ?")
  })
  private val rowQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? i = ? ORDER BY j")
  })
  private val colQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND j = ? ORDER BY i")
  })
  private val matrixQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND i >= ? AND i < ? AND j >= ? AND j < ? ORDER BY i, j")
  })
  private val insertQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("INSERT INTO CELL VALUES(?, ?, ?, ?)")
  })
  private val addQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("UPDATE TABLE CELL SET value = ? WHERE uid = ? AND i = ? AND j = ?")
  })


  private def use[R <: AutoCloseable, T](resource: R)(code: R => T): T =
    try
      code(resource)
    finally
      resource.close()

  private def iterate(rs: ResultSet): Iterable[ResultSet] = new Iterator[ResultSet] {
    def hasNext = rs.next()

    def next() = rs
  }.toStream

  def getCell(uid: Int, i: Int, j: Int): Int = {
    val p = cellQuery.get()
    p.setInt(1, uid)
    p.setInt(2, i)
    p.setInt(3, j)

    p.executeQuery().getInt(1)
  }

  def getRow(uid: Int, i: Int): Array[Int] = {
    val p = rowQuery.get()
    p.setInt(1, uid)
    p.setInt(2, i)

    iterate(p.executeQuery()).map(_.getInt(1)).toArray
  }

  def getCol(uid: Int, j: Int): Array[Int] = {
    val p = colQuery.get()
    p.setInt(1, uid)
    p.setInt(2, j)

    iterate(p.executeQuery()).map(_.getInt(1)).toArray
  }

  def getMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int): Matrix = {
    val p = matrixQuery.get()
    p.setInt(1, uid)
    p.setInt(2, iFrom)
    p.setInt(3, iTo)
    p.setInt(4, jFrom)
    p.setInt(5, jTo)

    val v = iterate(p.executeQuery())
      .map(_.getInt(1))
      .grouped(iTo - iFrom)
      .map(_.toIndexedSeq)
      .toIndexedSeq
    Matrix(v)
  }

  def setMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int, m: Matrix): Int = {
    val p = insertQuery.get()
    p.setInt(1, uid)
    m.zipWithIndex.foreach({ case (r, i) =>
      p.setInt(2, i + iFrom)
      r.zipWithIndex.foreach({ case (v, j) =>
          p.setInt(3, j + jFrom)
          p.setInt(4, v)
          p.addBatch()
      })
    })
    p.executeBatch().sum
  }

  def init(uid: Int, is: Int, js: Int): Int = {
    val p = insertQuery.get()
    p.setInt(1, uid)
    p.setInt(4, 0)
    for(i <- 0 until is; j <- 0 until js) {
      p.setInt(2, i)
      p.setInt(3, j)
      p.addBatch()
    }
    p.executeBatch().sum
  }


  def addMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int, m: Matrix): Int = {
    val p = addQuery.get()
    p.setInt(1, uid)
    m.zipWithIndex.foreach({ case (r, i) =>
      p.setInt(3, i + iFrom)
      r.zipWithIndex.foreach({ case (v, j) =>
        p.setInt(4, j + jFrom)
        p.setInt(2, v)
        p.addBatch()
      })
    })
    p.executeBatch().sum
  }

  def setCell(uid: Int, i: Int, j: Int, v: Int): Boolean = {
    val p = insertQuery.get()
    p.setInt(1, uid)
    p.setInt(2, i)
    p.setInt(3, j)
    p.setInt(4, v)
    p.executeUpdate() != 0
  }

  def setRow(uid: Int, i: Int, vs: Iterable[Int]): Int = {
    val p = insertQuery.get()
    p.setInt(1, uid)
    p.setInt(2, i)
    vs.zipWithIndex.foreach({ case (vi, j) =>
      p.setInt(3, j)
      p.setInt(4, vi)
      p.addBatch()
    })
    p.executeBatch().sum
  }

  def setCol(uid: Int, j: Int, vs: Iterable[Int]): Int = {
    val p = insertQuery.get()
    p.setInt(1, uid)
    p.setInt(3, j)
    vs.zipWithIndex.foreach({ case (vi, i) =>
      p.setInt(2, i)
      p.setInt(4, vi)
      p.addBatch()
    })
    p.executeBatch().sum
  }

  def save(uid: Int): Matrix = {
    use(conn.get.createStatement()) { stmt =>
      val rs = stmt.executeQuery(s"SELECT MAX(i), MAX(j) FROM CELL WHERE uid = $uid")
      val rows = rs.getInt(1)
      val cols = rs.getInt(2)
      rs.close()

      val r = (0 until rows).map((_) => mutable.IndexedSeq.fill(cols)(0)).toIndexedSeq
      val data = stmt.executeQuery(s"SELECT i, j, value FROM CELL WHERE uid = $uid")
      iterate(data).foreach((rs) => r(rs.getInt(1))(rs.getInt(2)) = rs.getInt(3))
      data.close()

      Matrix(r.map(_.toIndexedSeq))
    }
  }

  def load(uid: Int, matrix: Matrix): Unit = {
    use(conn.get.createStatement()) { stmt =>
      try {
        stmt.execute("CREATE TABLE CELL (uid INT NOT NULL, i INT NOT NULL, j INT NOT NULL, value INT NOT NULL, CONSTRAINT PK_CELL PRIMARY KEY (uid, i, j))")
      } catch {
        case e: SQLTransactionRollbackException if e.getMessage.startsWith("Table/View 'CELL' already exists in Schema 'APP'") => println("table already exists", e)
        case e: Exception =>
          println("unknown error ", e)
          e.printStackTrace()
          return
      }
    }
    val p = insertQuery.get()
    p.setInt(1, uid)
    for ((row, i) <- matrix.values.zipWithIndex; (cell, j) <- row.zipWithIndex) {
      p.setInt(2, i)
      p.setInt(3, j)
      p.setInt(4, cell)
      p.addBatch()
    }
    p.executeBatch()
  }

  def delete(uid: Int): Boolean = {
    use(conn.get.createStatement()) { stmt =>
      return stmt.execute(s"DELETE FROM CELL WHERE uid = $uid")
    }
  }
}
