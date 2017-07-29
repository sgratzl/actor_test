import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, SQLTransactionRollbackException}
import java.util.function.Supplier

import matrix.Matrix

import scala.collection.mutable


class DB(val host: String = "db") {
  private val driver = "org.apache.derby.jdbc.EmbeddedDriver"

  Class.forName(driver).newInstance()

  private val conn = ThreadLocal.withInitial(new Supplier[Connection]() {
    override def get(): Connection = {
      val c = DriverManager.getConnection(s"jdbc:derby://$host:1527/matrixStore;create=true", null)
      //c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
      c
    }

  })
  private val cellQuery = () => {
    val s = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND i = ? AND j = ?")
    s.closeOnCompletion()
    s
  }
  private val rowQuery = () => {
    val s = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND i = ? ORDER BY j")
    s.closeOnCompletion()
    s
  }
  private val colQuery = () => {
    val s = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND j = ? ORDER BY i")
    s.closeOnCompletion()
    s
  }
  private val matrixQuery = () => {
    val s = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND i >= ? AND i < ? AND j >= ? AND j < ? ORDER BY i, j")
    s.closeOnCompletion()
    s
  }
  private val matrixFullQuery = () => {
    val s = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? ORDER BY i, j")
    s.closeOnCompletion()
    s
  }

  private val insertQuery = () => {
    val s = conn.get().prepareStatement("INSERT INTO CELL VALUES(?, ?, ?, ?)")
    s.closeOnCompletion()
    s
  }
  private val addQuery = () => {
    val s = conn.get().prepareStatement("UPDATE CELL SET value = value + ? WHERE uid = ? AND i = ? AND j = ?")
    s.closeOnCompletion()
    s
  }

  use(conn.get.createStatement()) { stmt =>
    try {
      stmt.execute("CREATE TABLE CELL (uid INT NOT NULL, i INT NOT NULL, j INT NOT NULL, value INT NOT NULL, CONSTRAINT PK_CELL PRIMARY KEY (uid, i, j))")
    } catch {
      case e: SQLTransactionRollbackException if e.getMessage.startsWith("Table/View 'CELL' already exists in Schema 'APP'") => println("table already exists", e)
      case e: Exception =>
        println("unknown error ", e)
        e.printStackTrace()
    }
  }


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
    val p = cellQuery()
    p.setInt(1, uid)
    p.setInt(2, i)
    p.setInt(3, j)
    val r = p.executeQuery()
    r.next()
    r.getInt(1)
  }

  def addCell(uid: Int, i: Int, j: Int, v: Int): Int = {
    val p = addQuery()
    p.setInt(2, uid)
    p.setInt(3, i)
    p.setInt(4, j)
    p.setInt(1, v)

    p.executeUpdate()
  }

  def getMatrix(uid: Int, size: Int): Matrix = {
    val p = matrixFullQuery()
    p.setInt(1, uid)

    val v = iterate(p.executeQuery())
      .map(_.getInt(1))
      .grouped(size)
      .map(_.toIndexedSeq)
      .toIndexedSeq
    Matrix(v)
  }

  def getMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int): Matrix = {
    //println("full",Thread.currentThread(), uid, getMatrix(uid, 8))
    val p = matrixQuery()
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
    val r = Matrix(v)
    //println("get ",Thread.currentThread(),uid, full(iFrom, iTo, jFrom, jTo, r, 8))
    r
  }

  def setMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int, m: Matrix): Int = {
    val p = insertQuery()
    p.setInt(1, uid)
    m.zipWithIndex.foreach({ case (r, i) =>
      p.setInt(2, i + iFrom)
      r.zipWithIndex.foreach({ case (v, j) =>
          p.setInt(3, j + jFrom)
          p.setInt(4, v)
          p.addBatch()
      })
    })
    val r = p.executeBatch().sum
    println("Done")
    r
  }

  def init(uid: Int, is: Int, js: Int): Int = {
    val p = insertQuery()
    p.setInt(1, uid)
    p.setInt(4, 0)
    for(i <- 0 until is; j <- 0 until js) {
      p.setInt(2, i)
      p.setInt(3, j)
      p.addBatch()
    }
    p.executeBatch().sum
  }

  def full(iFrom: Int, iTo: Int, jFrom: Int, jTo: Int, m: Matrix, s: Int): Matrix = {
    val r = Matrix.empty(s, s)
    for(i <- iFrom until iTo; j <- jFrom until jTo) {
      r(i)(j) = m(i - iFrom)(j - jFrom)
    }
    Matrix(r)
  }

  def addMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int, m: Matrix): Int = {
    //println("before",Thread.currentThread(), uid, getMatrix(uid, 0, 4, 0, 4))
    //println("add   ",Thread.currentThread(),uid, full(iFrom, iTo, jFrom, jTo, m, 8))
    val p = addQuery()
    p.setInt(2, uid)
    m.zipWithIndex.foreach({ case (r, i) =>
      p.setInt(3, i + iFrom)
      r.zipWithIndex.foreach({ case (v, j) =>
        p.setInt(4, j + jFrom)
        p.setInt(1, v)
        p.addBatch()
      })
    })
    val r = p.executeBatch().sum
    //println("after",Thread.currentThread(),uid, getMatrix(uid, 0, 4, 0, 4))
    r
  }

  def setCell(uid: Int, i: Int, j: Int, v: Int): Boolean = {
    val p = insertQuery()
    p.setInt(1, uid)
    p.setInt(2, i)
    p.setInt(3, j)
    p.setInt(4, v)
    p.executeUpdate() != 0
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
    val p = insertQuery()
    p.setInt(1, uid)
    var b = 0
    for ((row, i) <- matrix.values.zipWithIndex; (cell, j) <- row.zipWithIndex) {
      p.setInt(2, i)
      p.setInt(3, j)
      p.setInt(4, cell)
      p.addBatch()
      b += 1
      if (b > 60000) {
        p.executeBatch()
        b = 0
      }
    }
    p.executeBatch()
    println("loaded", uid) //, matrix)
  }

  def delete(uid: Int): Boolean = {
    use(conn.get.createStatement()) { stmt =>
      println("delete", uid)
      return stmt.execute(s"DELETE FROM CELL WHERE uid = $uid")
    }
  }
}
