import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, SQLTransactionRollbackException}
import java.util.function.Supplier

import matrix.Matrix

import scala.collection.mutable


class DB(val host: String = "db") {
  private val driver = "org.apache.derby.jdbc.EmbeddedDriver"

  Class.forName(driver).newInstance()

  private val conn = ThreadLocal.withInitial(new Supplier[Connection]() {
    override def get(): Connection = {
      val c = DriverManager.getConnection(s"jdbc:derby://$host:1527/matrixDoubleStore;create=true", null)
      //c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
      c
    }
  })

  private val getQuery = () => {
    val s = conn.get().prepareStatement("SELECT value from CELL WHERE uid = ? AND i >= ? AND i < ? AND j >= ? AND j < ? ORDER BY i, j")
    s.closeOnCompletion()
    s
  }

  private val insertQuery = () => {
    val s = conn.get().prepareStatement("INSERT INTO CELL VALUES(?, ?, ?, ?)")
    s.closeOnCompletion()
    s
  }

  private val appendQuery = () => {
    val s = conn.get().prepareStatement("INSERT INTO CELL_APPENDONLY VALUES(?, ?, ?, ?)")
    s.closeOnCompletion()
    s
  }

  private val aggregateQuery = () => {
    val s = conn.get().prepareStatement("SELECT SUM(value) FROM CELL_APPENDONLY WHERE uid = ? GROUP BY i, j ORDER BY i, j")
    s.closeOnCompletion()
    s
  }

  use(conn.get.createStatement()) { stmt =>
    try {
      stmt.execute("CREATE TABLE CELL_APPENDONLY (uid INT NOT NULL, i INT NOT NULL, j INT NOT NULL, value DOUBLE NOT NULL)")
      stmt.execute("CREATE TABLE CELL (uid INT NOT NULL, i INT NOT NULL, j INT NOT NULL, value DOUBLE NOT NULL, CONSTRAINT PK_CELL PRIMARY KEY (uid, i, j))")
    } catch {
      case e: SQLTransactionRollbackException if e.getMessage.startsWith("Table/View 'CELL") => println("table already exists", e)
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

  def getMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int): Matrix = {
    //println("full",Thread.currentThread(), uid, getMatrix(uid, 8))
    val p = getQuery()
    p.setInt(1, uid)
    p.setInt(2, iFrom)
    p.setInt(3, iTo)
    p.setInt(4, jFrom)
    p.setInt(5, jTo)

    val v = iterate(p.executeQuery())
      .map(_.getDouble(1))
      .grouped(iTo - iFrom)
      .map(_.toIndexedSeq)
      .toIndexedSeq
    val r = Matrix(v)
    //println("get ",Thread.currentThread(),uid, full(iFrom, iTo, jFrom, jTo, r, 8))
    r
  }

  def getAggreatedMatrix(uid: Int, nRow: Int): Matrix = {
    //println("full",Thread.currentThread(), uid, getMatrix(uid, 8))
    val p = aggregateQuery()
    p.setInt(1, uid)

    val v = iterate(p.executeQuery())
      .map(_.getDouble(1))
      .grouped(nRow)
      .map(_.toIndexedSeq)
      .toIndexedSeq
    val r = Matrix(v)
    //println("get ",Thread.currentThread(),uid, full(iFrom, iTo, jFrom, jTo, r, 8))
    r
  }

  def addMatrix(uid: Int, iFrom: Int, iTo: Int, jFrom: Int, jTo: Int, m: Matrix): Int = {
    //println("before",Thread.currentThread(), uid, getMatrix(uid, 0, 4, 0, 4))
    //println("add   ",Thread.currentThread(),uid, full(iFrom, iTo, jFrom, jTo, m, 8))
    val p = appendQuery()
    p.setInt(2, uid)
    m.zipWithIndex.foreach({ case (r, i) =>
      p.setInt(3, i + iFrom)
      r.zipWithIndex.foreach({ case (v, j) =>
        p.setInt(4, j + jFrom)
        p.setDouble(1, v)
        p.addBatch()
      })
    })
    val r = p.executeBatch().sum
    //println("after",Thread.currentThread(),uid, getMatrix(uid, 0, 4, 0, 4))
    r
  }

  def save(uid: Int): Matrix = {
    use(conn.get.createStatement()) { stmt =>
      val rs = stmt.executeQuery(s"SELECT MAX(i), MAX(j) FROM CELL WHERE uid = $uid")
      val rows = rs.getInt(1)
      val cols = rs.getInt(2)
      rs.close()

      val r = (0 until rows).map((_) => mutable.IndexedSeq.fill(cols)(Matrix.zero)).toIndexedSeq
      val data = stmt.executeQuery(s"SELECT i, j, value FROM CELL WHERE uid = $uid")
      iterate(data).foreach((rs) => r(rs.getInt(1))(rs.getInt(2)) = rs.getDouble(3))
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
      p.setDouble(4, cell)
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

  def delete(uid: Int): Unit = {
    use(conn.get.createStatement()) { stmt =>
      println("delete", uid)
      stmt.execute(s"DELETE FROM CELL WHERE uid = $uid")
      stmt.execute(s"DELETE FROM CELL_APPENDONLY WHERE uid = $uid")
    }
  }

  def clear(): Unit = {
    use(conn.get.createStatement()) { stmt =>
      stmt.execute(s"DELETE FROM CELL")
      stmt.execute(s"DELETE FROM CELL_APPENDONLY")
    }
  }
}
