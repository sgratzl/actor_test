import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.function.Supplier

import matrix.Matrix

import scala.collection.mutable


package object db {
  private val driver = "org.apache.derby.jdbc.EmbeddedDriver"

  Class.forName(driver).newInstance()

  private val conn = ThreadLocal.withInitial(new Supplier[Connection]() {
    override def get(): Connection = DriverManager.getConnection("jdbc:derby:derbyDB;create=true", null)
  })
  private val cellQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE key = ? i = ? AND j = ?")
  })
  private val rowQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE key = ? i = ?")
  })
  private val colQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("SELECT value from CELL WHERE key = ? AND j = ?")
  })
  private val insertQuery = ThreadLocal.withInitial(new Supplier[PreparedStatement]() {
    override def get() = conn.get().prepareStatement("INSERT INTO CELL VALUES(?, ?, ?, ?)")
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

  def cell(key: Int, i: Int, j: Int): Int = {
    val p = cellQuery.get()
    p.setInt(1, key)
    p.setInt(2, i)
    p.setInt(3, j)

    p.executeQuery().getInt(1)
  }

  def row(key: Int, i: Int): Array[Int] = {
    val p = rowQuery.get()
    p.setInt(1, key)
    p.setInt(2, i)

    iterate(p.executeQuery()).map(_.getInt(1)).toArray
  }

  def col(key: Int, j: Int): Array[Int] = {
    val p = colQuery.get()
    p.setInt(1, key)
    p.setInt(2, j)

    iterate(p.executeQuery()).map(_.getInt(1)).toArray
  }

  def cell_(key: Int, i: Int, j: Int, v: Int): Boolean = {
    val p = insertQuery.get()
    p.setInt(1, key)
    p.setInt(2, i)
    p.setInt(3, j)
    p.setInt(4, v)
    p.executeUpdate() != 0
  }

  def row_(key: Int, i: Int, vs: Iterable[Int]): Int = {
    val p = insertQuery.get()
    p.setInt(1, key)
    p.setInt(2, i)
    vs.zipWithIndex.foreach({ case (vi, j) =>
      p.setInt(3, j)
      p.setInt(4, vi)
      p.addBatch()
    })
    p.executeBatch().sum
  }

  def col_(key: Int, j: Int, vs: Iterable[Int]): Int = {
    val p = insertQuery.get()
    p.setInt(1, key)
    p.setInt(3, j)
    vs.zipWithIndex.foreach({ case (vi, i) =>
      p.setInt(2, i)
      p.setInt(4, vi)
      p.addBatch()
    })
    p.executeBatch().sum
  }

  def save(key: Int): Matrix = {
    use(conn.get.createStatement()) { stmt =>
      val rs = stmt.executeQuery(s"SELECT MAX(i), MAX(j) FROM CELL WHERE key = $key")
      val rows = rs.getInt(1)
      val cols = rs.getInt(2)
      rs.close()

      val r = (0 until rows).map((_) => mutable.IndexedSeq.fill(cols)(0)).toIndexedSeq
      val data = stmt.executeQuery(s"SELECT i, j, value FROM CELL WHERE key = $key")
      iterate(data).foreach((rs) => r(rs.getInt(1))(rs.getInt(2)) = rs.getInt(3))
      data.close()

      Matrix(r.map(_.toIndexedSeq))
    }
  }

  def load(key: Int, matrix: Matrix): Unit = {
    use(conn.get.createStatement()) { stmt =>
      stmt.execute("CREATE IF NOT EXIST TABLE CELL (key INT, row INT, col INT, value INT, PRIMARY KEY (key, row, col)")
    }
    val p = insertQuery.get()
    p.setInt(1, key)
    for ((row, i) <- matrix.values.zipWithIndex; (cell, j) <- row.zipWithIndex) {
      p.setInt(2, i)
      p.setInt(3, j)
      p.setInt(4, cell)
      p.addBatch()
    }
    p.executeBatch()
  }

  def delete(key: Int): Boolean = {
    use(conn.get.createStatement()) { stmt =>
      return stmt.execute(s"DELETE FROM CELL WHERE key = $key")
    }
  }
}
