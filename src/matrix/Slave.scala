package matrix

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.collection.mutable

case class Init(n: Int, i: Int, j: Int, a: Int, b: Int)

case class SetValue(what: Symbol, v: Int)

case class Collect(i: Int, j: Int, c: Int)

case class SlaveLookup(val n: Int, val slaves: Int, val index: Int, val self: Actor) {
  private val remotes: IndexedSeq[OutputChannel[Any]] = (0 to slaves).map((i) => if (i == index) self else select(Node(s"slave$i", 9000), Symbol(s"slave$i")))

  def apply(i: Int, j: Int): OutputChannel[Any] = {
    //TODO compute the magic which slave it is
    remotes(0)
  }
}

class Master(val slaves: Int, val a: Matrix, val b: Matrix, val result: (Matrix) => Unit) extends Actor {
  def act() {
    val n = a.nrow
    val slaveAt = SlaveLookup(n, slaves, -1, null)

    for (i <- 0 until n; j <- 0 until n) {
      slaveAt(i, j) ! Init(n, i, j, a.values(i)(j), b.values(i)(j))
    }
    var missing = n * n
    val c = (0 until b.nrow).map((_) => mutable.IndexedSeq.fill(a.ncol)(0)).toIndexedSeq

    loopWhile(missing > 0) {
      react {
        case Collect(i, j, v) =>
          c(i)(j) = v
          missing -= 1
      }
    } andThen {
      println("Done")
      result(Matrix(c.map(_.toIndexedSeq)))
      exit()
    }
  }
}

class Slave(val index: Int, val slaves: Int) extends Actor {
  def act() {
    alive(9000)
    register(Symbol(s"slave$index"), self)

    var n: Int = 0
    var i: Int = 0
    var j: Int = 0
    var a: Int = 0
    var b: Int = 0
    var c: Int = 0
    var master: OutputChannel[Any] = null
    var slaveAt: SlaveLookup = null
    react {
      case Init(_n, _i, _j, _a, _b) =>
        n = _n
        i = _i
        j = _j
        a = _a
        b = _b
        c = 0
        master = sender
        slaveAt = SlaveLookup(n, slaves, index, this)

        slaveAt(i, (j - i) % n) ! SetValue('a, a)
        react {
          case SetValue('a, v) =>
            a = v
            slaveAt((i - j) % n, j) ! SetValue('b, b)
            react {
              case SetValue('b, v) =>
                b = v
                var i = 0
                loopWhile(i < n) {
                  c += a * b
                  slaveAt(i, (j - 1) % n) ! SetValue('a, a)
                  react {
                    case SetValue('a, v) =>
                      a = v
                      slaveAt((i - 1) % n, j) ! SetValue('b, b)
                      react {
                        case SetValue('b, v) =>
                          b = v
                      }
                  }
                } andThen {
                  master ! Collect(i, j, c)
                  println("Kill myself")
                  exit()
                }
            }
        }
    }
  }
}