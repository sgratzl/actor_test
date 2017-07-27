package matrix

import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.collection.mutable

case class Init(n: Short)

case class At(i: Short, j: Short)

case class Assign(at: At, a: Int, b: Int)
case class Assigns(v: Iterable[Assign])

case class SetValue(at: At, what: Symbol, v: Int)
case class SetValues(v: Iterable[SetValue])

case class Collect(at: At, c: Int)
case class Collects(v: Iterable[Collect])

class GridNode(var a: Int = 0, var b: Int = 0, var c: Int = 0) {

}

case class SlaveLookup(n: Int, slaves: Int, index: Int, self: Actor) {
  val remotes: IndexedSeq[OutputChannel[Any]] = (0 to slaves).map((i) => if (i == index) self else select(Node(s"slave$i", 9000), Symbol(s"slave$i")))

  def apply(at: At): OutputChannel[Any] = {
    val abs = (at.i * n) + at.j
    remotes(abs % slaves)
  }

  def nodesOf(slave: Int): Map[At, GridNode] = {
    val pairs = for (i <- 0 until n; j <- 0 until n if ((i * n) + j) % slaves == slave) yield At(i.asInstanceOf[Short], j.asInstanceOf[Short])
    pairs.map(_ -> new GridNode()).toMap
  }
}


class Master(val slaves: Int, val a: Matrix, val b: Matrix, val result: (Matrix) => Unit) extends Actor {
  def act() {
    val n = a.nrow
    val slaveAt = SlaveLookup(n, slaves, -1, null)

    slaveAt.remotes.foreach(_ ! Init(n.asInstanceOf[Short]))

    val assignments = for (i <- 0 until n; j <- 0 until n) yield {
      val at = At(i.asInstanceOf[Short], j.asInstanceOf[Short])
      (slaveAt(at), Assign(at, a.values(i)(j), b.values(i)(j)))
    }
    assignments.groupBy((kv) => kv._1).foreach((kvs) => kvs._1 ! Assigns(kvs._2.map(_._2)))

    var missing = n * n
    val c = (0 until b.nrow).map((_) => mutable.IndexedSeq.fill(a.ncol)(0)).toIndexedSeq

    loopWhile(missing > 0) {
      react {
        case Collect(At(i, j), v) =>
          //println(At(i,j), "Collect")
          c(i)(j) = v
          missing -= 1
        case Collects(vs) =>
          println("got collect")
          for ((Collect(At(i,j), v)) <- vs) {
            c(i)(j) = v
          }
          missing -= vs.size
      }
    } andThen {
      //println("Done")
      result(Matrix(c.map(_.toIndexedSeq)))
      exit()
    }
  }
}

class Slave(val index: Int, val slaves: Int) extends Actor {
  def act() {
    alive(9000)
    register(Symbol(s"slave$index"), self)

    react {
      case Init(_n) =>
        val n = _n
        val master = sender
        val slaveAt = SlaveLookup(n, slaves, index, this)
        val nodes = slaveAt.nodesOf(index)

        def sendAll(toSend: Iterable[(OutputChannel[Any], SetValue)]): Int = {
          var dec = nodes.size * 2
          toSend.groupBy((kv) => kv._1).filter((kvs) => {
            if (kvs._1 != this) {
              true
            } else {
              //apply directly
              for ((_, SetValue(at, s, v)) <- kvs._2) {
                //println(at, "set")
                val node = nodes(at)
                if (s == 'a) node.a = v else node.b = v
              }
              dec -= kvs._2.size
              false
            }
          }).foreach((kvs) => kvs._1 ! SetValues(kvs._2.map(_._2)))

          dec
        }
        //println("handle: ", nodes.keys)
        var dec = nodes.size
        loopWhile(dec > 0) {
          react {
            case Assign(at, a, b) =>
              //println(at, "assign")
              val node = nodes(at)
              node.a = a
              node.b = b
              dec -= 1
            case Assigns(vs) =>
              println("got assigns")
              for(Assign(at, a, b) <- vs) {
                val node = nodes(at)
                node.a = a
                node.b = b
              }
              dec -= vs.size
          }
        } andThen {
          val toSend = (for (Tuple2(at, node) <- nodes) yield {
            val targetA = At(at.i, ((at.j - at.i + n) % n).asInstanceOf[Short])
            val a = (slaveAt(targetA), SetValue(targetA, 'a, node.a))
            val targetB = At(((at.i - at.j + n) % n).asInstanceOf[Short], at.j)
            val b = (slaveAt(targetB), SetValue(targetB, 'b, node.b))
            Array[(OutputChannel[Any], SetValue)](a, b)
          }).flatten
          dec = sendAll(toSend)

          loopWhile(dec > 0) {
            react {
              case SetValue(at, s, v) =>
                //println(at, "set")
                val node = nodes(at)
                if (s == 'a) node.a = v else node.b = v
                dec -= 1
              case SetValues(vs) =>
                println("got set values")
                for(SetValue(at, s, v) <- vs) {
                  //println(at, "set")
                  val node = nodes(at)
                  if (s == 'a) node.a = v else node.b = v
                }
                dec -= vs.size
            }
          } andThen {
            var incN = 0
            loopWhile(incN < n) {
              //println("multiply")
              nodes.values.foreach((v) => v.c += v.a * v.b)

              val toSend = (for (Tuple2(at, node) <- nodes) yield {
                val targetA = At(at.i, ((at.j - 1 + n) % n).asInstanceOf[Short])
                val a = (slaveAt(targetA), SetValue(targetA, 'a, node.a))
                val targetB = At(((at.i - 1 + n) % n).asInstanceOf[Short], at.j)
                val b = (slaveAt(targetB), SetValue(targetB, 'b, node.b))
                Array[(OutputChannel[Any], SetValue)](a, b)
              }).flatten
              dec = sendAll(toSend)

              loopWhile(dec > 0) {
                react {
                  case SetValue(at, s, v) =>
                    //println(at, "set")
                    val node = nodes(at)
                    if (s == 'a) node.a = v else node.b = v
                    dec -= 1
                  case SetValues(vs) =>
                    println("got set values")
                    for(SetValue(at, s, v) <- vs) {
                      //println(at, "set")
                      val node = nodes(at)
                      if (s == 'a) node.a = v else node.b = v
                    }
                    dec -= vs.size
                }
              } andThen {
                incN += 1
                //println("iteration done")
                continue()
              }
            } andThen {
              println("Sent")
              master ! Collects(nodes.map((kv) => Collect(kv._1, kv._2.c)))
              println("Kill myself")
              exit()
            }
          }
        }
    }
  }
}