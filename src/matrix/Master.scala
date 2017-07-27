package matrix

import scala.actors.Actor
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

class Master(val slaves: Int) extends Actor {
  def act() {
    println("Start sending messages")
    for (slave <- (0 until slaves).map((i) => select(Node(s"slave$i", 9000), Symbol(s"slave$i")))) {
      slave ! "Hello"
    }
    var missing = slaves
    loopWhile(missing > 0) {
      react {
        case msg: String =>
          println(s"slave: $msg")
          missing -= 1
          println(s"missing: $missing")
          sender ! Kill
      }
    } andThen {
      println("Done Kill myself")
      exit()
    }
  }
}




