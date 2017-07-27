package matrix
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote.RemoteActor._

case object Kill

class Slave(val index: Int) extends Actor {
  def act() {
    println("Starting Slave")
    alive(9000)
    register(Symbol(s"slave$index"), self)
    react {
      case msg: String =>
        println(msg)
        val time = (Math.random()* 1000).toInt
        Thread.sleep(time)
        sender ! s"It's me after $time"
      case Kill =>
        println("Kill myself")
        exit()
    }
  }
}
