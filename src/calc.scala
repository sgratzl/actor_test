import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

package object calc {


  object OperationType extends Enumeration {
    val Plus, Minus, Times, Divides = Value
  }
  import OperationType._
  type Operation = OperationType.Value

  trait Term {
    def apply(): Int
  }

  case class Atom(v: Int) extends Term {
    override def apply(): Int = v

    override def toString: String = v.toString
  }

  case class Equation(left: Term, op: Operation, right: Term) extends Term {
    override def apply(): Int = {
      op match {
        case Plus => left.apply() + right.apply()
        case Minus => left.apply() - right.apply()
        case Times => left.apply() * right.apply()
        case Divides => left.apply() / right.apply()
      }
    }

    override def toString: String = {
      op match {
        case Plus => s"($left + $right)"
        case Minus => s"($left - $right)"
        case Times => s"($left * $right)"
        case Divides => s"($left / $right)"
      }
    }
  }

  def parse(s: String): Equation = {
    val r = s.split(' ')
    def toOp(op: String) = {
      op match {
        case "+" => Plus
        case "-" => Minus
        case "*" => Times
        case "/" => Divides
      }
    }
    Equation(Atom(r(0).toInt), toOp(r(1)), Atom(r(2).toInt))
  }

  def compute(args: Equation): Future[Int] = {
    Future {
      args.apply()
    }
  }
}
