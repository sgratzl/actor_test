import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedDeque
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global


package object tasks {

  object TaskType extends Enumeration {
    val Plus, Minus, Times, Divide = Value
  }
  type TaskTypeValue = TaskType.Value

  case class TaskException(result: InvalidTaskResult) extends Exception

  type TaskQueue = ConcurrentLinkedDeque[Task]

  case class Task(task: TaskType.Value, a: Int, b: Int, promise: Promise[Int]) {
    def byteBuffer: ByteBuffer = {
      val buffer = ByteBuffer.allocate(4 + 4 + 4)
      buffer.putInt(task.id)
      buffer.putInt(a)
      buffer.putInt(b)
      buffer
    }

    def compute(): Future[TaskResult] = {
      import TaskType._

      task match {
        case Plus =>
          Future {
            TaskResult(task, a + b)
          }
        case Minus =>
          Future {
            TaskResult(task, a - b)
          }
        case Times =>
          Future {
            TaskResult(task, a * b)
          }
        case Divide =>
          Future {
            TaskResult(task, a / b)
          }
      }
    }
  }

  object Task {
    def newResultByteBuffer: ByteBuffer = ByteBuffer.allocate(4 + 4 + 4)

    def apply(buffer: ByteBuffer): Task = {
      Task(TaskType(buffer.getInt), buffer.getInt, buffer.getInt, null)
    }
    def apply(task: TaskTypeValue, a: Int, b: Int): Task = Task(task, a, b, null.asInstanceOf[Promise[Int]])
  }

  case class TaskResult(task: TaskTypeValue, c: Int) {
    def byteBuffer: ByteBuffer = {
      val buffer = ByteBuffer.allocate(4 + 4)
      buffer.putInt(task.id)
      buffer.putInt(c)
      buffer
    }
  }

  case class InvalidTaskResult(taskId: Int) {
    def byteBuffer: ByteBuffer = {
      val buffer = ByteBuffer.allocate(4 + 4)
      buffer.putInt(-taskId) //negative = error
      buffer.putInt(0)
    }
  }

  object TaskResult {
    def newResultByteBuffer: ByteBuffer = ByteBuffer.allocate(4 + 4)

    def apply(buffer: ByteBuffer): TaskResult = {
      TaskResult(TaskType(buffer.getInt), buffer.getInt)
    }
  }
}