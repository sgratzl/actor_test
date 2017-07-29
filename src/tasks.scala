import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingDeque

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global


package object tasks {

  object TaskType extends Enumeration {
    val Plus, Minus, Times, Divide = Value
  }
  type TaskTypeValue = TaskType.Value

  case class TaskException(result: InvalidTaskResult) extends Exception

  type TaskQueue = LinkedBlockingDeque[Task]

  case class Task(uid: Int, task: TaskType.Value, a: Int, b: Int, promise: Promise[Int]) {
    def byteBuffer: ByteBuffer = {
      val buffer = ByteBuffer.allocate(4 + 4 + 4 + 4)
      buffer.putInt(uid)
      buffer.putInt(task.id)
      buffer.putInt(a)
      buffer.putInt(b)
      buffer.flip()
      buffer
    }

    def compute(): Future[TaskResult] = {
      import TaskType._

      task match {
        case Plus =>
          Future {
            TaskResult(uid, a + b)
          }
        case Minus =>
          Future {
            TaskResult(uid, a - b)
          }
        case Times =>
          Future {
            TaskResult(uid, a * b)
          }
        case Divide =>
          Future {
            TaskResult(uid, a / b)
          }
      }
    }
  }

  object Task {
    def newResultByteBuffer: ByteBuffer = ByteBuffer.allocate(4 + 4 + 4 + 4)

    def apply(buffer: ByteBuffer): Task = {
      Task(buffer.getInt, TaskType(buffer.getInt), buffer.getInt, buffer.getInt, null)
    }
    def apply(uid: Int, task: TaskTypeValue, a: Int, b: Int): Task = Task(uid, task, a, b, null.asInstanceOf[Promise[Int]])
  }

  case class TaskResult(uid: Int, c: Int) {
    def byteBuffer: ByteBuffer = {
      val buffer = ByteBuffer.allocate(4 + 4)
      buffer.putInt(uid)
      buffer.putInt(c)
      buffer.flip()
      buffer
    }
  }

  case class InvalidTaskResult(uid: Int) {
    def byteBuffer: ByteBuffer = {
      val buffer = ByteBuffer.allocate(4 + 4)
      buffer.putInt(-uid) //negative = error
      buffer.putInt(0)
    }
  }

  object TaskResult {
    def newResultByteBuffer: ByteBuffer = ByteBuffer.allocate(4 + 4)

    def apply(buffer: ByteBuffer): TaskResult = {
      TaskResult(buffer.getInt, buffer.getInt)
    }
  }
}