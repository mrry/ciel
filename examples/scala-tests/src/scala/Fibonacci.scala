import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._
import scala.util.continuations._

class Fibonacci extends SkylaThread[Int] {

  def fib(x : Int) : Int @suspendable = {
    if (x <= 1) {
      x
    } else {  
      val first = Skyla.spawn[Int] { _ => fib(x - 1) }
      val second = Skyla.spawn[Int] { _ => fib(x - 2) }
      first.get + second.get
    }
  }

  override def run = {
    val result = fib(10)
    println(result)
    result
  }
}
