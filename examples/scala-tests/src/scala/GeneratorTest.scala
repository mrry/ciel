import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._

class ScalaGeneratorTest extends SkylaThread[Int] {
  override def run = {

    val a = Skyla.spawnGenerator[Int] { _yield =>
      _yield(6)
      _yield(3)
      _yield(7)
    }

    val iter = a.iterator 
    val total = iter.sum

    total
  }
}
