import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._

class ScalaGeneratorTest extends SkylaThread[Int] {

  override def run = {

    val a = Skyla.spawnGenerator[Int] { cyield =>
      cyield(5)
      cyield(6)
      cyield(7)
    }

    val iter = a.iterator

    println(iter.sum)

    5

  }

}
