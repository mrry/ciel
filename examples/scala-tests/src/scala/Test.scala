import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._

class InitTailTest(deps : Array[CielFuture[Int]]) extends FirstClassJavaTask {

  override def setup = { }

  override def getDependencies = deps.asInstanceOf[Array[Reference]]

  override def invoke = {
    deps.foreach { i => println(i.get()) }
    Ciel.returnPlainString("Hello, world!")
  }

}

class ScalaInitTest extends FirstClassJavaTask {

  override def setup = { }

  override def getDependencies = Array()

  override def invoke = {
    val a = Skyla.spawn { _ => 42 }
    val b = Skyla.spawn { _ => 16 }
    val c = Skyla.spawn { _ => 10 }

    val tail = new InitTailTest(Array(a, b, c));
    Ciel.tailSpawn(tail, Array());

  }

}
